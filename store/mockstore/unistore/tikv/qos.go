// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"
	"time"
	"fmt"
	"sync"
	"sync/atomic"
	"container/heap"

	"github.com/pingcap/kvproto/pkg/coprocessor"
)

type PriorityQueue struct {
	data []*copReqTask
	size int
}

func newPriorityQueue(size int) *PriorityQueue {
	data := make([]*copReqTask, 0, size)
	return &PriorityQueue{
		data: data,
		size: size,
	}
}

func (pq *PriorityQueue) Len() int {
	return len(pq.data)
}

func (pq *PriorityQueue) Less(i, j int) bool {
	return pq.data[i].Request.Priority < pq.data[j].Request.Priority
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.data[i], pq.data[j] = pq.data[j], pq.data[i]
}

func (pq *PriorityQueue) Push(x any) {
	pq.data = append(pq.data, x.(*copReqTask))
}

func (pq *PriorityQueue) Pop() any {
	n := len(pq.data)
	ret := pq.data[n-1]
	pq.data = pq.data[:n-1:cap(pq.data)]
	return ret
}

func (pq *PriorityQueue) Enqueue(x *copReqTask) {
	heap.Push(pq, x)
}

func (pq *PriorityQueue) Dequeue() *copReqTask {
	x := heap.Pop(pq)
	return x.(*copReqTask)
}

func (pq *PriorityQueue) Full() bool {
	return len(pq.data) == pq.size
}

func (pq *PriorityQueue) Empty() bool {
	return len(pq.data) == 0
}

type copReqTask struct {
	Request *coprocessor.Request

	sync.WaitGroup
	Response *coprocessor.Response
	err      error

	*Server
	// notify chan<- struct{}
	// index  int
}

type FIFOQueue struct {
	data []*copReqTask
	start int
	end int
}

func newFIFOQueue(size int) *FIFOQueue {
	data := make([]*copReqTask, size)
	return &FIFOQueue{
		data: data,
		start: 0,
		end: 0,
	}
}

func (fifo *FIFOQueue) Enqueue(x *copReqTask) {
	fifo.data[fifo.start] = x
	fifo.start = (fifo.start + 1) % len(fifo.data)
}

func (fifo *FIFOQueue) Dequeue() *copReqTask {
	x := fifo.data[fifo.end]
	fifo.end = (fifo.end + 1) % len(fifo.data)
	return x
}

func (fifo *FIFOQueue) Full() bool {
	return ((fifo.start + 1) % len(fifo.data)) == fifo.end
}

func (fifo *FIFOQueue) Empty() bool {
	return fifo.start == fifo.end
}

type Queue interface {
	Full() bool
	Empty() bool
	Enqueue(*copReqTask)
	Dequeue() *copReqTask
}

// enqueue is the input channel, requests are send to this channel.
// workerPool is a pool of workers, use <-pool to get a worker from it.
func schedulerGoroutine(queue Queue, enqueue chan *copReqTask, pool workerPool, vt *uint64) {
	for {
		if queue.Full() {
			worker := <-pool
			task := queue.Dequeue()
			worker.Run(task.execute)
			fmt.Println("queue full...")
			continue
		}

		// serve either enqueue or dispatch
		select {
		case req := <-enqueue:
			queue.Enqueue(req)
		case worker := <-pool:
			if queue.Empty() {
				queue.Enqueue(<-enqueue)
				pool <-worker
			} else {
				task := queue.Dequeue()
				// fmt.Println("dequeue task ==", task.Request.Context.TaskId, "priority=", task.Request.Priority, "queue length=", queue.Len())
				// fmt.Println("dequeue task ==", task.Request.Context.TaskId, "priority=", task.Request.Priority)
				worker.Run(task.execute)
				updateVirtualTime(vt, task.Request.Priority)
			}
		}
	}
}

func updateVirtualTime(vt *uint64, min uint64) {
	if min > atomic.LoadUint64(vt) {
		atomic.StoreUint64(vt, min)
	}
}

var virtualTime uint64

func (task *copReqTask) execute() {
	start := time.Now()
	resp, err := task.Server.handleCoprocessor(context.Background(), task.Request)
	cost := time.Since(start) / time.Microsecond
	task.Response, task.err = resp, err
	task.Response.VirtualTime = atomic.LoadUint64(&virtualTime)
	task.Response.Cost = uint64(cost)
	task.Done()
}

func startQoS() chan *copReqTask {
	// pq := newFIFOQueue(300)
	pq := newPriorityQueue(300)
	enqueue := make(chan *copReqTask, 10)
	wp := newWorkerPool(1)
	go schedulerGoroutine(pq, enqueue, wp, &virtualTime)
	return enqueue
}

const enableQoS = true

type workerPool chan worker

func newWorkerPool(n int) workerPool {
	wp := make(chan worker, n)
	for i := 0; i < n; i++ {
		w := worker{
			ch:         make(chan func()),
			workerPool: wp,
		}
		go workerGoroutine(w)
		wp <- w
	}
	return wp
}

type worker struct {
	ch chan func()
	workerPool
}

func (w worker) Run(f func()) {
	w.ch <- f
}

// worker bind with a goroutine,
func workerGoroutine(w worker) {
	for f := range w.ch {
		// fmt.Println("before fn exec")
		f()
		// fmt.Println("after fn exec")
		// What's special about it is that the worker itself is put back to the pool,
		// after handling one task.
		w.workerPool <- w
	}
}
