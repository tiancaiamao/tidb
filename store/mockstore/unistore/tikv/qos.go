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
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

type priorityQueueItem interface {
	Runnable
	GetPriority() uint64
	GetTaskID() uint64
}

type PriorityQueue[T priorityQueueItem] struct {
	data []T
	size int
}

func newPriorityQueue[T priorityQueueItem](size int) *PriorityQueue[T] {
	data := make([]T, 0, size)
	return &PriorityQueue[T]{
		data: data,
		size: size,
	}
}

type ptrcopReqTask = *copReqTask

func (pq *PriorityQueue[priorityQueueItem]) Len() int {
	return len(pq.data)
}

func (pq *PriorityQueue[priorityQueueItem]) Less(i, j int) bool {
	return pq.data[i].GetPriority() < pq.data[j].GetPriority()
}

func (pq *PriorityQueue[priorityQueueItem]) Swap(i, j int) {
	pq.data[i], pq.data[j] = pq.data[j], pq.data[i]
}

func (pq *PriorityQueue[priorityQueueItem]) Push(x any) {
	pq.data = append(pq.data, x.(priorityQueueItem))
}

func (pq *PriorityQueue[priorityQueueItem]) Pop() any {
	n := len(pq.data)
	ret := pq.data[n-1]
	pq.data = pq.data[:n-1:cap(pq.data)]
	return ret
}

func (pq *PriorityQueue[priorityQueueItem]) Enqueue(x priorityQueueItem) {
	heap.Push(pq, x)
}

func (pq *PriorityQueue[priorityQueueItem]) Dequeue() priorityQueueItem {
	x := heap.Pop(pq)
	return x.(priorityQueueItem)
}

func (pq *PriorityQueue[priorityQueueItem]) Full() bool {
	return len(pq.data) == pq.size
}

func (pq *PriorityQueue[priorityQueueItem]) Empty() bool {
	return len(pq.data) == 0
}

type copReqTask struct {
	Request *coprocessor.Request

	sync.WaitGroup
	Response *coprocessor.Response
	err      error

	*Server
}

func (x *copReqTask) GetPriority() uint64 {
	return x.Request.Priority
}

func (x *copReqTask) GetTaskID() uint64 {
	return x.Request.Context.TaskId
}

type txnReqTask struct {
	Request *kvrpcpb.PrewriteRequest

	sync.WaitGroup
	Response *kvrpcpb.PrewriteResponse
	err      error

	*Server
}

func (x *txnReqTask) GetPriority() uint64 {
	return x.Request.Priority
}

func (x *txnReqTask) GetTaskID() uint64 {
	return x.Request.Context.TaskId
}

func (task *txnReqTask) Run() {
	start := time.Now()
	resp, err := task.Server.handleKVPrewrite(context.Background(), task.Request)
	cost := time.Since(start) / time.Microsecond
	task.Response, task.err = resp, err
	task.Response.VirtualTime = atomic.LoadUint64(&writeVirtualTime)
	task.Response.Cost = uint64(cost)
	// fmt.Println("run task takes ==", task.Response.Cost)
	task.Done()
}

type FIFOQueue[T priorityQueueItem] struct {
	data []T
	start int
	end int
}

func newFIFOQueue[T priorityQueueItem](size int) *FIFOQueue[T] {
	data := make([]T, size)
	return &FIFOQueue[T]{
		data: data,
		start: 0,
		end: 0,
	}
}

func (fifo *FIFOQueue[priorityQueueItem]) Len() int {
	return 0
}

func (fifo *FIFOQueue[priorityQueueItem]) Enqueue(x priorityQueueItem) {
	fifo.data[fifo.start] = x
	fifo.start = (fifo.start + 1) % len(fifo.data)
}

func (fifo *FIFOQueue[priorityQueueItem]) Dequeue() priorityQueueItem {
	x := fifo.data[fifo.end]
	fifo.end = (fifo.end + 1) % len(fifo.data)
	return x
}

func (fifo *FIFOQueue[priorityQueueItem]) Full() bool {
	return ((fifo.start + 1) % len(fifo.data)) == fifo.end
}

func (fifo *FIFOQueue[priorityQueueItem]) Empty() bool {
	return fifo.start == fifo.end
}

type Queue[T any] interface {
	Full() bool
	Empty() bool
	Enqueue(T)
	Dequeue() T
	Len() int
}

// enqueue is the input channel, requests are send to this channel.
// workerPool is a pool of workers, use <-pool to get a worker from it.
func schedulerGoroutine[T priorityQueueItem](queue Queue[priorityQueueItem], enqueue chan T, pool workerPool, vt *uint64) {
	for {
		if queue.Full() {
			worker := <-pool
			task := queue.Dequeue()
			worker.Run(task)
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
				// fmt.Println("dequeue task=", task.GetTaskID(), " priority=", task.GetPriority(), "queue len=", queue.Len())
				worker.Run(task)
				updateVirtualTime(vt, task.GetPriority())
			}
		}
	}
}

func updateVirtualTime(vt *uint64, min uint64) {
	if min > atomic.LoadUint64(vt) {
		atomic.StoreUint64(vt, min)
	}
}

var readVirtualTime uint64
var writeVirtualTime uint64

func (task *copReqTask) Run() {
	start := time.Now()
	resp, err := task.Server.handleCoprocessor(context.Background(), task.Request)
	cost := time.Since(start) / time.Microsecond
	task.Response, task.err = resp, err
	task.Response.VirtualTime = atomic.LoadUint64(&readVirtualTime)
	task.Response.Cost = uint64(cost)
	// fmt.Println("the cop task takes ==", task.Response.Cost)
	task.Done()
}

func (svr *Server) startQoS() {
	svr.copQueue = copQueueInit()
	svr.txnQueue = txnQueueInit()
}

func copQueueInit() chan *copReqTask {
	// pq := newFIFOQueue(300)
	pq := newPriorityQueue[priorityQueueItem](300)
	enqueue := make(chan *copReqTask, 10)
	wp := newWorkerPool(2)
	go schedulerGoroutine(pq, enqueue, wp, &readVirtualTime)
	return enqueue
}

func txnQueueInit() chan *txnReqTask {
	// pq := newFIFOQueue[priorityQueueItem](300)
	pq := newPriorityQueue[priorityQueueItem](300)
	enqueue := make(chan *txnReqTask, 10)
	wp := newWorkerPool(1)
	go schedulerGoroutine(pq, enqueue, wp, &writeVirtualTime)
	return enqueue
}

// const enableQoS = false
const enableQoS = true

type workerPool chan worker

func newWorkerPool(n int) workerPool {
	wp := make(chan worker, n)
	for i := 0; i < n; i++ {
		w := worker{
			ch:         make(chan Runnable),
			workerPool: wp,
		}
		go workerGoroutine(w)
		wp <- w
	}
	return wp
}

type Runnable interface {
	Run()
}

type worker struct {
	ch chan Runnable
	workerPool
}

func (w worker) Run(f Runnable) {
	w.ch <- f
}

// worker bind with a goroutine,
func workerGoroutine(w worker) {
	for f := range w.ch {
		// fmt.Println("before fn exec")
		f.Run()
		// fmt.Println("after fn exec")
		// What's special about it is that the worker itself is put back to the pool,
		// after handling one task.
		w.workerPool <- w
	}
}
