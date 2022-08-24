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
	"io"
	"math"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
)

// An Item is something we manage in a priority queue.
type Item struct {
	value    string // The value of the item; arbitrary.
	priority int    // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

type PriorityQueue []copReqTask

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].Request.Priority > pq[j].Request.Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(copReqTask)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	// old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1 : cap(*pq)]
	return item
}

func (pq *PriorityQueue) Full() bool {
	return len(*pq) == cap(*pq)
}

func (pq *PriorityQueue) Empty() bool {
	return len(*pq) == 0
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *copReqTask, value string, priority int) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}

type copReqTask struct {
	Request *coprocessor.Request

	sync.WaitGroup
	Response *coprocessor.Response
	err      error

	*Server
	notify chan<- struct{}
	index  int
}

// enqueue is the input channel, requests are send to this channel.
// workerCh is a pool of workers, use <-workerCh to get a worker from it.
// notify channel is used as a signal, to notify there are idle worker.
func schedulerGoroutine(queue *PriorityQueue, enqueue chan copReqTask, wp workerPool) {
	workerIdle := true
	notify := make(chan struct{}, 1)
	for {
		if queue.Full() {
			handleDispatch(queue, wp, notify)
			continue
		}

		// serve either enqueue or dispatch
		select {
		case req := <-enqueue:
			queue.Push(req)
			if workerIdle {
				triggerDispatch(notify)
			}
		case <-notify:
			workerIdle = handleDispatch(queue, wp, notify)
		}
	}
}

func triggerDispatch(notify chan struct{}) {
	select {
	case notify <- struct{}{}:
	default:
	}
}

// handleDispatch returns whether there is no more workers to handle the request.
func handleDispatch(queue *PriorityQueue, pool workerPool, notify chan<- struct{}) bool {
	for !queue.Empty() {
		worker, succ := pool.Get()
		if !succ {
			return false
		}
		task := queue.Pop()
		task.notify = notify
		worker.Run(task.execute)
	}
	return true
}

func (task *copReqTask) execute() {
	resp.Resp, err = task.Server.Coprocessor(ctx, task.Request)
	task.Done()
	triggerDispatch(task.notify)
}

func startQoS() chan copReqTask {
	pq := make([]*coprocessor.Request, 0, 300)
	enqueue := make(chan copReqTask, 10)
	wp := newWorkerPool(8)
	go schedulerGoroutine(pq, enqueue, wp)
	return enqueue
}

const enableQoS = true

type workerPool struct {
	ch chan worker
}

func newWorkerPool(n int) workerPool {
	wp := workerPool{
		ch: make(chan worker, n),
	}
	for i := 0; i < n; i++ {
		w := worker{
			ch:         make(chan func()),
			workerPool: wp,
		}
		go workerGoroutine(w)
		wp.ch <- w
	}
	return wp
}

func (wp workerPool) Get() (w worker, succ bool) {
	select {
	case w = <-wp.ch:
		return w, true
	default:
		return worker{}, false
	}
}

type worker struct {
	ch chan func()
	workerPool
}

func (w worker) Run(f func()) {
	worker.ch <- f
}

// worker bind with a goroutine,
func workerGoroutine(w worker) {
	for f := range w.ch {
		f()

		// What's special about it is that the worker itself is put back to the pool,
		// after handling one task.
		w.workerPool <- w
	}
}
