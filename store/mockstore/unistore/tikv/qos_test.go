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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/pingcap/kvproto/pkg/coprocessor"
)

func newItem(priority uint64) *copReqTask {
	return &copReqTask{
		Request: &coprocessor.Request{
			Priority: priority,
		},
	}
}

func enqueueSeq(pq *PriorityQueue, vals ...uint64) {
	for _, v := range vals {
		pq.Enqueue(newItem(v))
	}
}

func dequeueSeq(t *testing.T, pq *PriorityQueue, vals ...uint64) {
	for _, v := range vals {
		x := pq.Dequeue()
		require.Equal(t, x.Request.Priority, v)
	}
}

func TestPriorityQueue(t *testing.T) {
	pq := newPriorityQueue(10)
	require.Equal(t, pq.Len(), 0)

	pq.Enqueue(newItem(34))
	tmp := pq.Dequeue()
	require.Equal(t, tmp.Request.Priority, uint64(34))

	enqueueSeq(pq, 3, 2, 7, 11, 4, 1)
	require.Equal(t, pq.Len(), 6)
	
	dequeueSeq(t, pq, 1, 2, 3)
	require.Equal(t, pq.Len(), 3)

	dequeueSeq(t, pq, 4, 7, 11)
	require.Equal(t, pq.Len(), 0)
}
