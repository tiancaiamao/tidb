// Copyright 2021 PingCAP, Inc.
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

package chunk

import (
	"math"
	"unsafe"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mathutil"
//	"github.com/pingcap/tidb/metrics"
)

type Alloc interface {
	Alloc(int) []byte
}

func allocInt64(alloc Alloc, sz int) []int64 {
	b := alloc.Alloc(8 * sz)
	return (*[math.MaxInt32]int64)(unsafe.Pointer(&b[0]))[:sz]
}

type ArenaAlloc interface {
	Alloc
	Reset()
}

type defaultArenaAlloc struct {}

func (_ defaultArenaAlloc) Alloc(sz int) []byte {
	return make([]byte, sz)
}

func (_ defaultArenaAlloc) Reset() {
}

type arenaAlloc struct {
	inuse []blockNode
	freelist []blockNode
}

const blockSize = 4<<20
type block [blockSize]byte
type blockNode struct {
	data *block
	offset int
	next *blockNode // free list
}

func NewArenaAlloc() *arenaAlloc {
	return &arenaAlloc{}
}

func (a *arenaAlloc) Alloc(sz int) []byte {
	if sz > blockSize {
		return make([]byte, sz)
	}

	var b *blockNode
	if len(a.inuse) == 0 {
		b = a.newBlockNode()
	} else {
		b = &a.inuse[len(a.inuse) - 1]
		if b.offset + sz > blockSize {
			b = a.newBlockNode()
		}
	}
	ret := (*b.data)[b.offset: b.offset+sz]
	for i:=0; i<len(ret); i++ {
		ret[i] = 0
	}
	b.offset += sz
	return ret
}

func (a *arenaAlloc) newBlockNode() *blockNode {
	if len(a.freelist) > 0 {
		v := a.freelist[len(a.freelist) - 1]
		a.freelist = a.freelist[:len(a.freelist) - 1]
		a.inuse = append(a.inuse, v)
	} else {
		data := new(block)
		a.inuse = append(a.inuse, blockNode{data: data})
	}
	return &a.inuse[len(a.inuse) - 1]
}

func (a *arenaAlloc) Reset() {
	for _, v := range a.inuse {
		v.offset = 0
		a.freelist = append(a.freelist, v)
	}
}

// New creates a new chunk.
//  cap: the limit for the max number of rows.
//  maxChunkSize: the max limit for the number of rows.
func NewFromAlloc(alloc Alloc, fields []*types.FieldType, capacity, maxChunkSize int) *Chunk {
	chk := &Chunk{
		columns:  make([]*Column, 0, len(fields)),
		capacity: mathutil.Min(capacity, maxChunkSize),
		// set the default value of requiredRows to maxChunkSize to let chk.IsFull() behave
		// like how we judge whether a chunk is full now, then the statement
		// "chk.NumRows() < maxChunkSize"
		// equals to "!chk.IsFull()".
		requiredRows: maxChunkSize,
	}

	for _, f := range fields {
		chk.columns = append(chk.columns, newColumn(alloc, getFixedLen(f), chk.capacity))
	}
	return chk
}

// Allocator is an interface defined to reduce object allocation.
// The typical usage is to call Reset() to recycle objects into a pool,
// and Alloc() allocates from the pool.
type Allocator interface {
	Alloc(fields []*types.FieldType, capacity, maxChunkSize int) *Chunk
	Reset()
}

/*
// NewAllocator creates an Allocator.
func NewAllocator() *allocator {
	ret := &allocator{}
	ret.columnAlloc.init()
	return ret
}


var _ Allocator = &allocator{}

// allocator try to reuse objects.
// It uses `poolColumnAllocator` to alloc chunk column objects.
// The allocated chunks are recorded in the `allocated` array.
// After Reset(), those chunks are decoupled into chunk column objects and get
// into `poolColumnAllocator` again for reuse.
type allocator struct {
	allocated   []*Chunk
	free        []*Chunk
	columnAlloc poolColumnAllocator
}

// Alloc implements the Allocator interface.
func (a *allocator) Alloc(fields []*types.FieldType, capacity, maxChunkSize int) *Chunk {
	var chk *Chunk
	// Try to alloc from the free list.
	if len(a.free) > 0 {
		chk = a.free[len(a.free)-1]
		a.free = a.free[:len(a.free)-1]
	} else {
		chk = &Chunk{columns: make([]*Column, 0, len(fields))}
	}

	// Init the chunk fields.
	chk.capacity = mathutil.Min(capacity, maxChunkSize)
	chk.requiredRows = maxChunkSize
	// Allocate the chunk columns from the pool column allocator.
	for _, f := range fields {
		chk.columns = append(chk.columns, a.columnAlloc.NewColumn(f, chk.capacity))
	}

	a.allocated = append(a.allocated, chk)
	return chk
}

const (
	maxFreeChunks         = 64
	maxFreeColumnsPerType = 256
)

// Reset implements the Allocator interface.
func (a *allocator) Reset() {
	a.free = a.free[:0]
	for i, chk := range a.allocated {
		a.allocated[i] = nil
		// Decouple chunk into chunk column objects and put them back to the column allocator for reuse.
		for _, col := range chk.columns {
			a.columnAlloc.put(col)
		}
		// Reset the chunk and put it to the free list for reuse.
		chk.resetForReuse()

		if len(a.free) < maxFreeChunks { // Don't cache too much data.
			a.free = append(a.free, chk)
		}
	}
	a.allocated = a.allocated[:0]
}

var _ ColumnAllocator = &poolColumnAllocator{}

type poolColumnAllocator struct {
	pool     map[int]freeList
}

// poolColumnAllocator implements the ColumnAllocator interface.
func (alloc *poolColumnAllocator) NewColumn(ft *types.FieldType, count int) *Column {
	typeSize := getFixedLen(ft)
	l := alloc.pool[typeSize]
	if l != nil && !l.empty() {
		col := l.pop()
		metrics.ChunkReuseCounter.Add(float64(col.memUsage()))
		return col
	}
	col := newColumn(typeSize, count)
	metrics.ChunkAllocCounter.Add(float64(col.memUsage()))
	return col
}

func (alloc *poolColumnAllocator) init() {
	alloc.pool = make(map[int]freeList)
}

func (alloc *poolColumnAllocator) put(col *Column) {
	if col.avoidReusing {
		metrics.ChunkAvoidReuseCounter.Add(float64(col.memUsage()))
		return
	}
	typeSize := col.typeSize()
	if typeSize <= 0 {
		metrics.ChunkFreeCounter.Add(float64(col.memUsage()))
		return
	}

	l := alloc.pool[typeSize]
	if l == nil {
		l = make(map[*Column]struct{}, 8)
		alloc.pool[typeSize] = l
	}
	l.push(col)
}

// freeList is defined as a map, rather than a list, because when recycling chunk
// columns, there could be duplicated one: some of the chunk columns are just the
// reference to the others.
type freeList map[*Column]struct{}

func (l freeList) empty() bool {
	return len(l) == 0
}

func (l freeList) pop() *Column {
	for k := range l {
		delete(l, k)
		return k
	}
	return nil
}

func (l freeList) push(c *Column) {
	if len(l) >= maxFreeColumnsPerType {
		// Don't cache too much to save memory.
		metrics.ChunkFreeCounter.Add(float64(c.memUsage()))
		return
	}
	l[c] = struct{}{}
	metrics.ChunkRecycleCounter.Add(float64(c.memUsage()))
}

*/