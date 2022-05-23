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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics for the domain package.
var (
	// ChunkAllocCounter records the counter of loading sysvars
	ChunkAllocCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "chunk",
			Name:      "alloc_counter",
			Help:      "Counter of load sysvar cache",
		})
		
	// ChunkFreeCounter records the counter of loading sysvars
	ChunkFreeCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "chunk",
			Name:      "free_counter",
			Help:      "Counter of load sysvar cache",
		})
		
	// ChunkRecycleCounter records the counter of loading sysvars
	ChunkRecycleCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "chunk",
			Name:      "recycle_counter",
			Help:      "Counter of load sysvar cache",
		})
		
	// ChunkReuseCounter records the counter of loading sysvars
	ChunkReuseCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "chunk",
			Name:      "reuse_counter",
			Help:      "Counter of load sysvar cache",
		})	
		
	// ChunkAvoidReuseCounter records the counter of loading sysvars
	ChunkAvoidReuseCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "chunk",
			Name:      "avoid_reuse_counter",
			Help:      "Counter of load sysvar cache",
		})						
)