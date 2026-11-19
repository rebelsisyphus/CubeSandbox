// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"sync"
	"sync/atomic"
	"time"
)

// MemoryReplicaMetricSnapshot is a lock-free-ish snapshot for the Cubelet
// metrics collector. Snapshot IDs are deliberately not used as labels.
type MemoryReplicaMetrics struct {
	AcquireHit       uint64
	AcquireCreated   uint64
	AcquireFallback  uint64
	AcquireError     uint64
	CreateSeconds    float64
	CreateCount      uint64
	CachedObjects    uint64
	FallbackByReason map[string]uint64
}

var (
	memoryReplicaHit      atomic.Uint64
	memoryReplicaCreated  atomic.Uint64
	memoryReplicaFallback atomic.Uint64
	memoryReplicaError    atomic.Uint64
	memoryReplicaDuration atomic.Uint64
	memoryReplicaCreateN  atomic.Uint64
	memoryReplicaCached   atomic.Uint64

	memoryReplicaFallbackMu sync.Mutex
	memoryReplicaFallbacks  = map[string]uint64{}
)

// RecordMemoryReplicaAcquire increments the acquire result counter.
func RecordMemoryReplicaAcquire(result string) {
	recordMemoryReplicaAcquire(result)
}

func recordMemoryReplicaAcquire(result string) {
	switch result {
	case "hit":
		memoryReplicaHit.Add(1)
	case "created":
		memoryReplicaCreated.Add(1)
	case "fallback":
		memoryReplicaFallback.Add(1)
	default:
		memoryReplicaError.Add(1)
	}
}

func recordMemoryReplicaCreate(d time.Duration, ok bool) {
	memoryReplicaCreateN.Add(1)
	if d > 0 {
		memoryReplicaDuration.Add(uint64(d))
	}
	if ok {
		memoryReplicaCached.Add(1)
	}
}

func recordMemoryReplicaFallback(reason string) {
	if reason == "" {
		return
	}
	memoryReplicaFallbackMu.Lock()
	memoryReplicaFallbacks[reason]++
	memoryReplicaFallbackMu.Unlock()
}

func recordMemoryReplicaDeleted() {
	for {
		current := memoryReplicaCached.Load()
		if current == 0 {
			return
		}
		if memoryReplicaCached.CompareAndSwap(current, current-1) {
			return
		}
	}
}

// MemoryReplicaMetricSnapshot returns the current process-local counters.
func SnapshotMemoryReplicaMetrics() MemoryReplicaMetrics {
	memoryReplicaFallbackMu.Lock()
	fallbacks := make(map[string]uint64, len(memoryReplicaFallbacks))
	for reason, count := range memoryReplicaFallbacks {
		fallbacks[reason] = count
	}
	memoryReplicaFallbackMu.Unlock()
	return MemoryReplicaMetrics{
		AcquireHit:       memoryReplicaHit.Load(),
		AcquireCreated:   memoryReplicaCreated.Load(),
		AcquireFallback:  memoryReplicaFallback.Load(),
		AcquireError:     memoryReplicaError.Load(),
		CreateSeconds:    float64(memoryReplicaDuration.Load()) / float64(time.Second),
		CreateCount:      memoryReplicaCreateN.Load(),
		CachedObjects:    memoryReplicaCached.Load(),
		FallbackByReason: fallbacks,
	}
}
