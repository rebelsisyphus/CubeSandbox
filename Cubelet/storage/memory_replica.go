// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/log"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/numa"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/pathutil"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

const (
	MemoryReplicaModeOff              = "off"
	MemoryReplicaModePerNode          = "per_node"
	MemoryReplicaCreateOnSnapshot     = "snapshot"
	MemoryReplicaCreateOnFirstRestore = "first_restore"

	// MemorySnapshotTypeFull is the only capture mode eligible for a NUMA
	// memory replica. Incremental/soft-dirty snapshots keep the canonical path.
	MemorySnapshotTypeFull = "full"
)

// SnapshotMemoryReplica is defined next to SnapshotCatalogEntry.

// NumaMemoryReplicaEligible reports whether a catalog entry may use per-NUMA
// memory replicas. Pause snapshots, FS-only snapshots and non-XFS backends are
// deliberately excluded.
func NumaMemoryReplicaEligible(entry *SnapshotCatalogEntry) bool {
	if entry == nil || strings.TrimSpace(entry.MemoryVol) == "" {
		return false
	}
	if entry.Backend != "" {
		backend, err := cow.NormalizeBackend(entry.Backend)
		if err != nil || backend != cow.BackendXFS {
			return false
		}
	}
	switch entry.Kind {
	case CatalogKindPauseSnapshot:
		return false
	case CatalogKindTemplate:
		return entry.MemorySnapshotType == "" || entry.MemorySnapshotType == MemorySnapshotTypeFull
	case CatalogKindRuntimeSnapshot:
		if entry.MemorySnapshotType != "" {
			return entry.MemorySnapshotType == MemorySnapshotTypeFull
		}
		// Legacy runtime catalogs do not record the capture mode. Full
		// snapshots use a volume destination; incremental/soft-dirty use a
		// snapshot destination. Unknown entries stay conservative.
		return strings.TrimSpace(entry.MemoryKind) == cowKindVolume
	default:
		return false
	}
}

// SelectSnapshotMemory returns the memory object a restore on numaNode should
// use. It prefers the per-NUMA replica when present and eligible, otherwise it
// falls back to the canonical memory object.
func SelectSnapshotMemory(entry *SnapshotCatalogEntry, numaNode int32) (volume, kind string, replica bool) {
	if entry == nil {
		return "", "", false
	}
	volume = strings.TrimSpace(entry.MemoryVol)
	if volume == "" {
		return "", "", false
	}
	kind = strings.TrimSpace(entry.MemoryKind)
	if kind == "" {
		kind = CowKindVolume
	}
	if numaNode < 0 || !NumaMemoryReplicaEligible(entry) {
		return volume, kind, false
	}
	entryReplica, ok := entry.NumaMemoryReplicas[strconv.Itoa(int(numaNode))]
	if !ok {
		return volume, kind, false
	}
	replicaVolume := strings.TrimSpace(entryReplica.Volume)
	if replicaVolume == "" {
		return volume, kind, false
	}
	replicaKind := strings.TrimSpace(entryReplica.Kind)
	if replicaKind == "" {
		replicaKind = CowKindSnapshot
	}
	return replicaVolume, replicaKind, true
}

// NumaMemoryReplicaEnabledForConfig reports whether the XFS NUMA memory
// replica optimization is enabled in the current storage config.
func NumaMemoryReplicaEnabledForConfig() bool {
	return numaMemoryReplicaEnabled()
}

// EnsureSnapshotMemoryReplica lazily creates and registers one replica through
// the active storage plugin. Callers must only use this for XFS.
func EnsureSnapshotMemoryReplica(ctx context.Context, backend, snapshotID string, numaNode int32) (*SnapshotCatalogEntry, bool, error) {
	if localStorage == nil {
		return nil, false, fmt.Errorf("storage is not initialized")
	}
	return localStorage.ensureSnapshotMemoryReplica(ctx, backend, snapshotID, numaNode)
}

func numaMemoryReplicaEnabled() bool {
	if localStorage == nil || localStorage.config == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(localStorage.config.NumaMemoryReplicaMode), MemoryReplicaModePerNode)
}

func numaMemoryReplicaCreateOn() string {
	if localStorage == nil || localStorage.config == nil {
		return MemoryReplicaCreateOnSnapshot
	}
	if strings.EqualFold(strings.TrimSpace(localStorage.config.NumaMemoryReplicaCreateOn), MemoryReplicaCreateOnFirstRestore) {
		return MemoryReplicaCreateOnFirstRestore
	}
	return MemoryReplicaCreateOnSnapshot
}

// snapshotReplicaNumaNodes is a seam for tests. Production uses online nodes
// that can actually run a sandbox (have CPUs); memory-only nodes are skipped.
var snapshotReplicaNumaNodes = func() []int {
	nodes := numa.GetAllNumaNodes()
	ids := make([]int, 0, len(nodes))
	for _, node := range nodes {
		if strings.TrimSpace(node.Cpulist) == "" {
			continue
		}
		ids = append(ids, node.NodeId)
	}
	return ids
}

func cowTemplateMemoryReplicaName(templateID string, node int) string {
	return fmt.Sprintf("tpl-%s-memory-numa%d", templateID, node)
}

type memoryReplicaStore interface {
	CreateTemplateMemoryReplica(ctx context.Context, sourceName, sourceKind, replicaName string) (*cowVolume, error)
}

func createXFSMemoryReplica(ctx context.Context, sourceName, sourceKind, replicaName string) (*cowVolume, error) {
	store, err := requireCowStoreFor(cow.BackendXFS)
	if err != nil {
		return nil, err
	}
	replicaStore, ok := store.(memoryReplicaStore)
	if !ok || replicaStore == nil {
		return nil, fmt.Errorf("xfs store does not support memory replicas")
	}
	return replicaStore.CreateTemplateMemoryReplica(ctx, sourceName, sourceKind, replicaName)
}

// CreateMemoryReplicasForFullSnapshot eagerly clones a newly committed full
// memory snapshot for every eligible NUMA node. Callers persist the returned
// map in the catalog after the canonical snapshot is complete.
func CreateMemoryReplicasForFullSnapshot(ctx context.Context, backend string, snapshotID string, source *CowSnapshotObject) (map[string]SnapshotMemoryReplica, error) {
	if !numaMemoryReplicaEnabled() || numaMemoryReplicaCreateOn() != MemoryReplicaCreateOnSnapshot {
		return nil, nil
	}
	normalizedBackend, err := cow.NormalizeBackend(backend)
	if err != nil || normalizedBackend != cow.BackendXFS {
		return nil, nil
	}
	if source == nil || strings.TrimSpace(source.Name) == "" {
		return nil, fmt.Errorf("source memory object is required")
	}
	nodes := snapshotReplicaNumaNodes()
	if len(nodes) <= 1 {
		return nil, nil
	}
	replicas := make(map[string]SnapshotMemoryReplica)
	var errs error
	for _, node := range nodes {
		replicaName := cowTemplateMemoryReplicaName(snapshotID, node)
		unlock := lockReplicaCreate(snapshotID, int32(node))
		start := time.Now()
		volume, createErr := createXFSMemoryReplica(ctx, source.Name, source.Kind, replicaName)
		unlock()
		recordMemoryReplicaCreate(time.Since(start), createErr == nil)
		if createErr != nil {
			recordMemoryReplicaAcquire("error")
			recordMemoryReplicaFallback("create")
			errs = errors.Join(errs, fmt.Errorf("create NUMA%d memory replica %s: %w", node, replicaName, createErr))
			continue
		}
		recordMemoryReplicaAcquire("created")
		replicas[strconv.Itoa(node)] = SnapshotMemoryReplica{Volume: volume.VolumeName, Kind: volume.Kind}
	}
	return replicas, errs
}

// These locks are process-local. A single Cubelet process must own a storage
// root; running multiple Cubelets against the same root is unsupported until
// catalog updates use a cross-process file lock or CAS.
var (
	replicaCreateLocks  sync.Map // "snapshot/node" -> *sync.Mutex
	replicaCatalogLocks sync.Map // "snapshot" -> *sync.Mutex
)

func mutexFor(m *sync.Map, key string) *sync.Mutex {
	v, _ := m.LoadOrStore(key, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func lockReplicaCreate(snapshotID string, node int32) func() {
	mu := mutexFor(&replicaCreateLocks, fmt.Sprintf("%s/%d", snapshotID, node))
	mu.Lock()
	return mu.Unlock
}

func lockReplicaCatalog(snapshotID string) func() {
	mu := mutexFor(&replicaCatalogLocks, snapshotID)
	mu.Lock()
	return mu.Unlock
}

// ensureSnapshotMemoryReplica lazily creates and registers one NUMA replica for
// an existing full snapshot. It is idempotent and merges the catalog update
// under a per-snapshot lock.
func (l *local) ensureSnapshotMemoryReplica(ctx context.Context, backend, snapshotID string, numaNode int32) (*SnapshotCatalogEntry, bool, error) {
	if !numaMemoryReplicaEnabled() || numaNode < 0 {
		return nil, false, nil
	}
	normalizedBackend, err := cow.NormalizeBackend(backend)
	if err != nil || normalizedBackend != cow.BackendXFS {
		return nil, false, nil
	}
	unlockCreate := lockReplicaCreate(snapshotID, numaNode)
	defer unlockCreate()

	entry, err := GetLocalSnapshotFor(ctx, backend, snapshotID)
	if err != nil {
		return nil, false, err
	}
	if !NumaMemoryReplicaEligible(entry) {
		return entry, false, nil
	}
	key := strconv.Itoa(int(numaNode))
	if existing, ok := entry.NumaMemoryReplicas[key]; ok && strings.TrimSpace(existing.Volume) != "" {
		recordMemoryReplicaAcquire("hit")
		return entry, false, nil
	}
	replicaName := cowTemplateMemoryReplicaName(snapshotID, int(numaNode))
	start := time.Now()
	volume, err := createXFSMemoryReplica(ctx, strings.TrimSpace(entry.MemoryVol), entry.MemoryKind, replicaName)
	recordMemoryReplicaCreate(time.Since(start), err == nil)
	if err != nil {
		recordMemoryReplicaAcquire("error")
		recordMemoryReplicaFallback("create")
		return entry, false, fmt.Errorf("create NUMA%d memory replica %s from %s: %w", numaNode, replicaName, entry.MemoryVol, err)
	}
	recordMemoryReplicaAcquire("created")

	unlockCatalog := lockReplicaCatalog(snapshotID)
	defer unlockCatalog()
	fresh, err := GetLocalSnapshotFor(ctx, backend, snapshotID)
	if err != nil {
		return entry, false, err
	}
	if fresh.NumaMemoryReplicas == nil {
		fresh.NumaMemoryReplicas = make(map[string]SnapshotMemoryReplica)
	}
	if existing, ok := fresh.NumaMemoryReplicas[key]; ok && strings.TrimSpace(existing.Volume) != "" {
		return fresh, false, nil
	}
	fresh.NumaMemoryReplicas[key] = SnapshotMemoryReplica{Volume: volume.VolumeName, Kind: volume.Kind}
	if err := WriteSnapshotCatalogFor(backend, fresh); err != nil {
		return fresh, false, fmt.Errorf("persist NUMA%d memory replica %s: %w", numaNode, replicaName, err)
	}
	return fresh, true, nil
}

// EnumerateXFSMemoryReplicaRefs finds actual on-disk replica files for a
// snapshot. Cleanup uses this only when the catalog is missing or incomplete;
// it never infers replicas from the current NUMA topology.
func EnumerateXFSMemoryReplicaRefs(templateID string) ([]CowObjectRef, error) {
	if err := pathutil.ValidateSafeID(templateID); err != nil {
		return nil, fmt.Errorf("invalid template id: %w", err)
	}
	currentRoot, legacyRoot, ok := reflinkCleanupRoots()
	if !ok {
		return nil, nil
	}
	prefix := fmt.Sprintf("tpl-%s-memory-numa", templateID)
	seen := make(map[string]struct{})
	var refs []CowObjectRef
	for _, root := range []string{currentRoot, legacyRoot} {
		volumes := filepath.Join(root, "volumes")
		entries, err := os.ReadDir(volumes)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, err
		}
		for _, entry := range entries {
			if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
				continue
			}
			originDir := filepath.Join(volumes, entry.Name())
			files, err := os.ReadDir(originDir)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}
				return nil, err
			}
			for _, file := range files {
				name := file.Name()
				if !strings.HasPrefix(name, prefix) {
					continue
				}
				suffix := strings.TrimPrefix(name, prefix)
				if suffix == "" {
					continue
				}
				if _, err := strconv.Atoi(suffix); err != nil {
					continue
				}
				if err := pathutil.ValidateSafeID(name); err != nil {
					continue
				}
				info, err := os.Lstat(filepath.Join(originDir, name))
				if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
					continue
				}
				if _, ok := seen[name]; ok {
					continue
				}
				seen[name] = struct{}{}
				refs = append(refs, CowObjectRef{Name: name, Kind: CowKindSnapshot, Role: "memory_replica"})
			}
		}
	}
	log.G(context.Background()).Debugf("enumerated %d XFS memory replicas for %s", len(refs), templateID)
	return refs, nil
}
