// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

func TestNumaMemoryReplicaEligible(t *testing.T) {
	cases := []struct {
		name  string
		entry *SnapshotCatalogEntry
		want  bool
	}{
		{"template full", &SnapshotCatalogEntry{Kind: CatalogKindTemplate, MemoryVol: "mem"}, true},
		{"runtime full", &SnapshotCatalogEntry{Kind: CatalogKindRuntimeSnapshot, MemoryVol: "mem", MemorySnapshotType: MemorySnapshotTypeFull}, true},
		{"runtime incremental", &SnapshotCatalogEntry{Kind: CatalogKindRuntimeSnapshot, MemoryVol: "mem", MemorySnapshotType: "incremental"}, false},
		{"legacy runtime volume", &SnapshotCatalogEntry{Kind: CatalogKindRuntimeSnapshot, MemoryVol: "mem", MemoryKind: CowKindVolume}, true},
		{"legacy runtime snapshot", &SnapshotCatalogEntry{Kind: CatalogKindRuntimeSnapshot, MemoryVol: "mem", MemoryKind: CowKindSnapshot}, false},
		{"pause", &SnapshotCatalogEntry{Kind: CatalogKindPauseSnapshot, MemoryVol: "mem", MemorySnapshotType: MemorySnapshotTypeFull}, false},
		{"fs only", &SnapshotCatalogEntry{Kind: CatalogKindTemplate}, false},
		{"s3", &SnapshotCatalogEntry{Kind: CatalogKindTemplate, MemoryVol: "mem", Backend: "s3"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, NumaMemoryReplicaEligible(tc.entry))
		})
	}
}

func TestSelectSnapshotMemory(t *testing.T) {
	entry := &SnapshotCatalogEntry{
		Kind:       CatalogKindTemplate,
		MemoryVol:  "canonical",
		MemoryKind: CowKindVolume,
		NumaMemoryReplicas: map[string]SnapshotMemoryReplica{
			"2": {Volume: "numa2", Kind: CowKindSnapshot},
		},
	}
	vol, kind, replica := SelectSnapshotMemory(entry, 2)
	assert.Equal(t, "numa2", vol)
	assert.Equal(t, CowKindSnapshot, kind)
	assert.True(t, replica)

	vol, kind, replica = SelectSnapshotMemory(entry, 1)
	assert.Equal(t, "canonical", vol)
	assert.Equal(t, CowKindVolume, kind)
	assert.False(t, replica)
}

func TestCloneCatalogEntryCopiesNumaReplicas(t *testing.T) {
	in := &SnapshotCatalogEntry{
		NumaMemoryReplicas: map[string]SnapshotMemoryReplica{"0": {Volume: "mem0", Kind: CowKindSnapshot}},
	}
	out := cloneCatalogEntry(in)
	out.NumaMemoryReplicas["0"] = SnapshotMemoryReplica{Volume: "changed", Kind: CowKindSnapshot}
	assert.Equal(t, "mem0", in.NumaMemoryReplicas["0"].Volume)
}

func TestCreateMemoryReplicasForFullSnapshot(t *testing.T) {
	previousNodes := snapshotReplicaNumaNodes
	snapshotReplicaNumaNodes = func() []int { return []int{0, 2} }
	t.Cleanup(func() { snapshotReplicaNumaNodes = previousNodes })

	engine := &fakeCowEngine{}
	useTestCowStorage(t, engine)
	localStorage.config.NumaMemoryReplicaMode = MemoryReplicaModePerNode
	localStorage.config.NumaMemoryReplicaCreateOn = MemoryReplicaCreateOnSnapshot

	replicas, err := CreateMemoryReplicasForFullSnapshot(context.Background(), "xfs", "snap-1", &CowSnapshotObject{
		Name: "tpl-snap-1-memory",
		Kind: CowKindVolume,
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]SnapshotMemoryReplica{
		"0": {Volume: "tpl-snap-1-memory-numa0", Kind: CowKindSnapshot},
		"2": {Volume: "tpl-snap-1-memory-numa2", Kind: CowKindSnapshot},
	}, replicas)
	assert.Equal(t, [][2]string{
		{"tpl-snap-1-memory", "tpl-snap-1-memory-numa0"},
		{"tpl-snap-1-memory", "tpl-snap-1-memory-numa2"},
	}, engine.createSnapshots)
}

func TestCreateMemoryReplicasForFullSnapshotDisabled(t *testing.T) {
	engine := &fakeCowEngine{}
	useTestCowStorage(t, engine)
	localStorage.config.NumaMemoryReplicaMode = MemoryReplicaModeOff

	replicas, err := CreateMemoryReplicasForFullSnapshot(context.Background(), "xfs", "snap-1", &CowSnapshotObject{
		Name: "tpl-snap-1-memory",
		Kind: CowKindVolume,
	})
	require.NoError(t, err)
	assert.Empty(t, replicas)
	assert.Empty(t, engine.createSnapshots)
}

func TestEnsureSnapshotMemoryReplicaBackfillsCatalog(t *testing.T) {
	previousNodes := snapshotReplicaNumaNodes
	snapshotReplicaNumaNodes = func() []int { return []int{0, 2} }
	t.Cleanup(func() { snapshotReplicaNumaNodes = previousNodes })

	engine := &fakeCowEngine{}
	useTestCowStorage(t, engine)
	localStorage.config.NumaMemoryReplicaMode = MemoryReplicaModePerNode

	snapshotID := "snap-lazy-1"
	snapDir := t.TempDir()
	entry := &SnapshotCatalogEntry{
		SnapshotID:         snapshotID,
		SnapshotPath:       snapDir,
		MetaDir:            snapDir,
		RootfsVol:          "rootfs",
		MemoryVol:          "canonical",
		MemoryKind:         CowKindVolume,
		MemorySnapshotType: MemorySnapshotTypeFull,
		Kind:               CatalogKindTemplate,
	}
	require.NoError(t, WriteSnapshotCatalogFor("xfs", entry))
	t.Cleanup(func() { DeleteSnapshotCatalogFor("xfs", snapshotID) })

	updated, created, err := localStorage.ensureSnapshotMemoryReplica(context.Background(), "xfs", snapshotID, 2)
	require.NoError(t, err)
	require.True(t, created)
	require.NotNil(t, updated)
	assert.Equal(t, SnapshotMemoryReplica{Volume: "tpl-snap-lazy-1-memory-numa2", Kind: CowKindSnapshot}, updated.NumaMemoryReplicas["2"])
	assert.Equal(t, [][2]string{{"canonical", "tpl-snap-lazy-1-memory-numa2"}}, engine.createSnapshots)

	// A second acquire is a no-op and must not run FICLONE again.
	_, created, err = localStorage.ensureSnapshotMemoryReplica(context.Background(), "xfs", snapshotID, 2)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Len(t, engine.createSnapshots, 1)
}

func TestEnsureSnapshotMemoryReplicaMergesMultipleNodes(t *testing.T) {
	previousNodes := snapshotReplicaNumaNodes
	snapshotReplicaNumaNodes = func() []int { return []int{0, 2} }
	t.Cleanup(func() { snapshotReplicaNumaNodes = previousNodes })

	engine := &fakeCowEngine{}
	useTestCowStorage(t, engine)
	localStorage.config.NumaMemoryReplicaMode = MemoryReplicaModePerNode

	snapshotID := "snap-lazy-merge"
	snapDir := t.TempDir()
	entry := &SnapshotCatalogEntry{
		SnapshotID:         snapshotID,
		SnapshotPath:       snapDir,
		MetaDir:            snapDir,
		RootfsVol:          "rootfs",
		MemoryVol:          "canonical",
		MemoryKind:         CowKindVolume,
		MemorySnapshotType: MemorySnapshotTypeFull,
		Kind:               CatalogKindTemplate,
	}
	require.NoError(t, WriteSnapshotCatalogFor("xfs", entry))
	t.Cleanup(func() { DeleteSnapshotCatalogFor("xfs", snapshotID) })

	for _, node := range []int32{0, 2} {
		_, created, err := localStorage.ensureSnapshotMemoryReplica(context.Background(), "xfs", snapshotID, node)
		require.NoError(t, err)
		require.True(t, created)
	}
	fresh, err := GetLocalSnapshotFor(context.Background(), "xfs", snapshotID)
	require.NoError(t, err)
	require.NotNil(t, fresh)
	assert.Contains(t, fresh.NumaMemoryReplicas, "0")
	assert.Contains(t, fresh.NumaMemoryReplicas, "2")
}

func TestEnumerateXFSMemoryReplicaRefs(t *testing.T) {
	work := t.TempDir()
	volumes := filepath.Join(work, cow.BackendXFS, SnapshotObjectsDir, reflinkVolumesDirName)
	origin := filepath.Join(volumes, "base")
	require.NoError(t, os.MkdirAll(origin, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(origin, "tpl-snap-enum-memory-numa2"), []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(origin, "tpl-snap-enum-memory-numa3"), []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(origin, "other-memory-numa2"), []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(origin, "tpl-snap-enum-memory-numaX"), []byte("x"), 0o644))

	engine := &fakeCowEngine{}
	useTestCowStorage(t, engine)
	localStorage.config.DataPath = work

	refs, err := EnumerateXFSMemoryReplicaRefs("snap-enum")
	require.NoError(t, err)
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.Name)
		assert.Equal(t, CowKindSnapshot, ref.Kind)
		assert.Equal(t, "memory_replica", ref.Role)
	}
	assert.ElementsMatch(t, []string{"tpl-snap-enum-memory-numa2", "tpl-snap-enum-memory-numa3"}, names)
}
