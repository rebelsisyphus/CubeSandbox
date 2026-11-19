// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package cubebox

import (
	"testing"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
)

func TestNumaNodeFromCgroupPath(t *testing.T) {
	for _, tc := range []struct {
		path string
		node int32
		ok   bool
	}{
		{"/cube_sandbox_v2/sandbox/numa2/17", 2, true},
		{"/cube_sandbox_v2/sandbox/numa0/0", 0, true},
		{"/cube_sandbox/sandbox/7", -1, false},
		{"/cube_sandbox_v2/sandbox/numa2", -1, false},
		{"/cube_sandbox_v2/sandbox/numa2/17/extra", -1, false},
	} {
		node, ok := numaNodeFromCgroupPath(tc.path)
		if node != tc.node || ok != tc.ok {
			t.Fatalf("numaNodeFromCgroupPath(%q) = (%d,%v), want (%d,%v)", tc.path, node, ok, tc.node, tc.ok)
		}
	}
}

func TestEffectiveNumaNodeRejectsLegacyPool(t *testing.T) {
	box := &cubeboxstore.CubeBox{NumaNode: 2, CGroupPath: "/cube_sandbox/sandbox/7"}
	if got := effectiveNumaNode(box); got != -1 {
		t.Fatalf("legacy effective node = %d, want -1", got)
	}
	box.CGroupPath = "/cube_sandbox_v2/sandbox/numa3/9"
	if got := effectiveNumaNode(box); got != 3 {
		t.Fatalf("v2 effective node = %d, want 3", got)
	}
}

func TestAddNumaPlacementLabelsUsesEffectiveNode(t *testing.T) {
	box := &cubeboxstore.CubeBox{
		Metadata:   cubeboxstore.Metadata{Labels: map[string]string{"cube.numa_fallback_reason": "pool_full"}},
		NumaNode:   2,
		CGroupPath: "/cube_sandbox/sandbox/7",
	}
	labels := map[string]string{}
	addNumaPlacementLabels(labels, box)
	if got := labels[constants.LabelNumaNode]; got != "-1" {
		t.Fatalf("cube.numa_node = %q, want -1", got)
	}
	if got := labels["cube.use_pool_v2"]; got != "false" {
		t.Fatalf("cube.use_pool_v2 = %q, want false", got)
	}
	if got := labels["cube.numa_fallback_reason"]; got != "pool_full" {
		t.Fatalf("fallback reason = %q", got)
	}
}
