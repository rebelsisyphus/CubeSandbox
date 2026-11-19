// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package cubebox

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
)

const cubeSandboxV2PoolPrefix = "/cube_sandbox_v2/sandbox/numa"

// effectiveNumaNode is the node where cgroup placement actually succeeded.
// cgroup v1 has no NUMA pool, so old sandboxes and v1 fallbacks report -1
// even when the create request asked for a node.
func effectiveNumaNode(cb *cubeboxstore.CubeBox) int32 {
	if cb == nil {
		return -1
	}
	if node, ok := numaNodeFromCgroupPath(cb.CGroupPath); ok {
		return node
	}
	return -1
}

func numaNodeFromCgroupPath(cgroupPath string) (int32, bool) {
	path := strings.TrimSpace(cgroupPath)
	rest, ok := strings.CutPrefix(path, cubeSandboxV2PoolPrefix)
	if !ok {
		return -1, false
	}
	nodeText, slot, found := strings.Cut(rest, "/")
	if !found || strings.Contains(slot, "/") {
		return -1, false
	}
	node, err := strconv.ParseInt(nodeText, 10, 32)
	if err != nil || node < 0 {
		return -1, false
	}
	if _, err := strconv.ParseUint(slot, 10, 32); err != nil {
		return -1, false
	}
	return int32(node), true
}

func addNumaPlacementLabels(labels map[string]string, box *cubeboxstore.CubeBox) {
	if labels == nil || box == nil {
		return
	}
	usePoolV2 := strings.HasPrefix(box.CGroupPath, cubeSandboxV2PoolPrefix)
	effective := effectiveNumaNode(box)
	labels[constants.LabelNumaNode] = strconv.Itoa(int(effective))
	if box.CGroupPath != "" {
		labels["cube.sandbox_cgroup_path"] = box.CGroupPath
		labels["cube.use_pool_v2"] = strconv.FormatBool(usePoolV2)
	}
	if reason := strings.TrimSpace(box.Labels["cube.numa_fallback_reason"]); reason != "" {
		labels["cube.numa_fallback_reason"] = reason
	}
	if !usePoolV2 || effective < 0 {
		return
	}
	dir := filepath.Join(hostCgroupRoot, strings.TrimPrefix(box.CGroupPath, "/"))
	if data, err := os.ReadFile(filepath.Join(dir, "cpuset.cpus.effective")); err == nil {
		labels["cube.cpuset_cpus"] = strings.TrimSpace(string(data))
	}
	if data, err := os.ReadFile(filepath.Join(dir, "cpuset.mems.effective")); err == nil {
		labels["cube.cpuset_mems"] = strings.TrimSpace(string(data))
	}
}
