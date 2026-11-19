// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

//! Join the sandbox process into the pool cgroup that Cubelet assigned for
//! it, BEFORE the VMM starts, so vCPU/VMM threads inherit the target
//! cpuset. Cubelet's late
//! setCgroup remains the idempotent fallback.

use std::fs;
use std::path::Path;

pub const CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// Only Cubelet's pool paths are accepted: a foreign or malformed
/// cube.sandbox_cgroup_path annotation must not steer this process into an
/// arbitrary cgroup.
fn valid_pool_path(path: &str) -> bool {
    let p = path.trim();
    if p.contains("..") {
        return false;
    }

    let v2 = p
        .strip_prefix("/cube_sandbox_v2/sandbox/numa")
        .map(|rest| {
            let mut parts = rest.split('/');
            let node = parts.next().unwrap_or_default();
            let slot = parts.next().unwrap_or_default();
            node.parse::<u32>().is_ok() && slot.parse::<u32>().is_ok() && parts.next().is_none()
        })
        .unwrap_or(false);

    let v1 = p
        .strip_prefix("/cube_sandbox/sandbox/")
        .map(|slot| !slot.contains('/') && slot.parse::<u32>().is_ok())
        .unwrap_or(false);

    v2 || v1
}

/// Join this process into the pool cgroup at `path` (relative to the cgroup
/// root, as produced by Cubelet's MakeCgroupPathByID).
pub fn join(path: &str) -> Result<(), String> {
    let path = path.trim();
    if !valid_pool_path(path) {
        return Err(format!("invalid pool cgroup path {path:?}"));
    }

    let pid = std::process::id().to_string();
    let root = Path::new(CGROUP_ROOT);
    if root.join("cgroup.controllers").exists() {
        // cgroup v2: one tree, one write covers all controllers.
        let dir = root.join(path.trim_start_matches('/'));
        if !dir.is_dir() {
            return Err(format!("cgroup path {} does not exist", dir.display()));
        }
        return write_procs(&dir, &pid);
    }

    // cgroup v1: one tree per controller; the group exists only in the
    // trees Cubelet attached at creation -- write those, skip the rest.
    let mut joined = false;
    let mut last_err = None;
    for ctrl in ["cpu", "memory", "cpuset"] {
        let dir = root.join(ctrl).join(path.trim_start_matches('/'));
        if dir.is_dir() {
            match write_procs(&dir, &pid) {
                Ok(()) => joined = true,
                Err(e) => last_err = Some(e),
            }
        }
    }
    if joined {
        Ok(())
    } else {
        Err(last_err.unwrap_or_else(|| "no matching cgroup v1 controller tree".to_string()))
    }
}

fn write_procs(dir: &Path, pid: &str) -> Result<(), String> {
    let procs = dir.join("cgroup.procs");
    if let Ok(current) = fs::read_to_string(&procs) {
        let current = current.trim();
        if !current.is_empty() && current != pid {
            return Err(format!(
                "cgroup {} already contains pids: {}",
                dir.display(),
                current
            ));
        }
    }
    fs::write(&procs, pid).map_err(|e| format!("write {}: {e}", procs.display()))
}

#[cfg(test)]
mod tests {
    use super::valid_pool_path;

    #[test]
    fn accepts_pool_paths() {
        assert!(valid_pool_path("/cube_sandbox_v2/sandbox/numa0/5"));
        assert!(valid_pool_path("/cube_sandbox_v2/sandbox/numa3/4000"));
        assert!(valid_pool_path("/cube_sandbox/sandbox/7"));
    }

    #[test]
    fn rejects_foreign_paths() {
        assert!(!valid_pool_path("/sys/fs/cgroup"));
        assert!(!valid_pool_path(
            "/cube_sandbox_v2/sandbox/numa0/../numa1/1"
        ));
        assert!(!valid_pool_path("/cube_sandbox/sandbox/"));
        assert!(!valid_pool_path(
            "/cube_sandbox_v2/sandbox/numa0/not-a-slot"
        ));
    }
}
