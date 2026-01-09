#!/usr/bin/env python3
"""
Full-sync orchestrator for Optio node with height-based binary switching.

- Reads manifests/optio-upgrades.yaml
- Ensures binaries exist (optionally downloads and verifies sha256)
- Runs the node
- Polls CometBFT RPC /status for latest block height
- Stops + switches binary at configured heights
"""

from __future__ import annotations

import hashlib
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import urllib.request
import json

try:
    import yaml  # pip install pyyaml
except ImportError:
    print("Missing dependency: PyYAML. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(2)


@dataclass
class DownloadSpec:
    url: str
    sha256: str = ""
    mode: str = "direct"


@dataclass
class VersionSpec:
    version: str
    start_height: int
    end_height_exclusive: Optional[int]
    download: DownloadSpec
    exec_args: list[str]


@dataclass
class Manifest:
    chain_id: str
    home_dir: Path
    bin_dir: Path
    binary_name: str
    rpc_url: str
    versions: list[VersionSpec]


def load_manifest(path: Path) -> Manifest:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))

    versions: list[VersionSpec] = []
    for v in data["versions"]:
        dl = v.get("download", {}) or {}
        versions.append(
            VersionSpec(
                version=str(v["version"]),
                start_height=int(v["start_height"]),
                end_height_exclusive=None if v.get("end_height_exclusive") is None else int(v["end_height_exclusive"]),
                download=DownloadSpec(
                    url=str(dl.get("url", "") or ""),
                    sha256=str(dl.get("sha256", "") or ""),
                    mode=str(dl.get("mode", "direct") or "direct"),
                ),
                exec_args=[str(x) for x in (v.get("exec_args") or [])],
            )
        )

    # Validate ordering / overlaps
    versions_sorted = sorted(versions, key=lambda x: x.start_height)
    if versions_sorted != versions:
        raise ValueError("Manifest versions must be ordered by start_height ascending.")

    for i in range(len(versions) - 1):
        cur = versions[i]
        nxt = versions[i + 1]
        if cur.end_height_exclusive is None:
            raise ValueError(f"{cur.version} has end_height_exclusive=null but is not the last entry.")
        if cur.end_height_exclusive != nxt.start_height:
            raise ValueError(
                f"Gap/overlap: {cur.version}.end_height_exclusive={cur.end_height_exclusive} "
                f"must equal {nxt.version}.start_height={nxt.start_height}"
            )

    return Manifest(
        chain_id=str(data["chain_id"]),
        home_dir=Path(str(data["home_dir"])),
        bin_dir=Path(str(data["bin_dir"])),
        binary_name=str(data.get("binary_name", "optiod")),
        rpc_url=str(data.get("rpc_url", "http://127.0.0.1:26657")),
        versions=versions,
    )


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def download_file(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url) as r:
        if r.status != 200:
            raise RuntimeError(f"Download failed with HTTP {r.status}: {url}")
        with dest.open("wb") as f:
            while True:
                chunk = r.read(1024 * 256)
                if not chunk:
                    break
                f.write(chunk)


def ensure_binary(manifest: Manifest, spec: VersionSpec) -> Path:
    """
    Returns path to versioned binary: <bin_dir>/<version>/<binary_name>
    Downloads if needed and URL is provided.
    """
    target = manifest.bin_dir / spec.version / manifest.binary_name
    if target.exists():
        return target

    if not spec.download.url:
        raise FileNotFoundError(
            f"Missing binary {target}. Place it manually or set download.url for {spec.version}."
        )

    print(f"[download] {spec.version}: {spec.download.url}")
    download_file(spec.download.url, target)
    target.chmod(target.stat().st_mode | 0o111)

    if spec.download.sha256:
        actual = sha256_file(target)
        expected = spec.download.sha256.lower().strip()
        if actual.lower() != expected:
            raise RuntimeError(
                f"SHA256 mismatch for {target}\nexpected: {expected}\nactual:   {actual}"
            )
        print(f"[sha256] OK for {spec.version}")

    return target


def rpc_status_height(rpc_url: str, timeout_sec: float = 3.0) -> int:
    """
    Calls CometBFT RPC /status and returns latest_block_height as int.
    """
    url = rpc_url.rstrip("/") + "/status"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout_sec) as r:
        body = r.read().decode("utf-8", errors="replace")
    j = json.loads(body)
    height_str = j["result"]["sync_info"]["latest_block_height"]
    return int(height_str)


def graceful_stop(proc: subprocess.Popen, timeout: float = 45.0) -> None:
    """
    Try SIGINT, then SIGTERM, then SIGKILL.
    """
    if proc.poll() is not None:
        return

    print("[stop] Sending SIGINT...")
    proc.send_signal(signal.SIGINT)
    t0 = time.time()
    while time.time() - t0 < timeout:
        if proc.poll() is not None:
            print("[stop] Process exited cleanly.")
            return
        time.sleep(0.5)

    print("[stop] SIGINT timeout, sending SIGTERM...")
    proc.terminate()
    t0 = time.time()
    while time.time() - t0 < 10:
        if proc.poll() is not None:
            print("[stop] Process exited after SIGTERM.")
            return
        time.sleep(0.5)

    print("[stop] SIGTERM timeout, sending SIGKILL...")
    proc.kill()


def start_node(binary_path: Path, args: list[str], env: dict[str, str]) -> subprocess.Popen:
    cmd = [str(binary_path)] + args
    print(f"[start] {' '.join(cmd)}")
    # Stream logs to console; you can redirect to file if you want.
    return subprocess.Popen(
        cmd,
        stdout=sys.stdout,
        stderr=sys.stderr,
        env=env,
    )


def select_version(manifest: Manifest, height: int) -> VersionSpec:
    for v in manifest.versions:
        if height < v.start_height:
            continue
        if v.end_height_exclusive is None:
            return v
        if v.start_height <= height < v.end_height_exclusive:
            return v
    # If below first start_height, use first
    return manifest.versions[0]


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: scripts/fullsync_orchestrator.py manifests/optio-upgrades.yaml", file=sys.stderr)
        return 2

    manifest_path = Path(sys.argv[1]).resolve()
    manifest = load_manifest(manifest_path)

    manifest.home_dir.mkdir(parents=True, exist_ok=True)
    manifest.bin_dir.mkdir(parents=True, exist_ok=True)

    # Ensure all binaries exist (download if configured)
    for v in manifest.versions:
        ensure_binary(manifest, v)

    # Determine starting version based on current height if any
    current_height = 0
    try:
        current_height = rpc_status_height(manifest.rpc_url)
        print(f"[rpc] Node already running? current height = {current_height}")
    except Exception:
        print("[rpc] No running node detected (or RPC not reachable yet). Starting from configured first version.")

    # If node already running, we won't attempt to manage it; safest is to stop it yourself.
    # For initial bootstrap, run this when node is NOT already running.
    if current_height > 0:
        print("[warn] RPC responded, so a node is likely already running.")
        print("       Stop it first, then re-run orchestrator for clean management.")
        return 1

    # Environment
    env = os.environ.copy()

    # Start with first version
    active_spec = manifest.versions[0]
    active_bin = manifest.bin_dir / active_spec.version / manifest.binary_name
    proc = start_node(active_bin, active_spec.exec_args, env)

    poll_sec = 5.0
    last_height = -1

    try:
        while True:
            # If process died unexpectedly, surface it
            rc = proc.poll()
            if rc is not None:
                raise RuntimeError(f"Node process exited unexpectedly with code {rc} (was running {active_spec.version}).")

            try:
                h = rpc_status_height(manifest.rpc_url)
            except Exception as e:
                print(f"[rpc] status failed: {e!r} (retrying in {poll_sec}s)")
                time.sleep(poll_sec)
                continue

            if h != last_height:
                print(f"[height] {h} (running {active_spec.version})")
                last_height = h

            desired = select_version(manifest, h)

            # Switch if needed
            if desired.version != active_spec.version:
                print(f"[switch] Height {h} => switching {active_spec.version} -> {desired.version}")
                graceful_stop(proc)

                active_spec = desired
                active_bin = manifest.bin_dir / active_spec.version / manifest.binary_name
                proc = start_node(active_bin, active_spec.exec_args, env)

                # Give it a moment to come back up
                time.sleep(2.0)

            time.sleep(poll_sec)

    except KeyboardInterrupt:
        print("\n[exit] Ctrl+C received.")
        graceful_stop(proc)
        return 0
    except Exception as e:
        print(f"[error] {e}", file=sys.stderr)
        try:
            graceful_stop(proc)
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
