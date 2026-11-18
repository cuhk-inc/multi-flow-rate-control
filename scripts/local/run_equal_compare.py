#!/usr/bin/env python3
"""Single-flow vs 1000-flow at the same total bytes.

Total payload is 1,052,160,000 bytes in every cell.
  single: 1 x 1,052,160,000
  multi:  1000 x 1,052,160
Offered rate is 600 Mbit/s. Loss is 5% on each of the three hops.
BATS and --copy drop inside bats_line. Raw UDP drops inside hop_xfer.
TCP and wire_relay use tc netem on the data NIC only, then restore fq_codel.
"""

from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
HOP = REPO / "scripts" / "local" / "hop_xfer"
BATS = REPO / "scripts" / "local" / "run_bats_linear.py"
WIRE = REPO / "build" / "wire_relay"
LOG = REPO / "build" / "bats_linear" / "equal-compare.log"
SSH = ["ssh", "-n", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]
SCP = ["scp", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]
HOST = {
    1: "fyp1@10.10.10.161",
    2: "fyp1@10.10.10.162",
    3: "fyp1@10.10.10.163",
    4: "fyp1@10.10.10.164",
}
SINGLE = 1_052_160_000
MULTI_EACH = 1_052_160
RATE = 600
DATA = "/mnt/data/bats_cmp"
NETEM = ((1, "enp6s19"), (2, "enp6s20"), (3, "enp6s21"))


def log(msg: str) -> None:
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    print(line, flush=True)
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a") as f:
        f.write(line + "\n")


def ssh(node: int, script: str, timeout: int = 120, check: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*SSH, HOST[node], script],
        text=True,
        capture_output=True,
        timeout=timeout,
        check=check,
    )


def netem(on: bool) -> None:
    for node, dev in NETEM:
        if on:
            cmd = f"sudo -n tc qdisc replace dev {dev} root netem loss 5%"
        else:
            cmd = f"sudo -n tc qdisc replace dev {dev} root fq_codel"
        proc = ssh(node, cmd, timeout=30)
        if proc.returncode != 0:
            raise SystemExit(f"netem {dev} failed: {proc.stderr[-400:]}")
    log("netem " + ("5% on data NICs" if on else "cleared"))


def install() -> None:
    subprocess.run(
        ["gcc", "-std=c11", "-O2", "-Wall", "-Wextra", "-pthread", "-o", str(HOP), str(HOP.with_suffix(".c"))],
        check=True,
    )
    for node in HOST:
        subprocess.run([*SCP, str(HOP), f"{HOST[node]}:/tmp/hop_xfer"], check=True, timeout=60)
        if WIRE.exists():
            subprocess.run([*SCP, str(WIRE), f"{HOST[node]}:/tmp/wire_relay"], check=True, timeout=60)
        ssh(node, "chmod +x /tmp/hop_xfer /tmp/wire_relay 2>/dev/null; mkdir -p " + DATA, timeout=30)
    log("binaries installed")


def stop_hop() -> None:
    for node in HOST:
        ssh(node, "pkill -x hop_xfer >/dev/null 2>&1 || true", timeout=20)


def run_bats(tag: str, flows: int, nbytes: int, loss: int, copy: bool) -> None:
    env = os.environ.copy()
    env.update(
        {
            "BATS_RATE": str(RATE),
            "BATS_LOSS": str(loss),
            "BATS_FLOWS": str(flows),
            "BATS_BYTES": str(nbytes),
            "BATS_K": "128",
            "BATS_T": "1370",
            "BATS_ONE_SOURCE": "1",
            "BATS_WORKERS": "1",
            "BATS_DEST_WORKERS": "2",
            "BATS_DATA": DATA,
            "BATS_WAIT_SEC": "240",
            "BATS_SRC_TIMEOUT": "300",
            "BATS_IDLE": "8",
            "BATS_TAG": tag,
            "BATS_COPY": "1" if copy else "0",
            "BATS_RECORD": "1",
            "BATS_KEEP_FILES": "0",
        }
    )
    log(f"BEGIN {tag}")
    try:
        proc = subprocess.run([sys.executable, str(BATS)], text=True, capture_output=True, timeout=900, env=env)
    except subprocess.TimeoutExpired:
        log(f"END {tag} timeout")
        return
    text = proc.stdout + "\n" + proc.stderr
    with LOG.open("a") as f:
        f.write(text)
        if not text.endswith("\n"):
            f.write("\n")
    interesting = [
        ln
        for ln in text.splitlines()
        if ln.startswith(("PASS", "FAIL", "src_", "r1_", "r2_", "dest_", "drops ", "sha_"))
        or "goodput" in ln
        or "sha_bad" in ln
    ]
    log(f"END {tag} rc={proc.returncode}")
    for ln in interesting[-20:]:
        log("  " + ln)


def start_bg(node: int, cmd: str, logfile: str) -> None:
    remote = f"ulimit -n 8192; rm -f {logfile}; nohup {cmd} >{logfile} 2>&1 &"
    ssh(node, remote, timeout=30)


def wait_log(node: int, logfile: str, needle: str, timeout: int) -> str:
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        proc = ssh(node, f"cat {logfile} 2>/dev/null || true", timeout=30)
        last = proc.stdout
        if needle in last:
            return last
        time.sleep(1)
    return last


def run_hop(tag: str, mode: str, flows: int, nbytes: int, loss: int) -> None:
    log(f"BEGIN {tag}")
    stop_hop()
    remote_dir = f"{DATA}/{tag}"
    ssh(4, f"rm -rf {remote_dir}; mkdir -p {remote_dir}", timeout=30)
    tcp = mode == "tcp"
    flag = "--tcp" if tcp else ""
    hop_loss = 0 if tcp else loss
    start_bg(
        4,
        f"/tmp/hop_xfer dest {flag} --bind 10.40.40.2:{19340 if tcp else 19240} "
        f"--flows {flows} --bytes {nbytes} --dir {remote_dir}",
        "/tmp/hx_dest.log",
    )
    time.sleep(0.4)
    if tcp:
        start_bg(
            3,
            "/tmp/hop_xfer relay --tcp --bind 10.30.30.2:19330 --bind-ip 10.40.40.1 --to 10.40.40.2:19340",
            "/tmp/hx_r2.log",
        )
        start_bg(
            2,
            "/tmp/hop_xfer relay --tcp --bind 10.20.20.2:19320 --bind-ip 10.30.30.1 --to 10.30.30.2:19330",
            "/tmp/hx_r1.log",
        )
    else:
        start_bg(
            3,
            f"/tmp/hop_xfer relay --bind 10.30.30.2:19230 --send-bind 10.40.40.1 --to 10.40.40.2:19240 "
            f"--flows {flows} --loss {hop_loss}",
            "/tmp/hx_r2.log",
        )
        start_bg(
            2,
            f"/tmp/hop_xfer relay --bind 10.20.20.2:19220 --send-bind 10.30.30.1 --to 10.30.30.2:19230 "
            f"--flows {flows} --loss {hop_loss}",
            "/tmp/hx_r1.log",
        )
    if not wait_log(4, "/tmp/hx_dest.log", "listen=", 20).strip():
        log(f"END {tag} dest did not listen")
        stop_hop()
        return
    src = (
        f"ulimit -n 8192; /tmp/hop_xfer source {flag} --bind 10.20.20.1 "
        f"--to 10.20.20.2:{19320 if tcp else 19220} --flows {flows} --bytes {nbytes} "
        f"--rate-mbps {RATE} --loss {hop_loss}"
    )
    try:
        proc = ssh(1, src, timeout=500)
    except subprocess.TimeoutExpired as exc:
        log(f"END {tag} source timeout")
        log("  " + (exc.stdout or "")[-500:] if isinstance(exc.stdout, str) else f"END {tag} source timeout")
        stop_hop()
        return
    with LOG.open("a") as f:
        f.write(proc.stdout + proc.stderr + "\n")
    dest = wait_log(4, "/tmp/hx_dest.log", "elapsed_ms=", 120)
    check = ssh(
        4,
        f"/tmp/hop_xfer check --dir {remote_dir} --flows {flows} --bytes {nbytes}",
        timeout=180,
    )
    relays = ssh(2, "cat /tmp/hx_r1.log 2>/dev/null || true", timeout=20).stdout
    relays += ssh(3, "cat /tmp/hx_r2.log 2>/dev/null || true", timeout=20).stdout
    blob = proc.stdout + "\n" + dest + "\n" + check.stdout + "\n" + relays
    with LOG.open("a") as f:
        f.write(blob + "\n")
    for ln in blob.splitlines():
        if ln.startswith("role="):
            log("  " + ln)
    log(f"END {tag} rc={proc.returncode} check_rc={check.returncode}")
    ssh(4, f"rm -rf {remote_dir}", timeout=60)
    stop_hop()


def run_wire(tag: str, codec: str) -> None:
    log(f"BEGIN {tag}")
    for node in HOST:
        ssh(node, "pkill -x wire_relay >/dev/null 2>&1 || true", timeout=20)
    ssh(1, f"test -f {DATA}/wire-in.bin || dd if=/dev/urandom of={DATA}/wire-in.bin bs=1M count=1004 status=none", timeout=180)
    # 1003 MiB is larger than the BATS payload. Trim to the exact total.
    ssh(1, f"truncate -s {SINGLE} {DATA}/wire-in.bin", timeout=30)
    out = f"{DATA}/wire-out.bin"
    ssh(4, f"rm -f {out}", timeout=30)
    common = "--listen 9000 --idle-exit-sec 150"
    start_bg(
        4,
        f"/tmp/wire_relay --local-node-id 4 {common} --next-hop 127.0.0.1:9 "
        f"--local-decode --codec {codec} --output {out}",
        "/tmp/wire_dest.log",
    )
    time.sleep(0.3)
    start_bg(3, f"/tmp/wire_relay --local-node-id 3 {common} --next-hop 10.40.40.2:9000", "/tmp/wire_r2.log")
    start_bg(2, f"/tmp/wire_relay --local-node-id 2 {common} --next-hop 10.30.30.2:9000", "/tmp/wire_r1.log")
    time.sleep(0.5)
    src = (
        f"/tmp/wire_relay --local-node-id 1 {common} --next-hop 10.20.20.2:9000 "
        f"--source {DATA}/wire-in.bin --final-dst 4 --ttl 8 --codec {codec} --rate-mbps {RATE}"
    )
    t0 = time.monotonic()
    try:
        proc = ssh(1, src, timeout=500)
    except subprocess.TimeoutExpired:
        log(f"END {tag} source timeout")
        for node in HOST:
            ssh(node, "pkill -x wire_relay >/dev/null 2>&1 || true", timeout=20)
        return
    elapsed = time.monotonic() - t0
    sha_src = ssh(1, f"sha256sum {DATA}/wire-in.bin", timeout=120).stdout.strip()
    sha_dst = ssh(4, f"sha256sum {out} 2>/dev/null || echo MISSING", timeout=120).stdout.strip()
    same = sha_src.split()[0:1] == sha_dst.split()[0:1] and sha_dst and "MISSING" not in sha_dst
    goodput = (SINGLE * 8 / 1e6) / elapsed if same and elapsed > 0 else 0
    log(f"  wire codec={codec} sha={int(same)} wall_s={elapsed:.1f} goodput_mbps={goodput:.1f}")
    log(f"  src {sha_src[:80]}")
    log(f"  dst {sha_dst[:80]}")
    tail = ssh(1, "tail -n 5 /tmp/wire_src.log 2>/dev/null || true", timeout=20).stdout
    # source was foreground, its stdout is proc
    text = (proc.stdout + proc.stderr)[-1500:]
    with LOG.open("a") as f:
        f.write(text + "\n")
    for ln in text.splitlines()[-8:]:
        log("  " + ln)
    log(f"END {tag}")
    ssh(4, f"rm -f {out}", timeout=30)
    for node in HOST:
        ssh(node, "pkill -x wire_relay >/dev/null 2>&1 || true", timeout=20)
    _ = tail


def main() -> None:
    LOG.write_text("")
    install()
    try:
        cells = []
        for flows, nbytes, kind in ((1, SINGLE, "s"), (1000, MULTI_EACH, "m")):
            for loss in (0, 5):
                cells.append((f"bats-{kind}-{loss}", flows, nbytes, loss, False))
                cells.append((f"copy-{kind}-{loss}", flows, nbytes, loss, True))
        for tag, flows, nbytes, loss, copy in cells:
            run_bats(tag, flows, nbytes, loss, copy)
        for kind, flows, nbytes in (("s", 1, SINGLE), ("m", 1000, MULTI_EACH)):
            for loss in (0, 5):
                run_hop(f"udp-{kind}-{loss}", "udp", flows, nbytes, loss)
        for kind, flows, nbytes in (("s", 1, SINGLE), ("m", 1000, MULTI_EACH)):
            run_hop(f"tcp-{kind}-0", "tcp", flows, nbytes, 0)
        netem(True)
        try:
            for kind, flows, nbytes in (("s", 1, SINGLE), ("m", 1000, MULTI_EACH)):
                run_hop(f"tcp-{kind}-5", "tcp", flows, nbytes, 5)
            if WIRE.exists():
                for codec in ("copy", "rs", "wirehair"):
                    run_wire(f"wire-{codec}-s-5", codec)
        finally:
            netem(False)
        if WIRE.exists():
            for codec in ("copy", "rs", "wirehair"):
                run_wire(f"wire-{codec}-s-0", codec)
    finally:
        netem(False)
        stop_hop()
    log("campaign done")


if __name__ == "__main__":
    main()
