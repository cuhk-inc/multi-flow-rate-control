#!/usr/bin/env python3
"""Run real BATS on the linear VM path.

VM1 source --10.20.20--> VM2 relay --10.30.30--> VM3 relay --10.40.40--> VM4 dest

Each hop calls bats_encode / bats_recode / bats_decode from ../bats.
This is not wire_relay --bats-recoder identity.

Env:
  BATS_K=128
  BATS_T=256
  BATS_BYTES=32768
  BATS_LOSS=0          # artificial DATA loss percent on source and both relays
  BATS_RATE=200
  BATS_COPY=0          # 1: --copy, do not call encode/recode/decode
  BATS_RECORD=0        # 1: keep going when SHA-256 fails and print checkpoints
  BATS_SPLIT=0         # first flow id sent to the second destination
  BATS_DEST2=          # user@host of the second destination VM
  BATS_DEST2_BIND=     # address the last relay sends that half to
  BATS_DEST2_PORT=19041

Loss is applied on the source and both relays, once per hop, before send.
It is not applied on the destination. Five percent per hop compounds across
the three hops.

The long-file comparison uses the October run, not these defaults:
K=128, T=1370, 104865280 bytes per flow, 1000 flows, rate 1499, workers 1.
Do not use 1800: that rate already drops on relay 2.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bats_checkpoints import dest_row, summarize  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
BIN = REPO / "build" / "bats_line"
REMOTE_BIN = "/home/fyp1/work/multi-flow-rate-control/build/bats_line"
SSH_OPTS = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]
HOST = {
    1: "fyp1@10.10.10.161",
    2: "fyp1@10.10.10.162",
    3: "fyp1@10.10.10.163",
    4: "fyp1@10.10.10.164",
}

K = int(os.environ.get("BATS_K", "128"))
T = int(os.environ.get("BATS_T", "256"))
NBYTES = int(os.environ.get("BATS_BYTES", "32768"))
LOSS = int(os.environ.get("BATS_LOSS", "0"))
RATE = int(os.environ.get("BATS_RATE", "200"))
BATCHES = int(os.environ.get("BATS_BATCHES", "0"))
FLOWS = int(os.environ.get("BATS_FLOWS", "1"))
IDLE = int(os.environ.get("BATS_IDLE", "30"))
WAIT_SEC = int(os.environ.get("BATS_WAIT_SEC", "40"))
SRC_TIMEOUT = int(os.environ.get("BATS_SRC_TIMEOUT", "180"))
COPY = os.environ.get("BATS_COPY", "0") == "1"
GSO_OFF = os.environ.get("BATS_GSO", "1") == "0"
# 源和目的装同一份度数。中继不抽度数，不能带 --psi。
PSI = os.environ.get("BATS_PSI", "").strip()
# 只在目的端：这一轮仍用装入的度数，跑完按秩分布把新度数写到这个路径。
FIT = os.environ.get("BATS_FIT", "").strip()
# 按代 NACK：基线 batch 照发，译不动再补。中继在下游网卡上收，再交回上一跳。
NACK = os.environ.get("BATS_NACK", "0") == "1"
RECORD = os.environ.get("BATS_RECORD", "0") == "1"
SPLIT = int(os.environ.get("BATS_SPLIT", "0"))
DEST2 = os.environ.get("BATS_DEST2", "").strip()
DEST2_BIND = os.environ.get("BATS_DEST2_BIND", "").strip()
DEST2_PORT = int(os.environ.get("BATS_DEST2_PORT", "19041"))
DATA = os.environ.get("BATS_DATA", "/mnt/data/bats_run").rstrip("/")
ONE_SOURCE = os.environ.get("BATS_ONE_SOURCE", "0") == "1"
WORKERS = os.environ.get("BATS_WORKERS", "").strip()
SCALAR = os.environ.get("BATS_SCALAR", "").strip()
# 非空且不是 0 时，四台机器上的 bats_line 都改走标量 gf_axpy。
SCALAR_PREFIX = "" if SCALAR in ("", "0") else f"BATS_SCALAR={SCALAR} "
# 上次跑通的长文件用了 2 个目的端译码线程。源端和中继保持 BATS_WORKERS。
# 目的端只有 1 个线程时，1499 Mbit/s 会超过一个核，套接字丢掉整段 40 包的 GSO。
DEST_WORKERS = os.environ.get("BATS_DEST_WORKERS", "2").strip()
PORT = {2: 19020, 3: 19030, 4: 19040}
BIND = {2: "10.20.20.2", 3: "10.30.30.2", 4: "10.40.40.2"}
NEXT = {1: "10.20.20.2:19020", 2: "10.30.30.2:19030", 3: "10.40.40.2:19040"}


def run(cmd: list[str], check: bool = True, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if check and proc.returncode != 0:
        sys.stderr.write("FAIL: " + " ".join(cmd) + "\n")
        sys.stderr.write(proc.stdout[-2000:] + proc.stderr[-2000:])
        raise SystemExit(proc.returncode or 1)
    return proc


def ssh(node: int, script: str, check: bool = True, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return ssh_at(HOST[node], script, check=check, timeout=timeout)


def ssh_at(host: str, script: str, check: bool = True, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return run(["ssh", "-n", *SSH_OPTS, host, script], check=check, timeout=timeout)


def wait_log_at(host: str, path: str, pat: str, n: int = 80) -> bool:
    q = pat.replace("'", "'\\''")
    for _ in range(n):
        if ssh_at(host, f"grep -q '{q}' {path}", check=False).returncode == 0:
            return True
        time.sleep(0.1)
    return False


def wait_log(node: int, path: str, pat: str, n: int = 80) -> bool:
    return wait_log_at(HOST[node], path, pat, n)


def stop() -> None:
    hosts = [HOST[node] for node in (1, 2, 3, 4)]
    if DEST2:
        hosts.append(DEST2)
    for host in hosts:
        ssh_at(host, "pkill -f '[b]ats_line' >/dev/null 2>&1 || true", check=False)
    time.sleep(0.3)


def main() -> None:
    if SPLIT and (not DEST2 or not DEST2_BIND or SPLIT >= FLOWS):
        raise SystemExit("BATS_SPLIT needs BATS_DEST2, BATS_DEST2_BIND, and a split below BATS_FLOWS")
    if not BIN.is_file():
        raise SystemExit(f"missing {BIN}; make bats-line first")
    out_dir = REPO / "build" / "bats_linear"
    tag = os.environ.get("BATS_TAG", "").strip()
    if tag:
        out_dir = out_dir / tag
    out_dir.mkdir(parents=True, exist_ok=True)

    stop()
    if os.environ.get("BATS_SKIP_SCP", "0") != "1":
        print("copy binary")
        hosts = [HOST[node] for node in (1, 2, 3, 4)]
        if DEST2:
            hosts.append(DEST2)
        for host in hosts:
            run(["scp", *SSH_OPTS, str(BIN), f"{host}:{REMOTE_BIN}"])

    print("preflight linear hops")

    def ping_hop(node: int, cmd: str) -> None:
        for _ in range(3):
            if ssh(node, cmd, check=False).returncode == 0:
                return
            time.sleep(0.5)
        ssh(node, cmd)

    ping_hop(1, "ping -c 2 -W 2 10.20.20.2 >/dev/null")
    ping_hop(2, "ping -c 2 -W 2 10.30.30.2 >/dev/null")
    ping_hop(3, "ping -c 2 -W 2 10.40.40.2 >/dev/null")

    stop()
    ssh(1, f"mkdir -p {DATA}", timeout=30)
    ssh(4, f"mkdir -p {DATA}", timeout=30)
    n_inputs = 1 if ONE_SOURCE else FLOWS
    ssh(
        1,
        f"""python3 - <<'PY'
flows = {n_inputs}
nbytes = {NBYTES}
for flow in range(flows):
    block = bytes(((flow + 1) * i * 17 + 3 + flow) & 255 for i in range(1024 * 1024))
    path = '{DATA}/in.bin' if flow == 0 else f'{DATA}/in.bin.{{flow}}'
    left = nbytes
    with open(path, 'wb') as f:
        while left:
            n = min(left, len(block))
            f.write(block[:n])
            left -= n
print(flows, nbytes)
PY""",
        timeout=600,
    )

    remote_in = f"{DATA}/in.bin"
    remote_out = f"{DATA}/out.bin"
    logs = {
        4: "/tmp/bats_line_dest.log",
        3: "/tmp/bats_line_relay2.log",
        2: "/tmp/bats_line_relay1.log",
        1: "/tmp/bats_line_source.log",
    }
    for node in (1, 2, 3, 4):
        ssh(node, f"rm -f {logs[node]}", check=False)
    ssh(4, f"rm -f {DATA}/out.bin {DATA}/out.bin.[0-9]*", check=False, timeout=120)
    ssh(1, "rm -f /tmp/bats_line_source.log /tmp/bats_line_source.*.log", check=False)

    worker = f" --workers {WORKERS}" if WORKERS else ""
    dest_worker = f" --workers {DEST_WORKERS}" if DEST_WORKERS else worker
    copy = " --copy" if COPY else ""
    gso = " --gso-off" if GSO_OFF else ""
    batches = f" --batches {BATCHES}" if BATCHES else ""
    reorder_ms = 3 if NACK else 10
    nack_flag = " --nack" if NACK else ""
    base = (
        f"--k {K} --t {T} --seed 0xB175 --reorder-ms {reorder_ms} --idle-sec {IDLE} "
        f"--loss-percent {LOSS} --rate-mbps {RATE}{batches}{copy}{gso}{nack_flag}"
    )
    common = f"{base}{worker}"
    psi = f" --psi {PSI}" if PSI else ""
    fit = f" --fit-psi {FIT}" if FIT else ""
    src_common = f"{common}{psi}"
    dest_common = f"{base}{dest_worker}{psi}{fit}"
    hop = f"{common} --flows {FLOWS}"
    dest_flows = SPLIT if SPLIT else FLOWS
    split_args = ""
    if SPLIT:
        split_args = f" --next2 {DEST2_BIND}:{DEST2_PORT} --split-flow {SPLIT}"
    ssh(
        4,
        f"ulimit -n 8192; {SCALAR_PREFIX}nohup {REMOTE_BIN} --role dest --bind {BIND[4]} --listen {PORT[4]} "
        f"--output {remote_out} {dest_common} --flows {dest_flows}"
        f"{' --prev 10.40.40.1:19131' if NACK else ''} >{logs[4]} 2>&1 &",
    )
    if not wait_log(4, logs[4], "bats-line: listening"):
        ssh(4, f"cat {logs[4]}", check=False)
        raise SystemExit("dest did not listen")
    if SPLIT:
        ssh_at(
            DEST2,
            f"rm -f /tmp/bats_line_dest2.log /tmp/bats_out.bin /tmp/bats_out.bin.[0-9]*; "
            f"{SCALAR_PREFIX}nohup {REMOTE_BIN} --role dest --bind {DEST2_BIND} --listen {DEST2_PORT} "
            f"--output {remote_out} {dest_common} --flows {FLOWS - SPLIT} "
            f">/tmp/bats_line_dest2.log 2>&1 &",
        )
        if not wait_log_at(DEST2, "/tmp/bats_line_dest2.log", "bats-line: listening"):
            ssh_at(DEST2, "cat /tmp/bats_line_dest2.log", check=False)
            raise SystemExit("dest2 did not listen")
    ssh(
        3,
        f"ulimit -n 8192; {SCALAR_PREFIX}nohup {REMOTE_BIN} --role relay --bind {BIND[3]} --listen {PORT[3]} "
        f"--next {NEXT[3]}{split_args} --recode-seed 22 --loss-seed 3 {hop}"
        f"{' --prev 10.30.30.1:19121 --nack-listen 10.40.40.1:19131' if NACK else ''} >{logs[3]} 2>&1 &",
    )
    if not wait_log(3, logs[3], "bats-line: listening"):
        ssh(3, f"cat {logs[3]}", check=False)
        raise SystemExit("relay2 did not listen")
    ssh(
        2,
        f"ulimit -n 8192; {SCALAR_PREFIX}nohup {REMOTE_BIN} --role relay --bind {BIND[2]} --listen {PORT[2]} "
        f"--next {NEXT[2]} --recode-seed 11 --loss-seed 2 {hop}"
        f"{' --prev 10.20.20.1:19010 --nack-listen 10.30.30.1:19121' if NACK else ''} >{logs[2]} 2>&1 &",
    )
    if not wait_log(2, logs[2], "bats-line: listening"):
        ssh(2, f"cat {logs[2]}", check=False)
        raise SystemExit("relay1 did not listen")

    t0 = time.monotonic()
    if ONE_SOURCE:
        src_script = (
            f"ulimit -n 8192; {SCALAR_PREFIX}{REMOTE_BIN} --role source --bind 10.20.20.1 --flow-id 0 --flows {FLOWS} "
            f"{'--listen 19010 ' if NACK else ''}"
            f"--input {remote_in} --next {NEXT[1]} --loss-seed 1 "
            f"{src_common} >/tmp/bats_line_source.0.log 2>&1\n"
            "cat /tmp/bats_line_source.0.log > /tmp/bats_line_source.log\n"
        )
    else:
        if NACK and FLOWS != 1:
            raise SystemExit("NACK 这一版只跑一条流")
        src_script = "fail=0\npids=\n"
        for flow in range(FLOWS):
            remote_flow_in = remote_in if flow == 0 else f"{remote_in}.{flow}"
            src_script += (
                f"{SCALAR_PREFIX}{REMOTE_BIN} --role source --bind 10.20.20.1 --flow-id {flow} "
                f"{'--listen 19010 ' if NACK else ''}"
                f"--input {remote_flow_in} --next {NEXT[1]} --loss-seed {flow + 1} "
                f"{src_common} >/tmp/bats_line_source.{flow}.log 2>&1 &\n"
                "pids=\"$pids $!\"\n"
            )
        src_script += (
            "for p in $pids; do wait \"$p\" || fail=1; done\n"
            "cat /tmp/bats_line_source.*.log > /tmp/bats_line_source.log\n"
            "exit $fail\n"
        )
    src = ssh(1, src_script, check=False, timeout=SRC_TIMEOUT)
    if src.returncode != 0 and not RECORD:
        sys.stderr.write(src.stdout[-2000:] + src.stderr[-2000:])
        raise SystemExit(src.returncode or 1)

    deadline = time.monotonic() + WAIT_SEC
    done = False
    while time.monotonic() < deadline:
        text = ssh(4, f"grep -E 'gens_ok=|idle timeout' {logs[4]} || true", check=False).stdout
        dest2_ready = True
        if SPLIT:
            text2 = ssh_at(DEST2, "cat /tmp/bats_line_dest2.log", check=False).stdout
            dest2_ready = "gens_ok=" in text2 or "idle timeout" in text2
        if ("gens_ok=" in text or "idle timeout" in text) and dest2_ready:
            done = "gens_ok=" in text
            break
        time.sleep(0.25)
    if done:
        # 中继在最后一包之后还要空闲一段时间才写出 achieved_mbps。
        relay_deadline = time.monotonic() + IDLE + 30
        while time.monotonic() < relay_deadline:
            ready = True
            for node in (2, 3):
                if ssh(node, f"grep -q achieved_mbps= {logs[node]}", check=False).returncode != 0:
                    ready = False
            if ready:
                break
            time.sleep(2)
    if not done:
        for node, path in logs.items():
            text = ssh(node, f"cat {path}", check=False).stdout
            (out_dir / f"vm{node}.log").write_text(text)
            print(f"===== vm{node} =====")
            print(text, end="" if text.endswith("\n") else "\n")
        stop()
        raise SystemExit("dest did not finish decode")

    for node, path in logs.items():
        text = ssh(node, f"cat {path}").stdout
        (out_dir / f"vm{node}.log").write_text(text)
        print(f"===== vm{node} =====")
        print(text, end="" if text.endswith("\n") else "\n")

    dest2_text = ""
    if SPLIT:
        dest2_text = ssh_at(DEST2, "cat /tmp/bats_line_dest2.log", check=False).stdout
        (out_dir / "dest2.log").write_text(dest2_text)
        print("===== dest2 =====")
        print(dest2_text, end="" if dest2_text.endswith("\n") else "\n")

    sha_script = f"""python3 - <<'PY'
import hashlib, os, sys
data = {DATA!r}
flows = {FLOWS}
one = {str(ONE_SOURCE)}
split = {SPLIT}
def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()
bad = 0
missing = 0
for flow in range(flows):
    src = data + '/in.bin' if one == 'True' or flow == 0 else f'{{data}}/in.bin.{{flow}}'
    if split and flow >= split:
        continue
    dst = data + '/out.bin' if flow == 0 else f'{{data}}/out.bin.{{flow}}'
    if not os.path.isfile(src) or not os.path.isfile(dst):
        missing += 1
        print(f'missing flow={{flow}} src={{os.path.isfile(src)}} dst={{os.path.isfile(dst)}}')
        continue
    if digest(src) != digest(dst):
        bad += 1
        print(f'mismatch flow={{flow}}')
print(f'sha_bad={{bad}} sha_missing={{missing}} sha_checked={{flows - (split if split else 0)}}')
sys.exit(1 if bad or missing else 0)
PY"""
    # ONE_SOURCE compares every dest file with the single source file.
    # The check runs on the destination, which has the outputs. Copy the
    # source file's hash by reading it over ssh from VM1 when ONE_SOURCE,
    # and compare on VM4 without a second copy of the 100 MiB input.
    if ONE_SOURCE:
        in_hash = ssh(1, f"sha256sum {remote_in}").stdout.split()[0]
        sha_script = f"""python3 - <<'PY'
import hashlib, os, sys
expect = {in_hash!r}
data = {DATA!r}
flows = {FLOWS}
split = {SPLIT}
def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()
bad = 0
missing = 0
n = 0
for flow in range(flows):
    if split and flow >= split:
        continue
    n += 1
    dst = data + '/out.bin' if flow == 0 else f'{{data}}/out.bin.{{flow}}'
    if not os.path.isfile(dst):
        missing += 1
        continue
    if digest(dst) != expect:
        bad += 1
print(f'sha_bad={{bad}} sha_missing={{missing}} sha_checked={{n}}')
sys.exit(1 if bad or missing else 0)
PY"""
    sha_proc = ssh(4, sha_script, check=False, timeout=3600)
    print(sha_proc.stdout.strip() or sha_proc.stderr.strip())
    sha_ok = sha_proc.returncode == 0
    for name, needle in (
        ("vm1.log", "api=bats_encode"),
        ("vm1.log", "unit_coeff_ok=1"),
        ("vm1.log", "g_on_wire=0"),
        ("vm2.log", "api=bats_recode"),
        ("vm3.log", "api=bats_recode"),
        ("vm4.log", "api=bats_decode"),
        ("vm4.log", "rx_mbps="),
    ):
        if needle not in (out_dir / name).read_text():
            raise SystemExit(f"missing {needle} in {name}")
    if not COPY:
        dense = 0
        for line in (out_dir / "vm4.log").read_text().splitlines():
            if "dense_coeff_packets=" in line:
                dense = int(line.split("dense_coeff_packets=")[1].split()[0])
        if dense <= 0:
            raise SystemExit("destination coefficients were not recoded")
    print("===== checkpoints =====")
    print(summarize(
        (out_dir / "vm1.log").read_text(),
        (out_dir / "vm2.log").read_text(),
        (out_dir / "vm3.log").read_text(),
        (out_dir / "vm4.log").read_text(),
    ))
    if SPLIT:
        second = dest_row(dest2_text)
        print(
            f"dest2_rx_mbps={second['rx_mbps'] or 'missing'} "
            f"dest2_decoded_mbps={second['decoded_mbps'] or 'missing'} "
            f"dest2_ok={second['ok'] or 'missing'}"
        )
    total_bytes = NBYTES * FLOWS
    e2e_s = time.monotonic() - t0
    goodput = (total_bytes * 8 / 1e6) / e2e_s if e2e_s > 0 and sha_ok else 0
    status = "PASS" if sha_ok else "FAIL"
    print(
        f"{status} linear VMs flows={FLOWS} bytes={NBYTES} K={K} T={T} loss={LOSS}% "
        f"rate={RATE} copy={int(COPY)} sha={int(sha_ok)} e2e_s={e2e_s:.3f} "
        f"goodput_mbps={goodput:.1f}"
    )
    if os.environ.get("BATS_KEEP_FILES", "0") != "1":
        ssh(4, f"rm -f {DATA}/out.bin {DATA}/out.bin.*", check=False, timeout=600)
    stop()
    if not sha_ok:
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        stop()
        raise
