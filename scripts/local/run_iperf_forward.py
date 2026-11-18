#!/usr/bin/env python3
"""TCP and single-flow UDP from VM1 to VM4 through VM2 and VM3.

This is kernel forwarding, not bats_line. Ten seconds each.
UDP targets are the loss-free and loss-onset points from the single-hop table.
"""

from __future__ import annotations

import subprocess
import sys

SSH = ["ssh", "-n", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]
VM1 = "fyp1@10.10.10.161"
VM4 = "fyp1@10.10.10.164"
BIND = "10.20.20.1"
DEST = "10.40.40.2"
PORT = "5211"


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=40)
    if check and proc.returncode != 0:
        sys.stderr.write(proc.stdout[-2000:] + proc.stderr[-2000:])
        raise SystemExit(proc.returncode or 1)
    return proc


def ssh(host: str, script: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return run([*SSH, host, script], check=check)


def main() -> None:
    ssh(VM1, f"ping -c 1 -W 2 {DEST} >/dev/null")
    ssh(VM4, "pkill -f '[i]perf3' >/dev/null 2>&1 || true", check=False)
    print("traceroute")
    print(ssh(VM1, f"traceroute -n -w 1 -q 1 {DEST} || true", check=False).stdout)
    ssh(VM4, f"iperf3 -s -B {DEST} -p {PORT} -1 >/tmp/iperf_srv.txt 2>&1 &")
    tcp = ssh(VM1, f"iperf3 -c {DEST} -B {BIND} -p {PORT} -t 10")
    print("===== tcp =====")
    print(tcp.stdout)
    for label, mbps in (("udp1500", 1500), ("udp2500", 2500)):
        ssh(VM4, f"iperf3 -s -B {DEST} -p {PORT} -1 >/tmp/iperf_srv.txt 2>&1 &")
        udp = ssh(
            VM1,
            f"iperf3 -c {DEST} -B {BIND} -p {PORT} -u -b {mbps}M -t 10",
        )
        print(f"===== {label} =====")
        print(udp.stdout)
    ssh(VM4, "pkill -f '[i]perf3' >/dev/null 2>&1 || true", check=False)


if __name__ == "__main__":
    main()
