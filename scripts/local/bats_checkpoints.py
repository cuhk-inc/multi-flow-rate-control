#!/usr/bin/env python3
"""Pull the eight checkpoint rates out of bats_line logs.

Source raw rate is raw_mbps when the binary prints it, otherwise
file_bytes * flows * 8 / elapsed. Relay achieved_mbps is the encoded
send rate. rx_mbps is the encoded receive rate. On an older log that
has no rx_mbps, receive is reported equal to send only when loss_drops
is 0 and packets_in equals packets_out; that value is marked inferred.
Destination goodput is decoded_mbps only when the summary says ok=1.
A later SHA-256 check can still reject that run.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


def field(line: str, key: str) -> int | None:
    match = re.search(rf"(?:^|\s){re.escape(key)}=([0-9]+)", line)
    if match is None:
        return None
    return int(match.group(1))


def find_line(text: str, *needles: str) -> str:
    for line in text.splitlines():
        if all(needle in line for needle in needles):
            return line
    return ""


def mbps(nbytes: int, elapsed_ms: int) -> int:
    if elapsed_ms <= 0:
        return 0
    return nbytes * 8000 // elapsed_ms


def relay_row(text: str) -> dict[str, str]:
    line = find_line(text, "role=relay", "achieved_mbps=")
    tx = field(line, "achieved_mbps")
    rx = field(line, "rx_mbps")
    note = ""
    if rx is None and tx is not None:
        loss = field(line, "loss_drops")
        pin = field(line, "packets_in")
        pout = field(line, "packets_out")
        if loss == 0 and pin is not None and pin == pout:
            rx = tx
            note = "inferred"
        else:
            note = "missing"
    return {
        "rx_mbps": "" if rx is None else str(rx),
        "tx_mbps": "" if tx is None else str(tx),
        "rx_note": note,
        "socket_drops": "" if field(line, "socket_drops") is None else str(field(line, "socket_drops")),
        "loss_drops": "" if field(line, "loss_drops") is None else str(field(line, "loss_drops")),
    }


def source_row(text: str) -> dict[str, str]:
    raw = 0
    tx = 0
    loss = 0
    n = 0
    saw_raw = False
    for line in text.splitlines():
        if "role=source" not in line or "achieved_mbps=" not in line:
            continue
        n += 1
        tx += field(line, "achieved_mbps") or 0
        loss += field(line, "loss_drops") or 0
        one = field(line, "raw_mbps")
        if one is None:
            file_bytes = field(line, "file_bytes")
            flows = field(line, "flows")
            elapsed = field(line, "elapsed_ms")
            if file_bytes is None:
                start = find_line(text, "role=source", "file_bytes=")
                file_bytes = field(start, "file_bytes")
            if file_bytes is not None and flows is not None and elapsed is not None:
                one = mbps(file_bytes * flows, elapsed)
        if one is not None:
            raw += one
            saw_raw = True
    if n == 0:
        return {"raw_mbps": "", "tx_mbps": "", "loss_drops": ""}
    return {
        "raw_mbps": str(raw) if saw_raw else "",
        "tx_mbps": str(tx),
        "loss_drops": str(loss),
    }


def dest_row(text: str) -> dict[str, str]:
    line = find_line(text, "role=dest", "gens_ok=")
    if "rx_mbps=" not in line:
        line = find_line(text, "role=dest", "rx_mbps=") or line
    ok = field(line, "ok")
    decoded = field(line, "decoded_mbps")
    if ok == 0:
        decoded = 0
    rx = field(line, "rx_mbps")
    drops_line = find_line(text, "dest socket_drops=")
    return {
        "rx_mbps": "" if rx is None else str(rx),
        "decoded_mbps": "" if decoded is None else str(decoded),
        "ok": "" if ok is None else str(ok),
        "socket_drops": "" if field(drops_line, "socket_drops") is None else str(field(drops_line, "socket_drops")),
        "gens_ok": "" if field(line, "gens_ok") is None else str(field(line, "gens_ok")),
    }


def summarize(src: str, relay1: str, relay2: str, dest: str) -> str:
    s = source_row(src)
    r1 = relay_row(relay1)
    r2 = relay_row(relay2)
    d = dest_row(dest)
    rows = [
        ("src_raw_mbps", s["raw_mbps"], "源端原始速率"),
        ("src_tx_mbps", s["tx_mbps"], "源端编码后发送"),
        ("r1_rx_mbps", r1["rx_mbps"], "中继1收" + (f" ({r1['rx_note']})" if r1["rx_note"] else "")),
        ("r1_tx_mbps", r1["tx_mbps"], "中继1发"),
        ("r2_rx_mbps", r2["rx_mbps"], "中继2收" + (f" ({r2['rx_note']})" if r2["rx_note"] else "")),
        ("r2_tx_mbps", r2["tx_mbps"], "中继2发"),
        ("dest_rx_mbps", d["rx_mbps"], "目的端译码前"),
        ("dest_decoded_mbps", d["decoded_mbps"], "目的端译码后"),
    ]
    lines = [f"{key}={value or 'missing'}\t{label}" for key, value, label in rows]
    lines.append(
        "drops "
        f"src_loss={s['loss_drops'] or 'missing'} "
        f"r1_loss={r1['loss_drops'] or 'missing'} r1_socket={r1['socket_drops'] or 'missing'} "
        f"r2_loss={r2['loss_drops'] or 'missing'} r2_socket={r2['socket_drops'] or 'missing'} "
        f"dest_socket={d['socket_drops'] or 'missing'} dest_ok={d['ok'] or 'missing'} "
        f"gens_ok={d['gens_ok'] or 'missing'}"
    )
    return "\n".join(lines)


def main() -> None:
    if len(sys.argv) != 5:
        sys.stderr.write("usage: bats_checkpoints.py source.log relay1.log relay2.log dest.log\n")
        raise SystemExit(2)
    texts = [Path(path).read_text(errors="replace") for path in sys.argv[1:]]
    print(summarize(*texts))


if __name__ == "__main__":
    main()
