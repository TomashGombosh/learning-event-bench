#!/usr/bin/env python3
"""Per-second CPU and consumer-lag samples for one cell.

Writes results/<run>/cpu_samples.csv and lag_samples.csv.

CPU prefers cgroup cpu.stat usage_usec (cumulative since the container started).
cpu_fraction is the increase divided by wall time since the previous sample, so
it can exceed 1 when the container has more than one core. If the cgroup file
is not readable, docker stats is the fallback: cpu_fraction is the reported
share, and cpu_seconds_cumulative integrates that share from the first sample.

Lag is log end minus the consumer position.
  kafka        kafka-consumer-groups --describe, one row per group and partition
  streams      XINFO GROUPS lag, or entries after the gateway cursor vs the last id
  pubsub       no backlog; lag is n/a

Usage: python3 experiments/sample_cell.py --out results/<run_id> --broker kafka
"""
from __future__ import annotations

import argparse
import signal
import subprocess
import time
from pathlib import Path

CPU_HEADER = "ts,container,cpu_seconds_cumulative,cpu_fraction\n"
LAG_HEADER = "ts,broker,consumer,partition,log_end,read_position,lag\n"
COMPOSE_SERVICES = {"redis", "kafka", "postgres", "gateway1", "gateway2", "gateway3", "loadgen"}

# One round trip. Groups win when they exist. Otherwise lag is the number of
# entries strictly after the gateway's last-read id (bench:stream-cursors).
STREAMS_LAG_LUA = r"""
local function field(info, name)
  for i = 1, #info, 2 do
    if info[i] == name then return info[i + 1] end
  end
  return false
end
local cursors = {}
local flat = redis.call('HGETALL', 'bench:stream-cursors')
for i = 1, #flat, 2 do cursors[flat[i]] = flat[i + 1] end
local out = {}
local keys = redis.call('KEYS', 'room:*:events')
for _, key in ipairs(keys) do
  local info = redis.call('XINFO', 'STREAM', key)
  local last_id = field(info, 'last-generated-id')
  if last_id == false then last_id = '' end
  local groups = redis.call('XINFO', 'GROUPS', key)
  if #groups == 0 then
    local room = string.match(key, '^room:(.*):events$') or key
    local cursor = cursors[room] or ''
    local lag = 'n/a'
    if cursor ~= '' then
      local n = 0
      local id = cursor
      while n < 100000 do
        local entries = redis.call('XRANGE', key, '(' .. id, '+', 'COUNT', 500)
        local got = #entries
        if got == 0 then break end
        n = n + got
        id = entries[got][1]
        if got < 500 then break end
      end
      lag = tostring(n)
    end
    table.insert(out, key)
    table.insert(out, 'n/a')
    table.insert(out, tostring(last_id))
    table.insert(out, cursor == '' and 'n/a' or cursor)
    table.insert(out, lag)
  else
    for _, group in ipairs(groups) do
      local name = field(group, 'name')
      local delivered = field(group, 'last-delivered-id')
      local lag = field(group, 'lag')
      table.insert(out, key)
      table.insert(out, name == false and 'n/a' or tostring(name))
      table.insert(out, tostring(last_id))
      table.insert(out, delivered == false and 'n/a' or tostring(delivered))
      table.insert(out, lag == false and 'n/a' or tostring(lag))
    end
  end
end
return out
"""


def cpu_seconds(usage_usec: int) -> float:
    return usage_usec / 1_000_000


def cpu_fraction(prev_usec: int, prev_ts: float, usage_usec: int, ts: float) -> float | None:
    """Share of one core. Greater than 1 when several cores are busy."""
    dt = ts - prev_ts
    if dt <= 0:
        return None
    return ((usage_usec - prev_usec) / 1_000_000) / dt


def integrate_fraction(cumulative: float, fraction: float, dt: float) -> float:
    return cumulative + fraction * max(0.0, dt)


def parse_usage_usec(stat_text: str) -> int | None:
    for line in stat_text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] == "usage_usec":
            return int(parts[1])
    return None


def parse_cpu_percent(text: str) -> float | None:
    raw = text.strip().rstrip("%")
    if not raw or raw == "--":
        return None
    return float(raw) / 100


def parse_kafka_lag(text: str) -> list[dict]:
    """Rows from `kafka-consumer-groups.sh --describe`. Missing offsets stay n/a."""
    rows = []
    for line in text.splitlines():
        if not line.strip() or line.startswith("GROUP") or line.startswith("Error") or "has no active members" in line:
            continue
        parts = line.split()
        if len(parts) < 6:
            continue
        group, topic, partition, current, log_end, lag = parts[:6]
        if topic != "collab-events":
            continue
        rows.append({
            "broker": "kafka",
            "consumer": group,
            "partition": partition,
            "log_end": "n/a" if log_end == "-" else log_end,
            "read_position": "n/a" if current == "-" else current,
            "lag": "n/a" if lag == "-" else lag,
        })
    return rows


def parse_streams_lag(values: list[str]) -> list[dict]:
    """Flat EVAL reply: key, consumer, log_end, read_position, lag, repeated."""
    rows = []
    for index in range(0, len(values) - 4, 5):
        key, consumer, log_end, read_position, lag = values[index:index + 5]
        rows.append({
            "broker": "streams",
            "consumer": consumer or "n/a",
            "partition": key,
            "log_end": log_end or "n/a",
            "read_position": read_position or "n/a",
            "lag": lag or "n/a",
        })
    return rows


def pubsub_lag_row() -> dict:
    return {
        "broker": "pubsub",
        "consumer": "n/a",
        "partition": "n/a",
        "log_end": "n/a",
        "read_position": "n/a",
        "lag": "n/a",
    }


def format_cpu_row(ts: float, container: str, cumulative: float, fraction: float | None) -> str:
    share = "" if fraction is None else f"{fraction:.6f}"
    return f"{ts:.3f},{container},{cumulative:.6f},{share}\n"


def format_lag_row(ts: float, row: dict) -> str:
    return (
        f"{ts:.3f},{row['broker']},{row['consumer']},{row['partition']},"
        f"{row['log_end']},{row['read_position']},{row['lag']}\n"
    )


def append_rows(path: Path, header: str, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8") as handle:
        if new_file:
            handle.write(header)
        handle.writelines(lines)
        handle.flush()


def usage_usec_for_pid(pid: int) -> int | None:
    """cgroup v2 cpu.stat, or cgroup v1 cpuacct.usage (nanoseconds)."""
    cgroup = Path(f"/proc/{pid}/cgroup")
    if not cgroup.is_file():
        return None
    for line in cgroup.read_text(errors="replace").splitlines():
        rel = line.split(":", 2)[-1].strip().lstrip("/")
        if not rel:
            continue
        stat = Path("/sys/fs/cgroup") / rel / "cpu.stat"
        if stat.is_file():
            parsed = parse_usage_usec(stat.read_text(errors="replace"))
            if parsed is not None:
                return parsed
        acct = Path("/sys/fs/cgroup/cpuacct") / rel / "cpuacct.usage"
        if acct.is_file():
            return int(acct.read_text().strip()) // 1000
    return None


def _run(args: list[str], timeout: float = 5) -> str:
    try:
        completed = subprocess.run(args, check=False, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    if completed.returncode != 0:
        return completed.stdout or ""
    return completed.stdout


def compose_containers() -> list[tuple[str, str]]:
    """Running compose services that are gateways, brokers, or loadgen. (service, name)."""
    text = _run(["docker", "compose", "ps", "--status", "running", "--format", "{{.Service}}\t{{.Name}}"])
    found = []
    seen = set()
    for line in text.splitlines():
        parts = line.split("\t")
        if len(parts) != 2:
            continue
        service, name = parts
        if service in COMPOSE_SERVICES and name and name not in seen:
            found.append((service, name))
            seen.add(name)
    # `compose run` loadgen containers are one-off. Compose v2 lists them; this
    # label query covers a version that does not.
    extra = _run([
        "docker", "ps", "--filter", "label=com.docker.compose.service=loadgen",
        "--format", "{{.Names}}",
    ])
    for name in extra.splitlines():
        name = name.strip()
        if name and name not in seen:
            found.append(("loadgen", name))
            seen.add(name)
    return found


def container_pid(name: str) -> int | None:
    text = _run(["docker", "inspect", "-f", "{{.State.Pid}}", name], timeout=3).strip()
    if not text.isdigit() or text == "0":
        return None
    return int(text)


def docker_stat_fractions() -> dict[str, float]:
    text = _run(["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}"], timeout=8)
    fractions = {}
    for line in text.splitlines():
        parts = line.split("\t")
        if len(parts) != 2:
            continue
        name = parts[0].lstrip("/")
        fraction = parse_cpu_percent(parts[1])
        if fraction is not None:
            fractions[name] = fraction
    return fractions


class CpuSampler:
    def __init__(self) -> None:
        self._prev: dict[str, tuple[int, float]] = {}
        self._integrated: dict[str, tuple[float, float]] = {}

    def sample(self, ts: float) -> list[str]:
        rows = []
        containers = compose_containers()
        names = {name for _, name in containers}
        cgroup_hit = False
        for _service, name in containers:
            pid = container_pid(name)
            usage = usage_usec_for_pid(pid) if pid is not None else None
            if usage is None:
                continue
            cgroup_hit = True
            prev = self._prev.get(name)
            fraction = None if prev is None else cpu_fraction(prev[0], prev[1], usage, ts)
            self._prev[name] = (usage, ts)
            rows.append(format_cpu_row(ts, name, cpu_seconds(usage), fraction))
        if cgroup_hit or not containers:
            return rows
        fractions = docker_stat_fractions()
        for name, fraction in fractions.items():
            if name not in names:
                continue
            prev = self._integrated.get(name)
            if prev is None:
                cumulative, share = 0.0, None
            else:
                cumulative = integrate_fraction(prev[0], fraction, ts - prev[1])
                share = fraction
            self._integrated[name] = (cumulative, ts)
            rows.append(format_cpu_row(ts, name, cumulative, share))
        return rows


def kafka_lag_rows() -> list[dict]:
    text = _run([
        "docker", "compose", "exec", "-T", "kafka",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server", "localhost:9092", "--all-groups", "--describe",
    ], timeout=15)
    return parse_kafka_lag(text)


def streams_lag_rows() -> list[dict]:
    text = _run([
        "docker", "compose", "exec", "-T", "redis",
        "redis-cli", "--raw", "EVAL", STREAMS_LAG_LUA, "0",
    ], timeout=15)
    values = text.splitlines()
    return parse_streams_lag(values)


def lag_rows(broker: str) -> list[dict]:
    if broker == "pubsub":
        return [pubsub_lag_row()]
    if broker == "streams":
        rows = streams_lag_rows()
        return rows or [{
            "broker": "streams", "consumer": "n/a", "partition": "n/a",
            "log_end": "n/a", "read_position": "n/a", "lag": "n/a",
        }]
    if broker in ("kafka", "kafka-tuned", "kafka-acks"):
        rows = kafka_lag_rows()
        return rows or [{
            "broker": "kafka", "consumer": "n/a", "partition": "n/a",
            "log_end": "n/a", "read_position": "n/a", "lag": "n/a",
        }]
    return [pubsub_lag_row()]


def container_cpu_seconds(path: Path) -> float | None:
    """CPU-seconds in one cell: sum of positive cumulative deltas per container.

    `cpu_seconds_cumulative` is cgroup usage since the container started, so the
    cell total is the increase across the samples written for that cell.
    A restart shows up as a drop and is not counted backwards.
    Returns None when the file is missing or has no numeric rows.
    """
    if not path.is_file():
        return None
    import csv
    series: dict[str, list[float]] = {}
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            try:
                series.setdefault(row["container"], []).append(float(row["cpu_seconds_cumulative"]))
            except (KeyError, TypeError, ValueError):
                continue
    if not series:
        return None
    total = 0.0
    for values in series.values():
        for previous, current in zip(values, values[1:]):
            if current >= previous:
                total += current - previous
    return total


def sample_once(out: Path, broker: str, cpu: CpuSampler, ts: float | None = None) -> None:
    now = time.time() if ts is None else ts
    append_rows(out / "cpu_samples.csv", CPU_HEADER, cpu.sample(now))
    append_rows(out / "lag_samples.csv", LAG_HEADER, [format_lag_row(now, row) for row in lag_rows(broker)])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sample container CPU and consumer lag once a second")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--broker", required=True)
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args(argv)
    stop = False

    def request_stop(_signum, _frame) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    cpu = CpuSampler()
    args.out.mkdir(parents=True, exist_ok=True)
    while not stop:
        started = time.time()
        try:
            sample_once(args.out, args.broker, cpu, started)
        except Exception as error:  # keep the cell alive if one sample fails
            print(f"sample failed: {error}", flush=True)
        remaining = args.interval - (time.time() - started)
        if remaining > 0 and not stop:
            time.sleep(remaining)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
