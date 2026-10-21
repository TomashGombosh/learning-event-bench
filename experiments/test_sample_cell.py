"""CPU and lag CSV writers. No Docker."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import sample_cell  # noqa: E402


def test_cpu_fraction_is_core_seconds_over_wall_time():
    assert sample_cell.cpu_seconds(2_500_000) == 2.5
    # Two cores busy for one second: usage grows by 2s.
    assert sample_cell.cpu_fraction(0, 10.0, 2_000_000, 11.0) == 2.0
    assert sample_cell.cpu_fraction(0, 10.0, 100, 10.0) is None
    assert sample_cell.parse_usage_usec("usage_usec 42\nuser_usec 1\n") == 42
    assert sample_cell.parse_cpu_percent("150.00%") == 1.5
    assert sample_cell.integrate_fraction(1.0, 1.5, 1.0) == 2.5


def test_cpu_and_lag_csvs_have_the_documented_columns(tmp_path: Path):
    cpu = tmp_path / "cpu_samples.csv"
    lag = tmp_path / "lag_samples.csv"
    sample_cell.append_rows(cpu, sample_cell.CPU_HEADER, [
        sample_cell.format_cpu_row(1000.0, "gateway1", 1.5, None),
        sample_cell.format_cpu_row(1001.0, "kafka", 3.25, 1.75),
    ])
    sample_cell.append_rows(lag, sample_cell.LAG_HEADER, [
        sample_cell.format_lag_row(1000.0, sample_cell.pubsub_lag_row()),
        sample_cell.format_lag_row(1000.0, {
            "broker": "kafka", "consumer": "gateway-abc", "partition": "0",
            "log_end": "140", "read_position": "100", "lag": "40",
        }),
    ])
    cpu_lines = cpu.read_text().splitlines()
    assert cpu_lines[0] == "ts,container,cpu_seconds_cumulative,cpu_fraction"
    assert cpu_lines[1] == "1000.000,gateway1,1.500000,"
    assert cpu_lines[2] == "1001.000,kafka,3.250000,1.750000"
    lag_lines = lag.read_text().splitlines()
    assert lag_lines[0] == "ts,broker,consumer,partition,log_end,read_position,lag"
    assert lag_lines[1] == "1000.000,pubsub,n/a,n/a,n/a,n/a,n/a"
    assert "gateway-abc,0,140,100,40" in lag_lines[2]


def test_kafka_lag_is_log_end_minus_the_read_position():
    text = """
GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
gateway-abc collab-events 0 100 140 40 consumer-1 /a a
gateway-abc collab-events 1 - 10 - - - -
other-topic collab-other 0 1 2 1 c /a a
"""
    rows = sample_cell.parse_kafka_lag(text)
    assert len(rows) == 2
    assert rows[0]["lag"] == "40"
    assert rows[0]["log_end"] == "140"
    assert rows[0]["read_position"] == "100"
    assert rows[1]["lag"] == "n/a"
    assert rows[1]["read_position"] == "n/a"


def test_streams_lag_parser_keeps_groups_and_cursor_rows():
    rows = sample_cell.parse_streams_lag([
        "room:r1:events", "n/a", "100-0", "90-0", "4",
        "room:r2:events", "gateway", "200-1", "200-1", "0",
    ])
    assert rows[0]["broker"] == "streams"
    assert rows[0]["partition"] == "room:r1:events"
    assert rows[0]["read_position"] == "90-0"
    assert rows[0]["lag"] == "4"
    assert rows[1]["consumer"] == "gateway"
    assert rows[1]["lag"] == "0"


def test_container_cpu_seconds_sums_positive_deltas(tmp_path: Path):
    path = tmp_path / "cpu_samples.csv"
    sample_cell.append_rows(path, sample_cell.CPU_HEADER, [
        sample_cell.format_cpu_row(1.0, "gateway1", 10.0, None),
        sample_cell.format_cpu_row(2.0, "gateway1", 10.5, 0.5),
        sample_cell.format_cpu_row(1.0, "kafka", 4.0, None),
        sample_cell.format_cpu_row(2.0, "kafka", 3.0, None),  # restart: drop is ignored
        sample_cell.format_cpu_row(3.0, "kafka", 3.25, 0.25),
    ])
    assert sample_cell.container_cpu_seconds(path) == 0.75
    assert sample_cell.container_cpu_seconds(tmp_path / "missing.csv") is None


def test_pubsub_lag_is_not_applicable():
    assert sample_cell.lag_rows("pubsub") == [sample_cell.pubsub_lag_row()]
    assert sample_cell.pubsub_lag_row()["lag"] == "n/a"
