from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import bench


def test_percentile_uses_linear_interpolation() -> None:
    values = [1.0, 2.0, 3.0, 4.0, 5.0]

    assert bench.percentile(values, 50) == 3.0
    assert bench.percentile(values, 95) == 4.8
    assert bench.percentile(values, 99) == 4.96


def test_summarize_latencies_includes_tail_percentiles() -> None:
    summary = bench.summarize_latencies([2.0, 4.0, 6.0, 8.0])

    assert summary.min_ms == 2.0
    assert summary.mean_ms == 5.0
    assert summary.p50_ms == 5.0
    assert summary.p95_ms == 7.7
    assert summary.p99_ms == 7.94
    assert summary.max_ms == 8.0


def test_parser_enables_verbose_logging() -> None:
    args = bench.build_parser().parse_args(
        ["--events", "5", "--output", "/tmp/bench.json", "--verbose"]
    )

    assert args.verbose is True


@pytest.mark.asyncio
async def test_run_benchmark_processes_events_end_to_end() -> None:
    output = Path("/tmp/unused-bench-output.json")
    result = await bench.run_benchmark(bench.BenchmarkConfig(events=24, output=output))

    assert result.mode == "offline-in-memory"
    assert result.events == 24
    assert result.pipeline_counts["stream_messages_processed"] == 24
    assert result.pipeline_counts["stream_signals_published"] == 24
    assert result.pipeline_counts["ai_messages_processed"] == 24
    assert result.pipeline_counts["ai_insights_published"] == 24
    assert result.pipeline_counts["alerting_messages_processed"] == 48
    assert result.pipeline_counts["alerting_alerts_published"] >= 1
    assert result.throughput_events_per_second > 0
    assert result.latency_ms.p50_ms > 0
    assert result.latency_ms.p95_ms >= result.latency_ms.p50_ms
    assert result.latency_ms.p99_ms >= result.latency_ms.p95_ms


def test_main_writes_json_report(tmp_path: Path) -> None:
    output = tmp_path / "bench.json"

    rc = bench.main(["--events", "4", "--output", str(output)])

    assert rc == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["events"] == 4
    assert payload["mode"] == "offline-in-memory"
    assert payload["pipeline_counts"]["stream_messages_processed"] == 4
    assert payload["pipeline_counts"]["ai_insights_published"] == 4
    assert payload["latency_ms"]["p99_ms"] >= payload["latency_ms"]["p95_ms"]
    assert payload["limitations"]
    assert any("in-memory" in item for item in payload["limitations"])


def test_script_runs_from_repo_root_and_writes_output(tmp_path: Path) -> None:
    output = tmp_path / "bench-cli.json"
    result = subprocess.run(
        [
            sys.executable,
            "scripts/bench.py",
            "--events",
            "3",
            "--output",
            str(output),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["events"] == 3
    assert payload["pipeline_counts"]["stream_messages_processed"] == 3
    assert "limitations" in payload


def test_bench_script_avoids_bare_print_calls() -> None:
    source = Path("scripts/bench.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id == "print":
                pytest.fail("Found bare print() call in scripts/bench.py")
