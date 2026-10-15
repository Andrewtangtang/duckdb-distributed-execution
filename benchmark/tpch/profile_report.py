#!/usr/bin/env python3
"""Summarize a DUCKHERDER_PROFILE_DIR: per query, the driver's time, each fragment's task end times, and each worker
task's attach time and slowest operators.

Usage: profile_report.py <profile_dir> [query numbers in run order, e.g. 10 17 18]
"""
import json
import sys
from pathlib import Path

TOP_OPERATORS = 4


def operators(node):
    """Yields (name, seconds, rows, extra_info) for every operator under a profile node."""
    for child in node.get("children", []):
        name = child.get("operator_name") or child.get("operator_type") or "?"
        extra = child.get("extra_info") or {}
        table = extra.get("Table", "") if isinstance(extra, dict) else ""
        yield (f"{name.strip()} {table}".strip(), child.get("operator_timing", 0.0),
               child.get("operator_cardinality", 0))
        yield from operators(child)


def top(profile):
    ops = sorted(operators(profile), key=lambda op: -op[1])[:TOP_OPERATORS]
    return ", ".join(f"{name} {seconds:.2f}s/{rows}r" for name, seconds, rows in ops)


def load_profiles(directory, prefix):
    """Returns [(epoch_ms, worker_or_None, profile)] for <prefix>-*.json, sorted by time."""
    result = []
    for path in directory.glob(f"{prefix}-*.json"):
        parts = path.stem.split("-")
        try:
            profile = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        result.append((int(parts[-1]), "-".join(parts[1:-1]) or None, profile))
    return sorted(result, key=lambda entry: entry[0])


def read_tsv(path):
    return [line.rstrip("\n").split("\t") for line in path.open()] if path.exists() else []


def main():
    directory = Path(sys.argv[1])
    labels = sys.argv[2:]
    queries = [(ms, profile) for ms, _, profile in load_profiles(directory, "driver")
               if "duckdb_schemas()" not in profile.get("query_name", "")]
    fragments = [(int(f[0]), int(f[1]), int(f[2]), [int(x) for x in f[3].split(",") if x], f[4])
                 for f in read_tsv(directory / "fragments.tsv")]
    tasks = [(int(t[1]), t[0], int(t[2]), int(t[3])) for t in read_tsv(directory / "workers.tsv")]
    worker_profiles = {(worker, ms): profile for ms, worker, profile in load_profiles(directory, "worker")}

    for idx, (start_ms, profile) in enumerate(queries):
        label = f"Q{labels[idx]}" if idx < len(labels) else f"query {idx + 1}"
        latency = profile.get("latency", 0.0)
        end_ms = queries[idx + 1][0] if idx + 1 < len(queries) else float("inf")
        print(f"== {label}: driver {latency:.2f}s")
        print(f"   driver ops: {top(profile)}")
        for frag_start, dispatch, total, ends, description in fragments:
            if not start_ms <= frag_start <= end_ms:
                continue
            print(f"   fragment {total / 1000:.2f}s, all tasks started after {dispatch / 1000:.2f}s, "
                  f"task ends {', '.join(f'{e / 1000:.2f}s' for e in ends)}")
            print(f"     {description[:160]}")
        for received, worker, open_ms, start_task_ms in tasks:
            if not start_ms <= received <= end_ms:
                continue
            task_profile = worker_profiles.get((worker, received), {})
            # CPU time well below run time x threads means the task mostly waited, e.g. on storage.
            print(f"   {worker} +{(received - start_ms) / 1000:.2f}s: attach {open_ms / 1000:.2f}s, "
                  f"plan {start_task_ms / 1000:.2f}s, run {task_profile.get('latency', 0.0):.2f}s, "
                  f"cpu {task_profile.get('cpu_time', 0.0):.2f}s, "
                  f"read {task_profile.get('total_bytes_read', 0) / 2**20:.0f} MiB")
            print(f"     {top(task_profile)}")


if __name__ == "__main__":
    main()
