#!/usr/bin/env bash
# Start the driver inside a DuckDB session and register remote workers; runs until killed (use tmux on EC2).
# Usage: driver.sh <port> [worker_host:port ...]
# Example: driver.sh 8815 10.0.0.2:8816 10.0.0.3:8816
# The `distributed_server` executable cannot register remote workers, so the driver is started from SQL.
set -euo pipefail
PORT=$1
shift
DUCKDB=${DUCKDB:-$(cd "$(dirname "$0")/../.." && pwd)/build/release/duckdb}

{
	echo "SELECT duckherder_start_local_server($PORT, 0);"
	idx=0
	for worker in "$@"; do
		idx=$((idx + 1))
		echo "SELECT duckherder_register_worker('w$idx', 'grpc://$worker');"
	done
	echo "SELECT duckherder_get_worker_count() AS registered_workers;"
	# Signal readiness through a file DuckDB writes itself; redirected stdout may stay buffered.
	if [[ -n ${READY_FILE:-} ]]; then
		echo "COPY (SELECT duckherder_get_worker_count()) TO '$READY_FILE' (FORMAT csv, HEADER false);"
	fi
	# Keep stdin open so the session, and the driver inside it, stays alive.
	tail -f /dev/null
} | "$DUCKDB" -bail
