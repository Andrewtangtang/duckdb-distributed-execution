#!/usr/bin/env bash
# Run the TPC-H benchmark end to end on one machine: load the data once, then for each worker count start the
# workers and driver, verify results, time the queries, and stop everything.
# Usage: bench.sh [--sf N] [--workers "0 2"] [--reps N] [--env FILE] [--data-path s3://...] [--skip-verify]
# Needs an S3 endpoint whose bucket exists, e.g. local RustFS for the default rustfs.env.
# Results go to results/<time>-sf<N>/: metadata.json, n<N>.csv/.log, verify-n<N>.txt, process logs, summary.csv.
set -euo pipefail
set -m # Give each background job its own process group so the driver pipeline can be stopped as a whole.

DIR=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$DIR/../.." && pwd)
export DUCKDB=${DUCKDB:-$ROOT/build/release/duckdb}
WORKER_BIN=${WORKER_BIN:-$ROOT/build/release/extension/duckherder/distributed_worker}
DRIVER_PORT=${DRIVER_PORT:-8815}

SF=1
WORKER_COUNTS="0 2"
REPS=5
ENV_FILE=$DIR/rustfs.env
DATA_PATH=""
VERIFY=1
while (($#)); do
	case $1 in
	--sf) SF=$2 && shift 2 ;;
	--workers) WORKER_COUNTS=$2 && shift 2 ;;
	--reps) REPS=$2 && shift 2 ;;
	--env) ENV_FILE=$2 && shift 2 ;;
	--data-path) DATA_PATH=$2 && shift 2 ;;
	--skip-verify) VERIFY=0 && shift ;;
	*) echo "Unknown option: $1" >&2 && exit 1 ;;
	esac
done
DATA_PATH=${DATA_PATH:-s3://duckherder/tpch-sf$SF}
# shellcheck source=/dev/null
source "$ENV_FILE"

WORK=$DIR/work
OUT=$DIR/results/$(date +%Y%m%d-%H%M%S)-sf$SF
mkdir -p "$WORK" "$OUT"
export TPCH_FILE=$WORK/tpch_sf$SF.duckdb

port_open() { (echo >"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

wait_for() {
	local deadline=$((SECONDS + $1)) what=$2
	shift 2
	until "$@"; do
		if ((SECONDS >= deadline)); then
			echo "Timed out waiting for $what" >&2
			exit 1
		fi
		sleep 0.2
	done
}

pids=()
stop_all() {
	for pid in ${pids[@]+"${pids[@]}"}; do
		kill -- -"$pid" 2>/dev/null || true
	done
	wait 2>/dev/null || true
	pids=()
}
trap stop_all EXIT

# Leftover processes would answer on these ports and silently replace the ones started here.
max_workers=0
for n in $WORKER_COUNTS; do ((n > max_workers)) && max_workers=$n; done
for ((port = DRIVER_PORT; port <= DRIVER_PORT + max_workers; port++)); do
	if port_open "$port"; then
		echo "Port $port is in use; stop the running driver or worker first." >&2
		exit 1
	fi
done

# Load once per data path. ObjFS allows one writer, and the driver opens the database read-write, so load first.
marker=$WORK/.loaded-$(echo "$DATA_PATH" | tr -c 'a-zA-Z0-9\n' _)
if [[ ! -f $marker ]]; then
	"$DIR/load.sh" "$SF" "$DATA_PATH" | tee "$OUT/load.log"
	touch "$marker"
fi

cat >"$OUT/metadata.json" <<EOF
{
  "commit": "$(git -C "$ROOT" rev-parse HEAD)",
  "modified_tracked_files": $(git -C "$ROOT" status --porcelain --untracked-files=no | wc -l | tr -d ' '),
  "duckdb": "$("$DUCKDB" -noheader -list -c 'SELECT version()')",
  "sf": $SF,
  "worker_counts": "$WORKER_COUNTS",
  "reps": $REPS,
  "data_path": "$DATA_PATH",
  "host": "$(uname -sm)",
  "cpus": $(getconf _NPROCESSORS_ONLN),
  "started_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

failed=()
for n in $WORKER_COUNTS; do
	echo "== $n workers"
	workers=()
	for ((idx = 1; idx <= n; idx++)); do
		port=$((DRIVER_PORT + idx))
		"$WORKER_BIN" 127.0.0.1 "$port" "w$idx" >"$OUT/n$n-worker$idx.log" 2>&1 &
		pids+=($!)
		workers+=("127.0.0.1:$port")
		wait_for 30 "worker on port $port (see $OUT/n$n-worker$idx.log)" port_open "$port"
	done

	ready=$OUT/n$n-driver.ready
	READY_FILE=$ready "$DIR/driver.sh" "$DRIVER_PORT" ${workers[@]+"${workers[@]}"} >"$OUT/n$n-driver.log" 2>&1 &
	pids+=($!)
	wait_for 60 "driver (see $OUT/n$n-driver.log)" test -s "$ready"
	registered=$(tr -d '[:space:]' <"$ready")
	if [[ $registered != "$n" ]]; then
		echo "Driver registered $registered workers, expected $n" >&2
		exit 1
	fi

	if ((VERIFY)); then
		mkdir -p "$OUT/verify-n$n"
		if ! OUT=$OUT/verify-n$n "$DIR/verify.sh" "localhost:$DRIVER_PORT" "$DATA_PATH" "$TPCH_FILE" |
			tee "$OUT/verify-n$n.txt"; then
			failed+=("verify with $n workers")
		fi
	fi
	REPS=$REPS "$DIR/run.sh" "$OUT/n$n" "localhost:$DRIVER_PORT" "$DATA_PATH"
	stop_all
done

# Median seconds per query (columns are worker counts), excluding each query's warm-up run.
"$DUCKDB" -c "
CREATE TABLE timings AS
SELECT regexp_extract(filename, 'n([0-9]+)\.csv', 1)::INTEGER AS workers, query, seconds
FROM read_csv('$OUT/n*.csv', filename = true) WHERE run > 0;
COPY (PIVOT timings ON workers USING median(seconds) GROUP BY query ORDER BY query) TO '$OUT/summary.csv';
FROM read_csv('$OUT/summary.csv');"

echo "Results in $OUT"
if ((${#failed[@]})); then
	printf 'FAILED: %s\n' "${failed[@]}" >&2
	exit 1
fi
