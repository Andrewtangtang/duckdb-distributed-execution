#!/usr/bin/env bash
# Time the 22 TPC-H queries through a duckherder driver.
# Usage: run.sh <label> <driver_host:port> <s3://bucket/root>   (source rustfs.env or aws.env first)
# Writes <label>.log and <label>.csv (query,run,seconds). Run 0 of each query is a warm-up; set REPS for more runs.
set -euo pipefail
LABEL=$1
ENDPOINT=$2
DATA_PATH=$3
REPS=${REPS:-5}
DUCKDB=${DUCKDB:-$(cd "$(dirname "$0")/../.." && pwd)/build/release/duckdb}

{
	echo "$S3_SETUP_SQL"
	echo "ATTACH '$ENDPOINT/tpch' AS dh (TYPE duckherder, READ_ONLY, DATA_PATH '$DATA_PATH', SECRET s3);"
	echo "USE dh;"
	# Discard results without printing them; the client still fetches every row.
	echo ".mode trash"
	echo ".timer on"
	for q in $(seq 1 22); do
		for r in $(seq 0 "$REPS"); do
			echo ".print q=$q run=$r"
			echo "PRAGMA tpch($q);"
		done
	done
	echo ".timer off"
	echo ".mode csv"
	echo "SELECT * FROM duckherder_get_query_execution_stats();"
} | "$DUCKDB" -bail >"$LABEL.log"

echo "query,run,seconds" >"$LABEL.csv"
awk '/^q=/ { split($1, q, "="); split($2, r, "=") } /^Run Time/ { print q[2] "," r[2] "," $5 }' "$LABEL.log" >>"$LABEL.csv"
echo "Wrote $LABEL.csv ($(($(wc -l <"$LABEL.csv") - 1)) timings) and $LABEL.log"
