# TPC-H benchmark over ObjFS

Runs the 22 TPC-H queries through a duckherder driver with 0..N workers, checks the results, and times them.

## Local run

Requires Docker Desktop, `pkgconf`, `cargo`, and vcpkg. Run everything from the repository root.

1. Build with the `tpch` extension:

   ```sh
   VCPKG_TOOLCHAIN_PATH=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake CORE_EXTENSIONS='tpch' GEN=ninja \
     CMAKE_BUILD_PARALLEL_LEVEL=$(getconf _NPROCESSORS_ONLN) make release
   ```

2. Start RustFS and create the `duckherder` bucket:

   ```sh
   scripts/local-rustfs.sh start
   ```

3. Run:

   ```sh
   benchmark/tpch/bench.sh --sf 1 --workers "0 2"
   ```

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `--sf` | `1` | TPC-H scale factor |
| `--workers` | `"0 2"` | Worker counts to run |
| `--reps` | `5` | Measured runs per query, after one warm-up |
| `--data-path` | `s3://duckherder/tpch-sf<sf>` | Where the ObjFS database lives |
| `--env` | `rustfs.env` | File that sets `S3_SETUP_SQL`, the SQL creating the S3 secret `s3` |
| `--skip-verify` | off | Skip the result check |
| `--no-load` | off | Assume `--data-path` already holds the data, e.g. loaded from another machine |
| `--worker-hosts` | none | Use running workers at these `host:port` addresses instead of starting local ones; each run uses the first N |
| `--driver-ssh` | none | Start the driver on this SSH destination instead of locally; it needs `REMOTE_DUCKDB` (default `duckdb` on its `PATH`) |
| `--driver-endpoint` | `localhost:8815` | Address the client attaches to |

The first run with a `--data-path` generates the data and writes it there; later runs reuse it. A marker in `work/` records the load, so after deleting the data from S3, delete `work/.loaded-*` too. Locally started processes need ports 8815 up to 8815 + the largest worker count free.

## Remote driver and workers

Start `distributed_worker 0.0.0.0 8816 <id>` on each worker host, then run from the client:

```sh
benchmark/tpch/bench.sh --sf 10 --workers "0 2" --worker-hosts "10.0.0.2:8816 10.0.0.3:8816" \
  --driver-ssh ubuntu@10.0.0.1 --driver-endpoint 10.0.0.1:8815
```

The driver host needs key-based SSH from the client and must reach the workers. `bench.sh` runs SSH in batch mode, so connect once by hand first to accept the driver's host key. From a laptop, forward the driver port first (for example `ssh -N -L 8815:10.0.0.1:8815 ...`) and pass `--driver-endpoint localhost:8815`.

## Output

Each run writes `results/<time>-sf<sf>/`:

- `summary.csv`: median seconds per query, one column per worker count
- `metadata.json`: commit, scale factor, worker counts, host
- `n<N>.csv`: every timing; `n<N>.log` ends with each query's execution mode and task count
- `verify-n<N>/`: query results and reference results
- `n<N>-driver.log`, `n<N>-worker<i>.log`: process logs
