# TPC-H benchmark over ObjFS

Runs the 22 TPC-H queries through a duckherder driver with 0..N workers, checks the results, and times them.

## Local run

Requires Docker Desktop, `pkgconf`, `cargo`, and vcpkg. Run everything from the repository root.

1. Build with the `tpch` extension:

   ```sh
   VCPKG_TOOLCHAIN_PATH=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake CORE_EXTENSIONS='tpch' GEN=ninja \
     CMAKE_BUILD_PARALLEL_LEVEL=$(sysctl -n hw.ncpu) make release
   ```

2. Start RustFS and create the `duckherder` bucket (once):

   ```sh
   scripts/local-rustfs.sh start
   curl --aws-sigv4 "aws:amz:us-east-1:s3" --user rustfsadmin:rustfsadmin -X PUT http://127.0.0.1:19000/duckherder
   ```

   `local-rustfs.sh` fails at bucket creation because it cannot pull `quay.io/minio/mc`; the `curl` creates the bucket instead.

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
| `--env` | `rustfs.env` | S3 settings; use `aws.env` on AWS |
| `--skip-verify` | off | Skip the result check |

The first run with a `--data-path` generates the data and writes it there; later runs reuse it. A marker in `work/` records the load, so after deleting the data from S3, delete `work/.loaded-*` too. Ports 8815 up to 8815 + the largest worker count must be free.

## Output

Each run writes `results/<time>-sf<sf>/`:

- `summary.csv`: median seconds per query, one column per worker count
- `metadata.json`: commit, scale factor, worker counts, host
- `n<N>.csv`: every timing; `n<N>.log` ends with each query's execution mode and task count
- `verify-n<N>/`: query results and reference results
- `n<N>-driver.log`, `n<N>-worker<i>.log`: process logs
