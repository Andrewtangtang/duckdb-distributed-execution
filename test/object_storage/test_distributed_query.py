#!/usr/bin/env python3
"""Run with Python's pyarrow/protobuf packages and protoc on PATH; no pytest needed."""

import argparse
import importlib
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile

import pyarrow as pa
import pyarrow.flight as flight


ROOT = Path(__file__).resolve().parents[2]
OPTIONS = flight.FlightCallOptions(timeout=20)


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build/release")
    parser.add_argument("--duckdb", type=Path)
    parser.add_argument("--bin-dir", type=Path)
    parser.add_argument("--protoc", default=shutil.which("protoc"))
    args = parser.parse_args()
    duckdb = args.duckdb or args.build_dir / "duckdb"
    binaries = args.bin_dir or args.build_dir / "extension/duckherder"
    assert args.protoc, "protoc must be on PATH or supplied with --protoc"
    processes = []
    logs = []

    with tempfile.TemporaryDirectory(prefix="duckherder-query-") as directory:
        work = Path(directory)
        subprocess.run(
            [args.protoc, f"-I{ROOT / 'src/proto'}", f"--python_out={work}",
             str(ROOT / "src/proto/distributed.proto")], check=True,
        )
        sys.path.insert(0, str(work))
        proto = importlib.import_module("distributed_pb2")

        def action(client, request, expect_success=True):
            results = list(client.do_action(
                flight.Action("execute", request.SerializeToString()), options=OPTIONS,
            ))
            assert len(results) == 1
            response = proto.DistributedResponse.FromString(results[0].body.to_pybytes())
            assert response.success == expect_success, response.error_message
            return response

        def start(role, storage_args=()):
            port = free_port()
            log = open(work / f"{role}-{port}.log", "w+")
            logs.append(log)
            process = subprocess.Popen(
                [str(binaries / f"distributed_{role}"), "127.0.0.1", str(port),
                 "0" if role == "server" else f"worker-{port}"]
                + list(storage_args),
                stdout=log, stderr=log,
            )
            processes.append(process)
            client = flight.FlightClient(("127.0.0.1", port))
            request = proto.DistributedRequest()
            if role == "server":
                request.get_query_execution_stats.SetInParent()
            else:
                request.worker_heartbeat.worker_id = "readiness"
            try:
                client.wait_for_available(timeout=30)
                action(client, request)
            except flight.FlightError:
                log.flush()
                log.seek(0)
                raise AssertionError(log.read())
            return client, port

        def register(client, worker_id, port):
            request = proto.DistributedRequest()
            request.worker_register.worker_id = worker_id
            request.worker_register.host = "127.0.0.1"
            request.worker_register.port = port
            assert action(client, request).worker_register.accepted

        def query(client, sql):
            request = proto.DistributedRequest()
            request.scan_table.table_name = sql
            request.scan_table.limit = (1 << 64) - 1
            return client.do_get(flight.Ticket(request.SerializeToString()), options=OPTIONS).read_all()

        try:
            storage_root = work / "storage's root"
            setup = (
                "LOAD duckdb_object_storage;\n"
                "SET duckdb_objfs_backend = 'local';\n"
                f"SET duckdb_objfs_root = {sql_string(storage_root)};\n"
            )
            subprocess.run([str(duckdb), "-bail"], input=setup + """
                ATTACH 'duckdb_objfs://shared.db' AS object_db;
                CREATE TABLE object_db.items AS SELECT i FROM range(3) t(i);
                CHECKPOINT object_db;
            """, text=True, check=True, capture_output=True)
            storage_args = ("duckdb_objfs://shared.db", "local", str(storage_root))

            # Invalid configuration and failed attachments must prevent startup.
            invalid_configs = [
                ("ordinary.db", "local", str(storage_root)),
                ("duckdb_objfs://", "local", str(storage_root)),
                ("duckdb_objfs://shared.db", "unsupported", str(storage_root)),
                ("duckdb_objfs://shared.db", "local"),
                ("duckdb_objfs://shared.db", "s3"),
                ("duckdb_objfs://missing.db", "local", str(storage_root)),
                ("", "s3", "prefix", "bucket"),
                (*storage_args, "unexpected-bucket"),
                (*storage_args, "bucket", "extra-argument"),
            ]
            for role in ("server", "worker"):
                for config in invalid_configs:
                    result = subprocess.run(
                        [str(binaries / f"distributed_{role}"), "127.0.0.1", str(free_port()),
                         "0" if role == "server" else "bad-worker", *config],
                        capture_output=True, text=True, timeout=30,
                    )
                    assert result.returncode != 0, (role, config, result.stdout)
                    assert "started successfully" not in result.stdout

            # Omitting the optional argument preserves the original startup behavior.
            default_driver, _ = start("server")
            assert query(default_driver, "SELECT 42 AS answer").to_pylist() == [{"answer": 42}]
            start("worker")

            driver, _ = start("server", storage_args)
            # Query before registration keeps this check independent of partition changes.
            assert query(driver, "SELECT i FROM object_db.items ORDER BY i")["i"].to_pylist() == [0, 1, 2]
            request = proto.DistributedRequest()
            request.execute_sql.sql = "INSERT INTO object_db.items VALUES (3)"
            response = action(driver, request, expect_success=False)
            assert "read-only" in response.error_message.lower()
            for index in range(2):
                worker, port = start("worker", storage_args)
                request = proto.DistributedRequest()
                request.execute_partition.sql = "SELECT i FROM object_db.items ORDER BY i"
                request.execute_partition.partition_id = 0
                request.execute_partition.total_partitions = 1
                action(worker, request)
                rows = worker.do_get(flight.Ticket(request.SerializeToString()), options=OPTIONS).read_all()
                assert rows["i"].to_pylist() == [0, 1, 2]
                register(driver, f"reader-{index}", port)

                # Startup must attach read-only, not create a second writer.
                request.execute_partition.sql = "INSERT INTO object_db.items VALUES (3)"
                try:
                    worker.do_get(flight.Ticket(request.SerializeToString()), options=OPTIONS).read_all()
                    raise AssertionError("Worker attachment allowed writes")
                except (flight.FlightError, pa.ArrowInvalid) as error:
                    assert "read-only" in str(error).lower()

            # Reject duplicate IDs/locations and malformed registrations.
            invalid_registrations = [
                ("reader-1", "127.0.0.1", free_port()),
                ("another-reader", "127.0.0.1", port),
                ("", "127.0.0.1", port),
                ("invalid-host", "", port),
                ("invalid-port", "127.0.0.1", 0),
                ("invalid-port", "127.0.0.1", 65536),
            ]
            for worker_id, host, worker_port in invalid_registrations:
                request = proto.DistributedRequest()
                request.worker_register.worker_id = worker_id
                request.worker_register.host = host
                request.worker_register.port = worker_port
                try:
                    action(driver, request)
                    raise AssertionError("Invalid registration was accepted")
                except (flight.FlightError, pa.ArrowInvalid):
                    pass
            request = proto.DistributedRequest()
            request.get_query_execution_stats.SetInParent()
            action(driver, request)  # The driver remains responsive after rejected requests.

            print("Driver/worker initialization and registration checks passed")
        finally:
            for process in reversed(processes):
                process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for log in logs:
                log.close()


if __name__ == "__main__":
    main()
