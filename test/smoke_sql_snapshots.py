"""SQL CLI compaction and restart smoke; fault schedules live in C++ tests."""
import pathlib
import re
import socket
import subprocess
import sys
import tempfile
import time
sys.dont_write_bytecode = True
from smoke_kv_checkpoints import no_core, port, snapshot_boundary


def main():
    binary = str(pathlib.Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="miniraft-sql-snapshot-cli-") as temporary:
        root = pathlib.Path(temporary)
        endpoint = port()
        node = f"127.0.0.1:{endpoint}:1"
        args = [binary, "--server", "--id", "1", "--node", node, "--durable"]
        log_path = root / "server.log"
        process = None
        with log_path.open("w+") as log:
            def start(periodic):
                extra = ["--snapshots", "1"] if periodic else []
                return subprocess.Popen([*args, *extra], cwd=root, stdout=log, stderr=log, preexec_fn=no_core)

            def stop(child):
                child.terminate()
                child.wait(timeout=5)

            def wait_ready(child):
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    assert child.poll() is None, "SQL server exited during startup"
                    try:
                        with socket.create_connection(("127.0.0.1", endpoint), timeout=0.1):
                            return
                    except OSError:
                        time.sleep(0.05)
                raise AssertionError("SQL server did not listen")

            def run_client(line):
                return subprocess.run([binary, "--client", "--node", node], cwd=root,
                    input=line + "\n", text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    timeout=15, preexec_fn=no_core)

            try:
                for options in (["--server", "--snapshots", "1"],
                                ["--server", "--durable", "--snapshots", "0"],
                                ["--server", "--durable", "--snapshots"]):
                    rejected = subprocess.run([binary, *options], cwd=root, stdout=log,
                        stderr=log, timeout=5, preexec_fn=no_core)
                    assert rejected.returncode != 0, options

                process = start(periodic=True)
                wait_ready(process)
                write = "CREATE TABLE IF NOT EXISTS t (value TEXT); DELETE FROM t; INSERT INTO t VALUES ('persisted');"
                write_index = None
                deadline = time.monotonic() + 35
                while time.monotonic() < deadline:
                    assert process.poll() is None, "SQL server exited before accepting the write"
                    result = run_client(write)
                    match = re.search(r"commitIndex: (\d+)", result.stdout)
                    if match:
                        write_index = int(match.group(1))
                        break
                    time.sleep(0.1)
                assert write_index is not None, "SQL write did not commit"

                durable = root / "sql_state.durable.1"
                deadline = time.monotonic() + 12
                while time.monotonic() < deadline:
                    if durable.exists() and snapshot_boundary(durable) >= write_index:
                        break
                    assert process.poll() is None, "SQL server exited during compaction"
                    time.sleep(0.05)
                else:
                    raise AssertionError("SQL snapshot did not compact through the committed write")

                stop(process)
                process = start(periodic=False)
                wait_ready(process)
                result = run_client("select value from t")
                assert "value\npersisted\n" in result.stdout, result.stdout
                stop(process)

                log.flush()
                log.seek(0)
                output = log.read()
                assert "ERROR: AddressSanitizer" not in output and "runtime error:" not in output, output
                print("SQL snapshot CLI: option checks, compaction and durable restart/read passed")
            except Exception:
                log.flush()
                log.seek(0)
                print(log.read(), file=sys.stderr)
                raise
            finally:
                if process is not None and process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)


if __name__ == "__main__":
    main()
