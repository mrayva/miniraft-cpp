"""Real three-process durable catch-up smoke, separate from deterministic schedules."""
import pathlib
import socket
import struct
import subprocess
import sys
import tempfile
import time
sys.dont_write_bytecode = True
from smoke_kv_checkpoints import no_core, port, request, snapshot_boundary


def main():
    binary = str(pathlib.Path(sys.argv[1]).resolve())
    learner_mode = len(sys.argv) > 2 and sys.argv[2] == "learner"
    with tempfile.TemporaryDirectory(prefix="miniraft-install-snapshot-") as temporary:
        root = pathlib.Path(temporary)
        ports = []
        while len(ports) < 3:
            candidate = port()
            if candidate not in ports:
                ports.append(candidate)
        logs = []
        directories = []
        for index in range(3):
            directory = root / str(index + 1)
            directory.mkdir()
            directories.append(directory)
            logs.append((directory / "output.log").open("w+"))
        processes = [None] * 3

        def start(index, use_role=True):
            args = [binary, "--server", "--id", str(index + 1), "--durable"]
            for node, endpoint in enumerate(ports):
                args += ["--node", f"127.0.0.1:{endpoint}:{node + 1}"]
            if learner_mode and use_role:
                args += ["--learner", "3"]
            if index < 2:
                args += ["--snapshots", "1"]
            processes[index] = subprocess.Popen(args, cwd=directories[index], stdout=logs[index], stderr=logs[index], preexec_fn=no_core)

        def call(index, key, value=None):
            child = processes[index]
            assert child is not None and child.poll() is None, f"node {index + 1} exited"
            with socket.create_connection(("127.0.0.1", ports[index]), timeout=1) as sock:
                sock.settimeout(2)
                return request(sock, key, value, flags=2)  # local stale read, never forwarded

        def wait_for(action, timeout):
            end = time.monotonic() + timeout
            while time.monotonic() < end:
                try:
                    if action():
                        return
                except (OSError, AssertionError):
                    pass
                time.sleep(0.05)
            raise AssertionError("timed out waiting for cluster progress")

        try:
            start(0)
            start(1)  # two voters form a majority; node 3 starts with no log later
            values = {b"a": b"a" * 60000, b"b": b"b" * 60000, b"c": b"c" * 60000}
            last = [0]
            for key, value in values.items():
                def write():
                    last[0], _ = call(0, key, value)
                    return last[0] > 0
                wait_for(write, 30)

            def compacted():
                for index in (0, 1):
                    image = directories[index] / f"state.snapshot.{index + 1}"
                    durable = directories[index] / f"state.durable.{index + 1}"
                    if not image.exists() or struct.unpack("<Q", image.read_bytes()[8:16])[0] < last[0]:
                        return False
                    if snapshot_boundary(durable) < last[0]:
                        return False
                return True
            wait_for(compacted, 15)
            # Remove queued pre-compaction RPCs too: after restart neither active
            # node can supply the old prefix by log replication, only by snapshot.
            for index in (0, 1):
                processes[index].terminate()
            for index in (0, 1):
                processes[index].wait(timeout=5)
            start(0)
            start(1)
            start(2)

            def caught_up():
                durable = directories[2] / "state.durable.3"
                return durable.exists() and snapshot_boundary(durable) > 0 and all(call(2, key)[1] == value for key, value in values.items())
            wait_for(caught_up, 25)
            # Node 3 has no periodic snapshotting: its compacted image necessarily
            # came from the InstallSnapshot RPC, not a local checkpoint timer.
            assert not (directories[2] / "state.snapshot.3").exists()
            wait_for(lambda: call(0, b"tail", b"after install")[0] > last[0], 15)
            wait_for(lambda: call(2, b"tail")[1] == b"after install", 15)
            processes[2].terminate()
            processes[2].wait(timeout=5)
            if learner_mode:
                durable = directories[2] / "state.durable.3"
                before = durable.read_bytes()
                assert struct.unpack("<I", before[28:32])[0] == 0, "learner persisted a vote"
                start(2, use_role=False)
                assert processes[2].wait(timeout=5) != 0, "restart must not promote a learner"
                assert durable.read_bytes() == before
            start(2)
            wait_for(lambda: caught_up() and call(2, b"tail")[1] == b"after install", 25)
            for log in logs:
                log.flush(); log.seek(0)
                output = log.read()
                assert "ERROR: AddressSanitizer" not in output and "runtime error:" not in output, output
            print(("Learner" if learner_mode else "InstallSnapshot") + " CLI: compacted majority, empty-node catch-up, tail replication and durable restart passed")
        except Exception:
            for index, log in enumerate(logs):
                log.flush(); log.seek(0)
                print(f"node {index + 1}:\n{log.read()}", file=sys.stderr)
            raise
        finally:
            for child in processes:
                if child is not None and child.poll() is None:
                    child.kill(); child.wait(timeout=5)
            for log in logs:
                log.close()


if __name__ == "__main__":
    main()
