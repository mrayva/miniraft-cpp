"""Real three-process durable catch-up smoke, separate from deterministic schedules."""
import pathlib
import signal
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
        endpoints = [f"127.0.0.1:{endpoint}:{node + 1}" for node, endpoint in enumerate(ports)]
        learner_config = root / "learners.conf"
        def learner_catalog(include_learner):
            voters = f"# Two voters\n\nvoter {endpoints[0]}\nvoter {endpoints[1]}\n"
            return voters + (f"learner {endpoints[2]} # staging\n" if include_learner else "")

        learner_config.write_text(learner_catalog(False))
        voter_config = root / "voters.conf"
        voter_config.write_text("".join(f"voter {endpoint}\n" for endpoint in endpoints))

        def start(index, use_role=True):
            args = [binary, "--server", "--id", str(index + 1), "--durable"]
            if learner_mode:
                args += ["--peer-config", str(learner_config if use_role else voter_config)]
            else:
                for endpoint in endpoints:
                    args += ["--node", endpoint]
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

        def reload(contents, diagnostic):
            log_paths = [directory / "output.log" for directory in directories[:2]]
            counts = [path.read_text().count(diagnostic) for path in log_paths]
            staged = learner_config.with_suffix(".new")
            staged.write_text(contents)
            staged.replace(learner_config)
            for process in processes[:2]:
                process.send_signal(signal.SIGHUP)
            wait_for(lambda: all(path.read_text().count(diagnostic) > count
                                 for path, count in zip(log_paths, counts)), 5)

        try:
            if learner_mode:
                invalid_config = root / "invalid.conf"
                invalid_args = [binary, "--server", "--durable", "--id", "1",
                                "--peer-config", str(invalid_config)]
                for contents, diagnostic in (
                    (f"voter {endpoints[0]}\nlearner {endpoints[0]}\n", "Duplicate node ID"),
                    (f"voter {endpoints[0]}\nlearner {endpoints[0].rsplit(':', 1)[0]}:3\n",
                     "Duplicate node address and port"),
                    (f"voter {endpoints[1]}\n", "Local node ID is missing"),
                    (f"learner {endpoints[0]}\n", "requires at least one voter"),
                    (f"voter {endpoints[0]} extra\n", "Expected voter or learner"),
                ):
                    invalid_config.write_text(contents)
                    result = subprocess.run(invalid_args, cwd=directories[0], capture_output=True,
                                            text=True, timeout=5)
                    assert result.returncode != 0 and diagnostic in result.stderr, result
                    assert not (directories[0] / "state.durable.1").exists()
            start(0)
            start(1)  # Nodes 1 and 2 form a voter quorum in either configuration.
            values = {b"a": b"a" * 60000, b"b": b"b" * 60000, b"c": b"c" * 60000}
            last = [0]
            for key, value in values.items():
                def write():
                    last[0], _ = call(0, key, value)
                    return last[0] > 0
                wait_for(write, 30)
                if key == b"a":
                    if learner_mode:
                        invalid = (f"learner {endpoints[2]}\nlearner {endpoints[0]}\n"
                                   f"voter {endpoints[1]}\n")
                        reload(invalid, "Peer config reload cannot change bootstrap roles")
                        reload(learner_catalog(True), "Reloaded peer config (1 endpoint changes)")
                    start(2)  # The established leader learns voter 3's snapshot support.
                    wait_for(lambda: call(2, key)[1] == value, 15)
                    processes[2].terminate()
                    processes[2].wait(timeout=5)

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
                new_port = port()
                while new_port in ports:
                    new_port = port()
                ports[2] = new_port
                endpoints[2] = f"127.0.0.1:{new_port}:3"
                reload(learner_catalog(True), "Reloaded peer config (1 endpoint changes)")
            start(2)
            wait_for(lambda: caught_up() and call(2, b"tail")[1] == b"after install", 25)
            if learner_mode:
                wait_for(lambda: call(0, b"moved", b"new endpoint")[0] > last[0], 15)
                wait_for(lambda: call(2, b"moved")[1] == b"new endpoint", 15)
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
