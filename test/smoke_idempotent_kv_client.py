"""End-to-end idempotent KV client retry and snapshot restore smoke test."""
import pathlib
import resource
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time


def no_core():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def receive(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        assert chunk, "connection closed before the complete frame arrived"
        data.extend(chunk)
    return bytes(data)


def receive_frame(sock):
    header = receive(sock, 8)
    _, length = struct.unpack("<II", header)
    assert 8 <= length <= 16 * 1024 * 1024
    return header + receive(sock, length - 8)


def read_kv(sock, key):
    frame = struct.pack("<IIIIH", 8, 24 + len(key), 4, 17, len(key)) + key + bytes(6)
    sock.sendall(frame)
    kind, length = struct.unpack("<II", receive(sock, 8))
    assert kind == 9 and 24 <= length <= 16 * 1024 * 1024
    body = receive(sock, length - 8)
    _, _, error = struct.unpack("<QII", body[:16])
    assert error == 0, (error, body)
    return body[16:]


def snapshot_boundary(path):
    data = path.read_bytes()
    if data[:8] == b"MRFTD002":
        length_offset = 40
    elif data[:8] == b"MRFTD003":
        voters = struct.unpack("<I", data[40:44])[0]
        length_offset = 44 + 4 * voters
    elif data[:8] == b"MRFTD004":
        voters = struct.unpack("<I", data[48:52])[0]
        length_offset = 52 + 4 * voters
    elif data[:8] == b"MRFTD005":
        voters = struct.unpack("<I", data[48:52])[0]
        assert voters <= 4096
        snapshot_offset = 52 + 4 * voters
        return struct.unpack("<Q", data[snapshot_offset:snapshot_offset + 8])[0]
    else:
        return 0
    length = struct.unpack("<Q", data[length_offset:length_offset + 8])[0]
    if not length:
        return 0
    return struct.unpack("<Q", data[length_offset + 16:length_offset + 24])[0]


def drop_one_reply(proxy, target, captured, errors, ready):
    try:
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", proxy))
            listener.listen(1)
            listener.settimeout(8)
            ready.set()
            client, _ = listener.accept()
            with client, socket.create_connection(("127.0.0.1", target), timeout=5) as upstream:
                client.settimeout(5)
                upstream.settimeout(5)
                upstream.sendall(receive_frame(client))
                captured.append(receive_frame(upstream))
                # The server applied and replied, but the client never sees the reply.
    except Exception as error:  # surfaced on the main test thread
        errors.append(error)


def drop_reply_then_forward_retry(proxy, target, requests, responses, errors, ready):
    try:
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", proxy))
            listener.listen(2)
            listener.settimeout(8)
            ready.set()
            for attempt in range(2):
                client, _ = listener.accept()
                with client, socket.create_connection(("127.0.0.1", target), timeout=5) as upstream:
                    client.settimeout(5)
                    upstream.settimeout(5)
                    request = receive_frame(client)
                    requests.append(request)
                    upstream.sendall(request)
                    response = receive_frame(upstream)
                    responses.append(response)
                    if attempt == 1:
                        client.sendall(response)
    except Exception as error:  # surfaced on the main test thread
        errors.append(error)


def main():
    binary = str(pathlib.Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="miniraft-idempotent-client-") as directory:
        root = pathlib.Path(directory)
        log_path = root / "server.log"
        endpoint = port()
        args = [binary, "--server", "--id", "1", "--node", f"127.0.0.1:{endpoint}:1",
                "--durable", "--snapshots", "1", "--idempotent"]
        process = None
        with log_path.open("w+") as log:
            def launch():
                return subprocess.Popen(args, cwd=root, stdout=log, stderr=log, preexec_fn=no_core)

            def wait_listening(child):
                deadline = time.monotonic() + 8
                while time.monotonic() < deadline:
                    assert child.poll() is None, "idempotent KV server exited during startup"
                    try:
                        probe = socket.create_connection(("127.0.0.1", endpoint), timeout=1)
                        probe.close()
                        return
                    except OSError:
                        time.sleep(0.02)
                raise AssertionError("idempotent KV server did not listen")

            def stop(child):
                child.terminate()
                child.wait(timeout=5)

            def client(state, address_port, commands):
                return subprocess.run(
                    [binary, "--idempotent", "--client-state", str(state),
                     "--node", f"127.0.0.1:{address_port}:1"],
                    input=commands, text=True, capture_output=True, timeout=10, cwd=root,
                    preexec_fn=no_core)

            try:
                process = launch()
                wait_listening(process)
                time.sleep(0.7)  # Let the single voter complete its election.

                reconnect_state = root / "reconnect.state"
                retry_proxy_port = port()
                retry_requests = []
                retry_responses = []
                retry_proxy_errors = []
                retry_proxy_ready = threading.Event()
                retry_proxy_thread = threading.Thread(
                    target=drop_reply_then_forward_retry,
                    args=(retry_proxy_port, endpoint, retry_requests, retry_responses,
                          retry_proxy_errors, retry_proxy_ready), daemon=True)
                retry_proxy_thread.start()
                assert retry_proxy_ready.wait(timeout=3), "retry proxy did not listen"
                reconnected = client(
                    reconnect_state, retry_proxy_port, "set acct_2 balance_30\n")
                assert reconnected.returncode == 0, reconnected.stdout + reconnected.stderr
                retry_proxy_thread.join(timeout=8)
                assert not retry_proxy_thread.is_alive(), "same-process retry did not finish"
                assert not retry_proxy_errors, retry_proxy_errors
                assert len(retry_requests) == 2 and len(retry_responses) == 2
                assert retry_requests[0] == retry_requests[1], "retry changed the operation identity"
                first_retry_reply, second_retry_reply = retry_responses
                assert struct.unpack_from("<Q", first_retry_reply, 16)[0] == \
                    struct.unpack_from("<Q", second_retry_reply, 16)[0]
                assert struct.unpack_from("<I", first_retry_reply, 60)[0] == 0
                assert struct.unpack_from("<I", second_retry_reply, 60)[0] == 0
                assert "Idempotent write committed at " in reconnected.stdout, reconnected.stdout
                assert (reconnect_state.stat().st_mode & 0o777) == 0o600

                client_state = root / "client.state"
                proxy_port = port()
                captured = []
                proxy_errors = []
                proxy_ready = threading.Event()
                proxy_thread = threading.Thread(
                    target=drop_one_reply,
                    args=(proxy_port, endpoint, captured, proxy_errors, proxy_ready), daemon=True)
                proxy_thread.start()
                assert proxy_ready.wait(timeout=3), "reply-drop proxy did not listen"
                first = client(client_state, proxy_port, "set acct_1 balance_20\n")
                assert first.returncode == 0, first.stdout + first.stderr
                proxy_thread.join(timeout=8)
                assert not proxy_thread.is_alive(), "reply-drop proxy did not finish"
                assert not proxy_errors, proxy_errors
                assert len(captured) == 1, "proxy did not capture the committed response"
                response = captured[0]
                kind, length = struct.unpack("<II", response[:8])
                assert kind == 22 and length == len(response) and length >= 64
                original_index = struct.unpack_from("<Q", response, 16)[0]
                original_error = struct.unpack_from("<I", response, 60)[0]
                assert original_error == 0, response
                assert "remains saved after reconnect retries" in first.stdout, first.stdout + first.stderr

                retry = client(client_state, endpoint, "")
                assert retry.returncode == 0, retry.stdout + retry.stderr
                assert f"Idempotent write committed at {original_index}" in retry.stdout, retry.stdout
                assert (client_state.stat().st_mode & 0o777) == 0o600

                durable = root / "state.durable.1"
                deadline = time.monotonic() + 8
                boundary = 0
                while time.monotonic() < deadline:
                    try:
                        boundary = snapshot_boundary(durable)
                    except FileNotFoundError:
                        boundary = 0
                    if boundary >= original_index:
                        break
                    time.sleep(0.02)
                assert boundary >= original_index, "combined snapshot was not compacted"
                stop(process)
                process = launch()
                wait_listening(process)
                with socket.create_connection(("127.0.0.1", endpoint), timeout=5) as sock:
                    sock.settimeout(5)
                    value = read_kv(sock, b"acct_1")
                assert value == b"balance_20", value

                deleted = client(client_state, endpoint, "del acct_1\n")
                assert deleted.returncode == 0, deleted.stdout + deleted.stderr
                assert "Idempotent write committed at " in deleted.stdout, deleted.stdout
                with socket.create_connection(("127.0.0.1", endpoint), timeout=5) as sock:
                    sock.settimeout(5)
                    value = read_kv(sock, b"acct_1")
                assert value == b"", value

                log.flush()
                log.seek(0)
                output = log.read()
                assert "ERROR: AddressSanitizer" not in output and "runtime error:" not in output, output
                print("Idempotent KV network client: same-process reconnect, restart retry, cached result and snapshot restore passed")
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
