"""
Load test client for MyRedis-Java.

Speaks raw RESP over TCP (no redis-py dependency) so it can drive the
server exactly the way a real Redis client would. Opens one persistent
connection per worker thread, fires requests sequentially on each
connection, and reports aggregate throughput and latency percentiles.

Usage:
    python3 scripts/loadtest.py <PING|SET|GET> <concurrency> <requests_per_worker> [host] [port]

Examples:
    python3 scripts/loadtest.py PING 1 2000
    python3 scripts/loadtest.py SET 10 2000
    python3 scripts/loadtest.py GET 50 1000 127.0.0.1 6379
"""

import socket
import threading
import time
import sys
import statistics

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 6379


def resp_array(*args):
    out = f"*{len(args)}\r\n"
    for a in args:
        out += f"${len(a)}\r\n{a}\r\n"
    return out.encode()


def read_reply(sock, buf):
    """Reads exactly one RESP reply from the stream, buffering partial reads."""
    while b"\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("closed")
        buf.extend(chunk)
    line_end = buf.index(b"\r\n")
    line = bytes(buf[:line_end])
    del buf[:line_end + 2]
    if line.startswith(b"$"):
        n = int(line[1:])
        if n == -1:
            return None
        while len(buf) < n + 2:
            chunk = sock.recv(4096)
            if not chunk:
                raise ConnectionError("closed")
            buf.extend(chunk)
        data = bytes(buf[:n])
        del buf[:n + 2]
        return data
    elif line.startswith(b"*"):
        n = int(line[1:])
        return [read_reply(sock, buf) for _ in range(n)]
    else:
        return line


def worker(worker_id, num_requests, command, host, port, results, errors):
    try:
        sock = socket.create_connection((host, port))
        sock.settimeout(10)
        buf = bytearray()
        latencies = []
        key = f"benchkey:{worker_id}"
        for i in range(num_requests):
            if command == "PING":
                payload = resp_array("PING")
            elif command == "SET":
                payload = resp_array("SET", key, f"value-{worker_id}-{i}")
            elif command == "GET":
                payload = resp_array("GET", key)
            else:
                raise ValueError(f"Unsupported command: {command}")
            t0 = time.perf_counter()
            sock.sendall(payload)
            read_reply(sock, buf)
            t1 = time.perf_counter()
            latencies.append(t1 - t0)
        sock.close()
        results[worker_id] = latencies
    except Exception as e:
        errors[worker_id] = str(e)


def run_benchmark(command, concurrency, requests_per_worker, host, port):
    results = {}
    errors = {}
    threads = [
        threading.Thread(
            target=worker,
            args=(i, requests_per_worker, command, host, port, results, errors),
        )
        for i in range(concurrency)
    ]

    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    end = time.perf_counter()

    total_requests = sum(len(v) for v in results.values())
    wall_time = end - start
    all_latencies = sorted(l for v in results.values() for l in v)

    print(f"\n=== {command} | concurrency={concurrency} | requests/worker={requests_per_worker} ===")
    print(f"Total requests completed: {total_requests}")
    print(f"Errors: {len(errors)} {list(errors.values())[:3] if errors else ''}")
    print(f"Wall time: {wall_time:.3f}s")
    if total_requests > 0 and wall_time > 0:
        print(f"Throughput: {total_requests / wall_time:.1f} req/sec")
    if all_latencies:
        avg_ms = statistics.mean(all_latencies) * 1000
        p50_ms = all_latencies[len(all_latencies) // 2] * 1000
        p99_ms = all_latencies[int(len(all_latencies) * 0.99)] * 1000
        max_ms = all_latencies[-1] * 1000
        print(f"Latency avg: {avg_ms:.3f}ms  p50: {p50_ms:.3f}ms  p99: {p99_ms:.3f}ms  max: {max_ms:.3f}ms")


if __name__ == "__main__":
    command = sys.argv[1]
    concurrency = int(sys.argv[2])
    requests_per_worker = int(sys.argv[3])
    host = sys.argv[4] if len(sys.argv) > 4 else DEFAULT_HOST
    port = int(sys.argv[5]) if len(sys.argv) > 5 else DEFAULT_PORT
    run_benchmark(command, concurrency, requests_per_worker, host, port)
