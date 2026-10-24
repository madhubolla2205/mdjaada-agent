"""Append-only JSONL disk buffer with a size cap. Used when the platform API is unreachable,
so check results recorded during an outage are delivered (in order) once it comes back."""
import json
import logging
import os
import threading

log = logging.getLogger("mdjaada.agent")


class DiskBuffer:
    def __init__(self, path: str, max_bytes: int = 50 * 1024 * 1024):
        self.path = path
        self.max_bytes = max_bytes
        self._lock = threading.Lock()
        self.dropped = 0   # payloads lost because the buffer itself could not be written (e.g. a full disk)
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

    def __len__(self) -> int:
        with self._lock:
            return len(self._read())

    def _read(self) -> list[str]:
        try:
            with open(self.path, encoding="utf-8") as f:
                return [line for line in f.read().splitlines() if line.strip()]
        except FileNotFoundError:
            return []

    def _write(self, lines: list[str]) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write("".join(line + "\n" for line in lines))
        os.replace(tmp, self.path)

    def append(self, item: dict) -> bool:
        """Buffer one payload; False (and the payload dropped) when the disk can't take it. Checks keep running:
        a full disk must never crash the agent."""
        with self._lock:
            try:
                with open(self.path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(item, separators=(",", ":")) + "\n")
                if os.path.getsize(self.path) > self.max_bytes:
                    lines = self._read()
                    size = sum(len(line) + 1 for line in lines)
                    while lines and size > self.max_bytes * 0.8:  # drop oldest first
                        size -= len(lines.pop(0)) + 1
                    self._write(lines)
                return True
            except OSError as exc:
                self.dropped += 1
                if self.dropped == 1 or self.dropped % 100 == 0:
                    log.error("cannot buffer to %s (%s): %d payload(s) dropped so far", self.path, exc, self.dropped)
                return False

    def drain(self, send) -> int:
        """Send buffered items oldest-first; stop at the first failure and keep the rest."""
        with self._lock:
            lines = self._read()
            if not lines:
                return 0
            sent = 0
            for line in lines:
                try:
                    send(json.loads(line))
                except Exception:
                    break
                sent += 1
            try:
                self._write(lines[sent:])
            except OSError as exc:   # sent but not removed: they may be delivered twice, never lost
                log.error("cannot rewrite %s (%s)", self.path, exc)
            return sent
