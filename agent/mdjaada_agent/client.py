import json
import logging
import urllib.request

from . import USER_AGENT
from .buffer import DiskBuffer

log = logging.getLogger("mdjaada.agent")


class Api:
    """Outbound-only connection to the MDJaada platform."""

    def __init__(self, base: str, key: str, buffer: DiskBuffer, timeout: float = 10):
        self.base = base.rstrip("/")
        self.key = key
        self.buffer = buffer
        self.timeout = timeout

    def call(self, method: str, path: str, body=None):
        data = json.dumps(body, default=str).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method,
                                     headers={"Content-Type": "application/json", "X-API-Key": self.key,
                                              "User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return json.loads(resp.read() or b"null")

    def post_or_buffer(self, path: str, body) -> bool:
        try:
            self.flush()
            if len(self.buffer):
                raise RuntimeError("older payloads still buffered")  # keep delivery in order
            self.call("POST", path, body)
            return True
        except Exception as exc:
            kept = self.buffer.append({"path": path, "body": body})
            log.warning("POST %s failed (%s); %s", path, exc, "buffered to disk" if kept else "could not buffer, dropped")
            return False

    def flush(self) -> int:
        n = self.buffer.drain(lambda item: self.call("POST", item["path"], item["body"]))
        if n:
            log.info("delivered %d buffered payloads", n)
        return n
