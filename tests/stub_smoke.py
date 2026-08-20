#!/usr/bin/env python
"""Standalone stub smoke test (stdlib-only, Python 3.6+ syntax).

CI runs this under Python 3.7 as the closest available proxy for the stub's 3.6 floor: it proves
the stub actually *boots and answers* on an old interpreter, complementing the static `vermin`
gate. It deliberately does NOT import the ``remoteslurm`` client package (which needs 3.9+ syntax),
so it can run under a bare 3.7 interpreter with no dependencies installed.

Usage: ``python tests/stub_smoke.py`` (exit 0 on success).
"""

import json
import os
import subprocess
import sys

STUB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "src",
    "remoteslurm",
    "stub.py",
)
RS = "\x1e"  # record separator prefixing every stub reply frame


def _readline(f):
    return f.readline().decode("utf-8")


def main():
    p = subprocess.Popen(
        [sys.executable, "-u", STUB],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        ready = _readline(p.stdout)
        assert ready.startswith("REMOTESLURM-READY"), f"no READY line: {ready!r}"

        def call(req):
            p.stdin.write((json.dumps(req) + "\n").encode("utf-8"))
            p.stdin.flush()
            return json.loads(_readline(p.stdout).lstrip(RS))

        ping = call({"id": "1", "op": "ping", "args": {}})
        assert ping["ok"] is True, ping
        assert "protocol" in ping["result"], ping

        # A bounded filesystem op, to exercise more than the trivial ping path.
        info = call({"id": "2", "op": "info", "args": {}})
        assert info["ok"] is True, info
        assert "user" in info["result"], info

        p.stdin.write((json.dumps({"op": "shutdown"}) + "\n").encode("utf-8"))
        p.stdin.flush()
        p.wait(timeout=10)
    finally:
        if p.poll() is None:
            p.kill()
    print(f"stub smoke OK under Python {sys.version.split()[0]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
