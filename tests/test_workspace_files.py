import importlib.util
import time
from pathlib import Path

from remoteslurm.activity.store import ActivityStore
from remoteslurm.config import Config
from remoteslurm.workspace import service
from remoteslurm.workspace.observer import WorkspaceObserver


def test_browse_caps_actual_enumeration(monkeypatch, sandbox):
    path = Path(__file__).parents[1] / "src/remoteslurm/stub.py"
    spec = importlib.util.spec_from_file_location("bounded_browse_stub", path)
    stub = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(stub)

    class Entry:
        name = "entry"
        path = "unused"

    class Infinite:
        count = 0

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def __next__(self):
            self.count += 1
            assert self.count <= 4096
            return Entry()

    iterator = Infinite()
    monkeypatch.setattr(stub.os, "scandir", lambda _: iterator)
    monkeypatch.setattr(stub, "_stat_entry", lambda *a, **k: {"name": "entry", "type": "file"})
    result = stub.op_browse({"path": str(sandbox / "proj")})
    assert result["partial"] and result["inspected"] == 4096


def test_browse_preserves_links_and_read_bounds(cluster, sandbox):
    (sandbox / "proj/link").symlink_to(sandbox / "proj/sub", target_is_directory=True)
    result = cluster.call("browse", path=str(sandbox / "proj"))
    assert result["complete"]
    link = next(e for e in result["entries"] if e["name"] == "link")
    assert link["target"] == str(sandbox / "proj/sub")
    assert link["type"] == "link"


def test_tail_is_bounded_shared_and_detects_replacement(cluster, sandbox, monkeypatch):
    workspace = service.subscribe(
        service.select(
            Config(hosts={"local": cluster.host}),
            "local",
            path=str(sandbox / "proj"),
            cluster=cluster,
        )
    )
    observer = WorkspaceObserver(lambda _: cluster, lambda _: cluster.host)
    path = sandbox / "proj/selected.log"
    path.write_text("prefix\n" * 20000 + "last line\n")

    def tail():
        request = service.inspect_request(workspace, "tail", str(path))
        if request["state"] == "QUEUED":
            observer.inspect(request)
        return service.inspect_request(workspace, "tail", request_id=request["request_id"])

    first = tail()
    assert first["state"] == "DONE" and first["result"]["length"] <= 65536
    assert first["result"]["content"].endswith("last line\n")
    assert tail()["request_id"] == first["request_id"]
    # Advance the completed receipt age without sleeping or changing global clocks.
    first["completed_at"] = time.time() - 10
    ActivityStore().save("inspection", first["request_id"], first)
    path.rename(path.with_suffix(".old"))
    path.write_text("replacement\n")
    second = tail()
    assert "rotated" in second["result"]["gap"]
    assert second["result"]["content"] == "replacement\n"
    monkeypatch.setattr(cluster, "call", lambda *a, **k: (_ for _ in ()).throw(AssertionError()))
    assert service.inspect_request(workspace, "tail", str(path), cached_only=True)["result"]


def test_inspection_never_mutates_remote_and_binary_preview_is_metadata(
    cluster, sandbox, monkeypatch
):
    workspace = service.subscribe(
        service.select(
            Config(hosts={"local": cluster.host}),
            "local",
            path=str(sandbox / "proj"),
            cluster=cluster,
        )
    )
    original = cluster.call
    calls = []

    def read_only(op, **kwargs):
        assert op in {"workspace_anchor", "workspace_read", "browse"}
        calls.append(op)
        return original(op, **kwargs)

    monkeypatch.setattr(cluster, "call", read_only)
    observer = WorkspaceObserver(lambda _: cluster, lambda _: cluster.host)
    request = service.inspect_request(workspace, "preview", "bin.dat")
    observer.inspect(request)
    result = service.inspect_request(workspace, "preview", request_id=request["request_id"])[
        "result"
    ]
    assert result["binary"] and "content_b64" not in result
    assert calls == ["workspace_anchor", "workspace_read"]
