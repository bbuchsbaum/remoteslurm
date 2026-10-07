import asyncio
import time

import pytest

pytest.importorskip("textual")
from textual.widgets import DataTable, Input, TabbedContent, Tree  # noqa: E402

from remoteslurm.activity.store import ActivityStore  # noqa: E402
from remoteslurm.config import Config  # noqa: E402
from remoteslurm.tui.app import WorkspaceApp  # noqa: E402
from remoteslurm.workspace import service  # noqa: E402
from remoteslurm.workspace.observer import WorkspaceObserver  # noqa: E402


@pytest.mark.parametrize("size", [(80, 24), (120, 40)])
def test_terminal_keyboard_resize_and_offline_exit(cluster, sandbox, monkeypatch, size):
    cfg = Config(hosts={"local": cluster.host}, default_host="local")
    workspace = service.select(cfg, "local", path=str(sandbox / "proj"), cluster=cluster)
    store = ActivityStore()
    store.save(
        "view",
        workspace["workspace_id"],
        {
            "state": "HEALTHY",
            "observed_at": time.time(),
            "work": [
                {
                    "job_id": "123",
                    "work_id": "work",
                    "name": "example",
                    "execution": "COMPLETED",
                    "artifacts": "MISSING",
                    "validation": "NOT_RUN",
                    "stdout_path": "/remote/log.out",
                }
            ],
        },
    )

    def forbidden(*a, **k):
        pytest.fail("offline UI attempted remote read")

    monkeypatch.setattr(cluster, "call", forbidden)

    async def exercise():
        app = WorkspaceApp(workspace, offline=True)
        async with app.run_test(size=size) as pilot:
            await pilot.pause()
            assert app.query_one("#jobs", DataTable).row_count == 1
            await pilot.press("slash")
            assert app.query_one("#tabs", TabbedContent).active == "files-tab"
            await pilot.resize_terminal(80, 24)
            assert app.has_class("narrow")
            await pilot.press("escape", "tab")
            app.set_focus(app.query_one("#files", Tree))
            await pilot.press("q")
        assert not store.get("workspace", workspace["workspace_id"])["enabled"]

    asyncio.run(exercise())


@pytest.mark.parametrize("size", [(80, 24), (120, 40)])
def test_files_mouse_open_and_path_return(cluster, sandbox, size):
    from textual.widgets import RichLog

    cfg = Config(hosts={"local": cluster.host}, default_host="local")
    workspace = service.subscribe(
        service.select(cfg, "local", path=str(sandbox / "proj"), cluster=cluster)
    )
    observer = WorkspaceObserver(lambda _: cluster, lambda _: cluster.host)

    async def settle(app, pilot):
        for request in ActivityStore().documents("inspection"):
            if request["state"] == "QUEUED":
                await asyncio.to_thread(observer.inspect, request)
        await app.poll()
        await pilot.pause()

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=size) as pilot:
            await settle(app, pilot)
            await pilot.press("f")
            await pilot.pause()
            tree = app.query_one("#files", Tree)
            directory = next(n for n in tree.root.children if n.data["type"] == "dir")
            tree.move_cursor(directory)
            await pilot.pause()
            assert await pilot.click(tree, offset=(8, tree.cursor_line))
            await settle(app, pilot)
            assert directory.is_expanded
            file = next(n for n in directory.children if n.data["path"].endswith("/c.py"))
            tree.move_cursor(file)
            await pilot.pause()
            assert await pilot.click(tree, offset=(13, tree.cursor_line))
            await settle(app, pilot)
            preview = app.query_one("#logs" if size[0] < 100 else "#preview", RichLog)
            # Tree selection and the timer poll both await threads; press/pause does not
            # establish that the asynchronous preview has finished rendering.
            for _ in range(20):
                if "import os" in "\n".join(line.text for line in preview.lines):
                    break
                await settle(app, pilot)
                await pilot.pause(0.05)
            assert "import os" in "\n".join(line.text for line in preview.lines)
            await pilot.press("f", "slash")
            assert app.focused is app.query_one("#path", Input)
            await pilot.press("escape")
            assert app.focused is tree
            await pilot.press("ctrl+c")

    asyncio.run(exercise())


def test_file_selection_uses_observer_queue(cluster, sandbox):
    cfg = Config(hosts={"local": cluster.host}, default_host="local")
    workspace = service.subscribe(
        service.select(cfg, "local", path=str(sandbox / "proj"), cluster=cluster)
    )
    observer = WorkspaceObserver(lambda _: cluster, lambda _: cluster.host)

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            pending = ActivityStore().documents("inspection")
            assert pending and pending[0]["state"] == "QUEUED"
            await asyncio.to_thread(observer.inspect, pending[0])
            await app.poll()
            tree = app.query_one("#files", Tree)
            assert any(str(node.label).startswith("a.txt ") for node in tree.root.children)
            app.set_focus(tree)
            await pilot.press("q")
        assert ActivityStore().get("workspace", workspace["workspace_id"])["enabled"]

    asyncio.run(exercise())


def test_ctrl_c_exits_even_in_path_input(cluster, sandbox):
    cfg = Config(hosts={"local": cluster.host}, default_host="local")
    workspace = service.select(cfg, "local", path=str(sandbox / "proj"), cluster=cluster)

    async def exercise():
        app = WorkspaceApp(workspace, offline=True)
        async with app.run_test(size=(80, 24)) as pilot:
            await pilot.press("slash", "ctrl+c")
            await pilot.pause()
            assert not app.is_running

    asyncio.run(exercise())


def test_unavailable_navigation_preserves_displayed_root(cluster, sandbox):
    cfg = Config(hosts={"local": cluster.host}, default_host="local")
    workspace = service.select(cfg, "local", path=str(sandbox / "proj"), cluster=cluster)

    async def exercise():
        app = WorkspaceApp(workspace, offline=True)
        async with app.run_test(size=(80, 24)) as pilot:
            await pilot.press("slash")
            app.query_one("#path", Input).value = "/uncached/other/root"
            await pilot.press("enter")
            tree = app.query_one("#files", Tree)
            assert str(tree.root.label) == workspace["remote_root"]
            assert tree.root.data["path"] == workspace["remote_root"]
            await pilot.press("ctrl+c")

    asyncio.run(exercise())


@pytest.mark.parametrize("size", [(80, 24), (120, 40)])
def test_files_tab_keyboard_journey(cluster, sandbox, size):
    cfg = Config(hosts={"local": cluster.host}, default_host="local")
    workspace = service.subscribe(
        service.select(cfg, "local", path=str(sandbox / "proj"), cluster=cluster)
    )
    observer = WorkspaceObserver(lambda _: cluster, lambda _: cluster.host)

    async def settle(app, pilot):
        for request in ActivityStore().documents("inspection"):
            if request["state"] == "QUEUED":
                await asyncio.to_thread(observer.inspect, request)
        await app.poll()
        await pilot.pause()

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=size) as pilot:
            await settle(app, pilot)
            tabs = app.query_one("#tabs", TabbedContent)
            await pilot.click(tabs.get_tab("files-tab"))
            await pilot.pause()
            tree = app.query_one("#files", Tree)
            assert app.focused is tree
            await pilot.press("down")
            assert tree.cursor_node is not tree.root
            directory = next(n for n in tree.root.children if n.data["type"] == "dir")
            tree.move_cursor(directory)
            await pilot.press("right")
            await settle(app, pilot)
            assert directory.is_expanded
            assert any(n.data["path"].endswith("/c.py") for n in directory.children)
            file = next(n for n in directory.children if n.data["path"].endswith("/c.py"))
            tree.move_cursor(file)
            await pilot.press("enter")
            await settle(app, pilot)
            from textual.widgets import RichLog

            preview = app.query_one("#logs" if size[0] < 100 else "#preview", RichLog)
            for _ in range(20):
                if "import os" in "\n".join(line.text for line in preview.lines):
                    break
                await settle(app, pilot)
                await pilot.pause(0.05)
            assert "import os" in "\n".join(line.text for line in preview.lines)
            await pilot.press("ctrl+c")

    asyncio.run(exercise())


def test_failed_folder_can_be_retried_and_refresh_keeps_files_tab(cluster, sandbox, monkeypatch):
    from remoteslurm.errors import PermissionDenied

    cfg = Config(hosts={"local": cluster.host}, default_host="local")
    workspace = service.subscribe(
        service.select(cfg, "local", path=str(sandbox / "proj"), cluster=cluster)
    )
    observer = WorkspaceObserver(lambda _: cluster, lambda _: cluster.host)
    original = cluster.call
    fail_once = True

    def unavailable(op, **kwargs):
        nonlocal fail_once
        if op == "browse" and kwargs["path"].endswith("/sub") and fail_once:
            fail_once = False
            raise PermissionDenied("temporary directory failure")
        return original(op, **kwargs)

    monkeypatch.setattr(cluster, "call", unavailable)

    async def settle(app, pilot):
        for request in ActivityStore().documents("inspection"):
            if request["state"] == "QUEUED":
                await asyncio.to_thread(observer.inspect, request)
        await app.poll()
        await pilot.pause()

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=(80, 24)) as pilot:
            await settle(app, pilot)
            await pilot.press("f")
            tree = app.query_one("#files", Tree)
            directory = next(n for n in tree.root.children if n.data["type"] == "dir")
            tree.move_cursor(directory)
            await pilot.press("right")
            await settle(app, pilot)
            assert not directory.data.get("loaded")
            assert not directory.data.get("loading")
            await pilot.press("left", "right")
            await settle(app, pilot)
            assert directory.children and directory.data["loaded"]
            await pilot.press("r")
            await settle(app, pilot)
            assert app.query_one("#tabs", TabbedContent).active == "files-tab"
            assert tree.cursor_node is directory
            await pilot.press("ctrl+c")

    asyncio.run(exercise())
