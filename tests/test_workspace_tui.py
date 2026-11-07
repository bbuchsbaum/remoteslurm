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


async def _press_to_exit(app, pilot, *keys):
    """Press keys that close the app.

    Textual's pilot can wait forever for an app that already exited while handling the key,
    so bound the wait and require that the app really closed.
    """
    try:
        await asyncio.wait_for(pilot.press(*keys), 10)
    except TimeoutError:
        assert not app.is_running


async def _drain(app, pilot, observer, only=None):
    """Answer queued inspections until the view has nothing pending.

    Key and click handlers queue their reads from async handlers that may still be running
    when the pilot returns, so a single pass can miss them.
    """
    idle = 0
    for _ in range(20):
        await pilot.pause(0.02)
        queued = [
            request
            for request in ActivityStore().documents("inspection")
            if request["state"] == "QUEUED" and (only is None or only(request))
        ]
        for request in queued:
            await asyncio.to_thread(observer.inspect, request)
        await app.poll()
        await pilot.pause()
        idle = idle + 1 if not queued and not app.pending else 0
        if only is not None or idle >= 2:
            return


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
            await _press_to_exit(app, pilot, "q")
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
        await _drain(app, pilot, observer)

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
            preview = app.query_one("#preview", RichLog)
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
            await _press_to_exit(app, pilot, "ctrl+c")

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
            for _ in range(50):
                pending = ActivityStore().documents("inspection")
                if pending:
                    break
                await pilot.pause(0.02)
            assert pending and pending[0]["state"] == "QUEUED"
            await asyncio.to_thread(observer.inspect, pending[0])
            await app.poll()
            tree = app.query_one("#files", Tree)
            assert any(str(node.label).startswith("a.txt ") for node in tree.root.children)
            app.set_focus(tree)
            await _press_to_exit(app, pilot, "q")
        assert ActivityStore().get("workspace", workspace["workspace_id"])["enabled"]

    asyncio.run(exercise())


def test_ctrl_c_exits_even_in_path_input(cluster, sandbox):
    cfg = Config(hosts={"local": cluster.host}, default_host="local")
    workspace = service.select(cfg, "local", path=str(sandbox / "proj"), cluster=cluster)

    async def exercise():
        app = WorkspaceApp(workspace, offline=True)
        async with app.run_test(size=(80, 24)) as pilot:
            await _press_to_exit(app, pilot, "slash", "ctrl+c")
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
            await _press_to_exit(app, pilot, "ctrl+c")

    asyncio.run(exercise())


@pytest.mark.parametrize("size", [(80, 24), (120, 40)])
def test_files_tab_keyboard_journey(cluster, sandbox, size):
    cfg = Config(hosts={"local": cluster.host}, default_host="local")
    workspace = service.subscribe(
        service.select(cfg, "local", path=str(sandbox / "proj"), cluster=cluster)
    )
    observer = WorkspaceObserver(lambda _: cluster, lambda _: cluster.host)

    async def settle(app, pilot):
        await _drain(app, pilot, observer)

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

            preview = app.query_one("#preview", RichLog)
            for _ in range(20):
                if "import os" in "\n".join(line.text for line in preview.lines):
                    break
                await settle(app, pilot)
                await pilot.pause(0.05)
            assert "import os" in "\n".join(line.text for line in preview.lines)
            await _press_to_exit(app, pilot, "ctrl+c")

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
        await _drain(app, pilot, observer)

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
            await _press_to_exit(app, pilot, "ctrl+c")

    asyncio.run(exercise())


def _online(cluster, sandbox, work=None):
    cfg = Config(hosts={"local": cluster.host}, default_host="local")
    workspace = service.subscribe(
        service.select(cfg, "local", path=str(sandbox / "proj"), cluster=cluster)
    )
    if work is not None:
        ActivityStore().save(
            "view",
            workspace["workspace_id"],
            {"state": "HEALTHY", "observed_at": time.time(), "work": work},
        )
    observer = WorkspaceObserver(lambda _: cluster, lambda _: cluster.host)

    async def settle(app, pilot, only=None):
        await _drain(app, pilot, observer, only)

    return workspace, settle


def _job(sandbox, **extra):
    return {
        "job_id": "123",
        "work_id": "work",
        "name": "example",
        "execution": "FAILED",
        "artifacts": "MISSING",
        "validation": "NOT_RUN",
        "workdir": str(sandbox / "proj" / "sub"),
        "stdout_path": str(sandbox / "proj" / "b.log"),
        **extra,
    }


def _text(log):
    return "\n".join(line.text for line in log.lines)


def test_load_more_appends_to_the_folder(cluster, sandbox):
    for i in range(450):
        (sandbox / "proj" / f"f{i:03}.txt").write_text("x")
    workspace, settle = _online(cluster, sandbox)

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=(120, 40)) as pilot:
            await settle(app, pilot)
            await pilot.press("f")
            tree = app.query_one("#files", Tree)
            names = [n.data["path"].rsplit("/", 1)[-1] for n in tree.root.children]
            assert "a.txt" in names and "f196.txt" not in names
            tree.move_cursor(tree.root.children[-1])
            await pilot.press("enter")
            await pilot.pause()
            names = [n.data["path"].rsplit("/", 1)[-1] for n in tree.root.children]
            assert {"a.txt", "f000.txt", "f196.txt", "f395.txt"} <= set(names)
            assert tree.root.children[-1].data["type"] == "page"
            assert tree.cursor_node.data["path"].endswith("/f196.txt")
            await _press_to_exit(app, pilot, "ctrl+c")

    asyncio.run(exercise())


def test_unreadable_receipt_does_not_stall_polling(cluster, sandbox):
    workspace, settle = _online(cluster, sandbox)

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=(120, 40)) as pilot:
            app.pending["missing"] = {
                "node": None,
                "kind": "preview",
                "slot": "stale",
                "gen": 0,
                "created": time.time(),
                "path": "/gone",
                "quiet": True,
            }
            # The 1s timer may be mid-poll and make one settle a no-op; settle twice.
            await settle(app, pilot)
            await settle(app, pilot)
            assert not app.pending
            assert "unavailable" not in str(app.query_one("#status").render())
            assert any(n.data["type"] == "dir" for n in app.query_one("#files", Tree).root.children)
            await _press_to_exit(app, pilot, "ctrl+c")

    asyncio.run(exercise())


def test_newer_preview_wins_over_late_older_result(cluster, sandbox):
    from textual.widgets import RichLog

    workspace, settle = _online(cluster, sandbox)

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=(120, 40)) as pilot:
            await settle(app, pilot)
            await pilot.press("2")
            await app.request("preview", "a.txt")
            await app.request("preview", "sub/c.py")
            await settle(app, pilot, only=lambda r: r["path"].endswith("c.py"))
            await settle(app, pilot)
            preview = _text(app.query_one("#preview", RichLog))
            assert "import os" in preview and "alpha" not in preview
            await _press_to_exit(app, pilot, "ctrl+c")

    asyncio.run(exercise())


def test_narrow_preview_stays_in_files_and_survives_log_tail(cluster, sandbox):
    from textual.widgets import RichLog

    workspace, settle = _online(cluster, sandbox, work=[_job(sandbox)])

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=(80, 24)) as pilot:
            await settle(app, pilot)
            await pilot.press("l")
            await settle(app, pilot)
            assert app.query_one("#tabs", TabbedContent).active == "logs-tab"
            assert "line 999" in _text(app.query_one("#logs", RichLog))
            await pilot.press("2")
            await pilot.pause()
            tree = app.query_one("#files", Tree)
            assert app.focused is tree
            text_file = next(n for n in tree.root.children if n.data["path"].endswith("a.txt"))
            tree.move_cursor(text_file)
            await pilot.press("enter")
            await settle(app, pilot)
            preview = app.query_one("#preview", RichLog)
            assert app.has_class("previewing") and app.focused is preview
            assert app.query_one("#tabs", TabbedContent).active == "files-tab"
            app.log_at = 0
            await settle(app, pilot)
            assert "alpha" in _text(preview)
            assert "line 999" in _text(app.query_one("#logs", RichLog))
            await pilot.press("escape")
            assert not app.has_class("previewing") and app.focused is tree
            await _press_to_exit(app, pilot, "ctrl+c")

    asyncio.run(exercise())


def test_tab_keys_and_escape_stay_in_context(cluster, sandbox):
    from textual.widgets import RichLog

    workspace, settle = _online(cluster, sandbox, work=[_job(sandbox)])

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=(120, 40)) as pilot:
            await settle(app, pilot)
            tabs = app.query_one("#tabs", TabbedContent)
            for key, tab, widget in (
                ("4", "activity-tab", "#activity"),
                ("3", "logs-tab", "#logs"),
                ("2", "files-tab", "#files"),
                ("1", "work-tab", "#jobs"),
            ):
                await pilot.press(key)
                await pilot.pause()
                assert tabs.active == tab and app.focused is app.query_one(widget)
                await pilot.press("escape")
                assert tabs.active == tab
            assert isinstance(app.query_one("#logs"), RichLog)
            await _press_to_exit(app, pilot, "ctrl+c")

    asyncio.run(exercise())


def test_job_enter_opens_log_and_workdir_and_parent_navigation(cluster, sandbox):
    workspace, settle = _online(cluster, sandbox, work=[_job(sandbox)])

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=(120, 40)) as pilot:
            await settle(app, pilot)
            tabs = app.query_one("#tabs", TabbedContent)
            await pilot.press("enter")
            assert tabs.active == "logs-tab" and app.log_stream == "stdout"
            await pilot.press("1", "o")
            await settle(app, pilot)
            tree = app.query_one("#files", Tree)
            assert tabs.active == "files-tab"
            assert tree.root.data["path"] == str(sandbox / "proj" / "sub")
            assert app.query_one("#path", Input).value == str(sandbox / "proj" / "sub")
            await pilot.press("backspace")
            await settle(app, pilot)
            assert tree.root.data["path"] == str(sandbox / "proj")
            assert tree.cursor_node.data["path"] == str(sandbox / "proj" / "sub")
            await _press_to_exit(app, pilot, "ctrl+c")

    asyncio.run(exercise())


def test_refresh_keeps_open_folders_and_cursor(cluster, sandbox):
    workspace, settle = _online(cluster, sandbox)

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=(120, 40)) as pilot:
            await settle(app, pilot)
            await pilot.press("f")
            tree = app.query_one("#files", Tree)
            directory = next(n for n in tree.root.children if n.data["type"] == "dir")
            tree.move_cursor(directory)
            await pilot.press("right")
            await settle(app, pilot)
            file = next(n for n in directory.children if n.data["path"].endswith("/c.py"))
            tree.move_cursor(file)
            (sandbox / "proj" / "new.txt").write_text("new")
            # Receipts younger than 5s are reused; age them so this is a fresh capture.
            store = ActivityStore()
            for receipt in store.documents("inspection"):
                if receipt["state"] == "DONE":
                    store.save("inspection", receipt["request_id"], {**receipt, "completed_at": 0})
            await app.request("list", workspace["remote_root"], tree.root)
            await settle(app, pilot)
            names = [n.data["path"].rsplit("/", 1)[-1] for n in tree.root.children]
            assert "new.txt" in names
            directory = next(n for n in tree.root.children if n.data["type"] == "dir")
            assert directory.is_expanded
            assert tree.cursor_node.data["path"].endswith("/sub/c.py")
            await _press_to_exit(app, pilot, "ctrl+c")

    asyncio.run(exercise())


def test_offline_refresh_is_quiet(cluster, sandbox):
    cfg = Config(hosts={"local": cluster.host}, default_host="local")
    workspace = service.select(cfg, "local", path=str(sandbox / "proj"), cluster=cluster)

    async def exercise():
        app = WorkspaceApp(workspace, offline=True)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            app.clear_notifications()
            for key in ("r", "2", "r", "3", "r", "4", "r"):
                await pilot.press(key)
            await pilot.pause()
            assert not [n for n in app._notifications if n.severity == "error"]
            await _press_to_exit(app, pilot, "ctrl+c")

    asyncio.run(exercise())


def test_activity_hides_the_views_own_reads_until_toggled(cluster, sandbox):
    from textual.widgets import RichLog

    workspace, settle = _online(cluster, sandbox)

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=(120, 40)) as pilot:
            await settle(app, pilot)
            await settle(app, pilot)
            activity = app.query_one("#activity", RichLog)
            assert "file.inspect" not in _text(activity)
            await pilot.press("4", "v")
            assert "file.inspect.requested" in _text(activity)
            await pilot.press("v")
            assert "file.inspect" not in _text(activity)
            await _press_to_exit(app, pilot, "ctrl+c")

    asyncio.run(exercise())


def _age_receipts():
    """Receipts younger than 5s are reused; age them so the next read is a fresh capture."""
    store = ActivityStore()
    for receipt in store.documents("inspection"):
        if receipt["state"] == "DONE":
            store.save("inspection", receipt["request_id"], {**receipt, "completed_at": 0})


def test_refresh_keeps_a_collapsed_folder_collapsed(cluster, sandbox):
    workspace, settle = _online(cluster, sandbox)

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=(120, 40)) as pilot:
            await settle(app, pilot)
            await pilot.press("f")
            tree = app.query_one("#files", Tree)
            directory = next(n for n in tree.root.children if n.data["type"] == "dir")
            tree.move_cursor(directory)
            await pilot.press("right")
            await settle(app, pilot)
            await pilot.press("left")
            _age_receipts()
            await app.request("list", workspace["remote_root"], tree.root)
            await settle(app, pilot)
            directory = next(n for n in tree.root.children if n.data["type"] == "dir")
            assert directory.data["loaded"] and not directory.is_expanded
            await _press_to_exit(app, pilot, "ctrl+c")

    asyncio.run(exercise())


def test_narrow_preview_is_not_reopened_by_a_late_result(cluster, sandbox):
    workspace, settle = _online(cluster, sandbox)

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=(80, 24)) as pilot:
            await settle(app, pilot)
            await pilot.press("2")
            tree = app.query_one("#files", Tree)
            tree.move_cursor(
                next(n for n in tree.root.children if n.data["path"].endswith("a.txt"))
            )
            await pilot.press("enter")
            await settle(app, pilot)
            await pilot.press("escape")
            _age_receipts()
            await pilot.press("enter")  # the cached capture is shown while a fresh read runs
            await pilot.pause()
            assert app.has_class("previewing")
            await pilot.press("escape", "down")
            await settle(app, pilot)
            assert app.focused is tree and not app.has_class("previewing")
            await _press_to_exit(app, pilot, "ctrl+c")

    asyncio.run(exercise())


def test_parent_navigation_reveals_old_root_despite_other_listings(cluster, sandbox):
    inner = sandbox / "proj" / "sub" / "inner"
    (inner / "x").mkdir(parents=True)
    for name in ("aaa", "bbb", "ccc"):
        (sandbox / "proj" / "sub" / name).mkdir()
    workspace, settle = _online(cluster, sandbox, work=[_job(sandbox, workdir=str(inner))])

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=(120, 40)) as pilot:
            await settle(app, pilot)
            await pilot.press("o")
            await settle(app, pilot)
            tree = app.query_one("#files", Tree)
            tree.move_cursor(tree.root.children[0])
            await pilot.press("right", "backspace")
            await settle(app, pilot, only=lambda r: r["path"].endswith("/x"))
            await settle(app, pilot)
            assert tree.root.data["path"] == str(sandbox / "proj" / "sub")
            assert tree.cursor_node.data["path"] == str(inner)
            await _press_to_exit(app, pilot, "ctrl+c")

    asyncio.run(exercise())


def test_missing_log_reports_once_and_then_in_the_status_line(cluster, sandbox):
    job = _job(sandbox, stdout_path=str(sandbox / "proj" / "missing.out"))
    workspace, settle = _online(cluster, sandbox, work=[job])

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=(120, 40)) as pilot:
            await settle(app, pilot)
            await pilot.press("l")
            for _ in range(3):
                await settle(app, pilot)
                app.log_at = 0
                _age_receipts()
                await settle(app, pilot)
            errors = [n for n in app._notifications if n.severity == "error"]
            assert len(errors) == 1
            assert "missing.out" in str(app.query_one("#log-status").render())
            await _press_to_exit(app, pilot, "ctrl+c")

    asyncio.run(exercise())


def test_repeated_tail_still_times_out(cluster, sandbox, monkeypatch):
    import remoteslurm.tui.app as app_module

    monkeypatch.setattr(app_module, "PENDING_TIMEOUT", 1.0)
    workspace, settle = _online(cluster, sandbox, work=[_job(sandbox)])

    async def exercise():
        app = WorkspaceApp(workspace)
        async with app.run_test(size=(120, 40)) as pilot:
            await settle(app, pilot)
            await pilot.press("l")  # the observer never answers from here on
            for _ in range(8):
                await asyncio.sleep(0.3)
                app.log_at = 0
                await app.poll()
            assert "did not answer" in str(app.query_one("#log-status").render())
            await _press_to_exit(app, pilot, "ctrl+c")

    asyncio.run(exercise())
