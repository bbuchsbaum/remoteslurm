"""A separate local terminal consuming workspace observations."""

from __future__ import annotations

import asyncio
import json
import posixpath
import shlex
import time
from datetime import UTC, datetime
from typing import Any

from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.widgets import DataTable, Footer, Input, RichLog, Static, TabbedContent, TabPane, Tree

from ..activity.reader import event_line, safe_text
from ..errors import RemoteSlurmError
from ..workspace import service


class FileTree(Tree[dict[str, Any]]):
    """Use familiar directory navigation in addition to Textual's Enter/Space controls."""

    BINDINGS = [
        Binding("right", "expand_or_child", show=False),
        Binding("left", "collapse_or_parent", show=False),
    ]

    def action_expand_or_child(self) -> None:
        node = self.cursor_node
        if node is None:
            self.move_cursor(self.root)
        elif node.allow_expand:
            if not node.is_expanded:
                node.expand()
            elif node.children:
                self.move_cursor(node.children[0])

    def action_collapse_or_parent(self) -> None:
        node = self.cursor_node
        if node is not None:
            if node.allow_expand and node.is_expanded:
                node.collapse()
            elif node.parent is not None:
                self.move_cursor(node.parent)


class WorkspaceApp(App[None]):
    TITLE = "remoteslurm"
    CSS = """
    #status { height: 3; padding: 0 1; background: $panel; }
    TabbedContent { height: 1fr; }
    #jobs { height: 1fr; }
    #details { height: 6; padding: 0 1; overflow-y: auto; }
    #file-row { height: 1fr; }
    #files { width: 1fr; min-width: 24; }
    #preview { width: 2fr; border-left: solid $panel; padding: 0 1; }
    #path { height: 3; }
    #notice { height: 2; padding: 0 1; color: $text-muted; }
    .narrow #preview { display: none; }
    .narrow #files { width: 1fr; }
    """
    BINDINGS = [
        Binding("ctrl+c", "quit", show=False, priority=True),
        ("q", "quit", "Close view"),
        ("r", "refresh_files", "Refresh"),
        ("l", "job_log", "Job log"),
        ("slash", "focus_path", "Path"),
        ("f", "focus_files", "Files"),
        Binding("escape", "focus_files", show=False),
        ("question_mark", "help", "Help"),
    ]

    def __init__(self, workspace: dict, *, offline: bool = False):
        super().__init__()
        self.workspace, self.offline = workspace, offline
        self.cursor: str | None = None
        self.pending: dict[str, tuple[Any, str]] = {}
        self.work: list[dict] = []
        self.work_signature = ""
        self.polling = False
        self.log_path: str | None = None
        self.log_at = 0.0
        self.new_events = 0

    def compose(self) -> ComposeResult:
        yield Static("Loading local observations…", id="status", markup=False)
        with TabbedContent(initial="work-tab", id="tabs"):
            with TabPane("Work", id="work-tab"):
                yield DataTable(id="jobs", cursor_type="row")
                yield Static(
                    "Select a job to inspect its recorded paths. L opens its log.",
                    id="details",
                    markup=False,
                )
            with TabPane("Files", id="files-tab"):
                yield Input(
                    value=self.workspace["remote_root"], placeholder="Remote path", id="path"
                )
                with Horizontal(id="file-row"):
                    yield FileTree(Text(safe_text(self.workspace["remote_root"])), id="files")
                    yield RichLog(id="preview", wrap=True, markup=False, max_lines=1000)
            with TabPane("Logs", id="logs-tab"):
                yield RichLog(id="logs", wrap=True, markup=False, max_lines=2000)
            with TabPane("Activity", id="activity-tab"):
                yield RichLog(id="activity", wrap=True, markup=False, max_lines=1000)
        yield Static("", id="notice", markup=False)
        yield Footer()

    async def on_mount(self) -> None:
        table = self.query_one("#jobs", DataTable)
        table.add_columns("Job", "Name", "Execution", "Outputs", "Validation")
        tree = self.query_one("#files", Tree)
        tree.root.data = {"path": self.workspace["remote_root"], "type": "dir"}
        self.set_interval(1, self.poll)
        await self.poll()
        await self.request("list", self.workspace["remote_root"], tree.root)
        self.query_one("#notice", Static).update(
            "Offline cached view"
            if self.offline
            else "Closing this view leaves monitoring and jobs running. Tab: focus; ?: help."
        )

    def on_resize(self, event: Any) -> None:
        self.set_class(event.size.width < 100, "narrow")

    async def poll(self) -> None:
        if self.polling:
            return
        self.polling = True
        try:
            data = await asyncio.to_thread(service.snapshot, self.workspace, after=self.cursor)
            self.cursor = None if data["resync_required"] else data["next_cursor"]
            health = data["monitor"]
            age = health["age_seconds"]
            status = "current" if health["fresh"] else "stale / unobserved"
            age_text = f"{age:.0f}s ago" if age is not None else "no observation yet"
            omitted = f" · {data['work_omitted']} more jobs" if data["work_omitted"] else ""
            self.query_one("#status", Static).update(
                safe_text(
                    f"{self.workspace['host']} / {self.workspace.get('project') or 'workspace'}"
                    f"  |  {status}  |  {age_text}\n{self.workspace['remote_root']}\n"
                    f"{health.get('action') or data['attention']['reason']}{omitted}"
                )
            )
            signature = json.dumps(
                [{k: v for k, v in w.items() if k != "observed_at"} for w in data["work"]],
                sort_keys=True,
            )
            if signature != self.work_signature:
                table = self.query_one("#jobs", DataTable)
                row = table.cursor_row
                selected = self.work[row]["work_id"] if row < len(self.work) else None
                self.work = data["work"]
                table.clear()
                for job in self.work:
                    table.add_row(
                        *[
                            Text(safe_text(job.get(key) or "—"))
                            for key in ("job_id", "name", "execution", "artifacts", "validation")
                        ]
                    )
                if self.work:
                    row = next(
                        (i for i, w in enumerate(self.work) if w["work_id"] == selected),
                        min(row, len(self.work) - 1),
                    )
                    table.move_cursor(row=row)
                else:
                    self.query_one("#details", Static).update(
                        "Work snapshot exceeds the response budget. Use `rslurm jobs` for details."
                        if data.get("snapshot_omitted")
                        else "No recorded jobs under this root. "
                        "Submit through remoteslurm with this cwd."
                    )
                self.work_signature = signature
            activity = self.query_one("#activity", RichLog)
            if data["history_gap"]:
                activity.write(Text("History gap — reloading retained observations."))
            at_end = activity.is_vertical_scroll_end
            for event in data["events"]:
                activity.write(Text(event_line(event)), scroll_end=at_end)
            if at_end:
                self.new_events = 0
            else:
                self.new_events += len(data["events"])
            self.query_one("#tabs", TabbedContent).get_tab("activity-tab").label = (
                f"Activity ({self.new_events} new)" if self.new_events else "Activity"
            )
            for rid, (node, kind) in list(self.pending.items()):
                receipt = await asyncio.to_thread(
                    service.inspect_request, self.workspace, kind, request_id=rid
                )
                if receipt["state"] == "DONE":
                    self.display_file(receipt["result"], node, kind)
                    del self.pending[rid]
                elif receipt["state"] == "ERROR":
                    if node is not None and node.data:
                        node.data["loading"] = False
                    self.notify(
                        safe_text(receipt.get("action") or receipt["error"]), severity="error"
                    )
                    del self.pending[rid]
            if (
                self.log_path
                and not self.offline
                and time.monotonic() - self.log_at >= 5
                and (self.query_one("#tabs", TabbedContent).active == "logs-tab")
            ):
                self.log_at = time.monotonic()
                await self.request("tail", self.log_path)
        except (RemoteSlurmError, OSError) as exc:
            self.query_one("#status", Static).update(
                "Local observation unavailable: " + safe_text(exc)
            )
        finally:
            self.polling = False

    async def request(self, kind: str, path: str, node: Any = None) -> bool:
        try:
            result = await asyncio.to_thread(
                service.inspect_request, self.workspace, kind, path, cached_only=self.offline
            )
            if result["state"] == "DONE":
                self.display_file(result["result"], node, kind)
            else:
                self.pending[result["request_id"]] = (node, kind)
                self.query_one("#notice", Static).update("Inspecting " + safe_text(path))
            return True
        except RemoteSlurmError as exc:
            if node is not None and node.data:
                node.data["loading"] = False
            self.notify(safe_text(exc), severity="error")
            return False

    def display_file(self, result: dict, node: Any, kind: str) -> None:
        if kind == "refresh":
            self.query_one("#notice", Static).update("Scheduler refresh requested.")
            return
        if kind == "list" and node is not None:
            node.data["path"] = result["path"]
            if node is self.query_one("#files", Tree).root:
                node.set_label(Text(safe_text(result["path"])))
            node.remove_children()
            for entry in result["entries"]:
                path = posixpath.join(result["path"], entry["name"])
                label = entry["name"] + ("/" if entry["type"] == "dir" else "")
                if entry.get("type") == "link":
                    label += " -> " + str(entry.get("target", "unknown"))
                elif entry.get("type") == "file":
                    label += f"  {entry.get('size', '?')} B"
                node.add(
                    Text(safe_text(label)),
                    {**entry, "path": path},
                    allow_expand=entry["type"] == "dir",
                )
            if result.get("next_offset") is not None:
                node.add_leaf(Text("Load more captured entries…"), {"type": "page", **result})
            node.expand()
            node.data["loaded"] = True
            node.data["loading"] = False
            self.query_one("#notice", Static).update(
                f"{result['path']} | {result['captured_count']} entries captured"
                + (" | partial scan; unlisted files may exist" if result.get("partial") else "")
            )
        else:
            text = result.get("content") or result.get("note") or json.dumps(result, indent=2)
            preview = self.query_one(
                "#logs" if kind == "tail" or self.size.width < 100 else "#preview", RichLog
            )
            scroll_y = preview.scroll_y
            was_at_end = preview.is_vertical_scroll_end
            preview.clear()
            modified = (
                datetime.fromtimestamp(result["mtime"], UTC).strftime("%Y-%m-%d %H:%M UTC")
                if result.get("mtime") is not None
                else "unknown"
            )
            details = (
                f"{result.get('path', '')}\n{result.get('size', 0)} bytes · modified {modified}\n"
            )
            preview.write(Text(safe_text(details) + safe_text(text)))
            if not was_at_end:
                preview.scroll_to(y=scroll_y, animate=False)
            self.query_one("#notice", Static).update(
                safe_text(
                    result.get("gap")
                    or f"Captured {time.time() - result['observed_at']:.0f}s ago · "
                    f"{result.get('size', 0)} bytes · bounded preview"
                )
            )
            if kind != "tail" and self.size.width < 100:
                self.query_one("#tabs", TabbedContent).active = "logs-tab"

    async def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:
        data = event.node.data
        if (
            data
            and data.get("type") == "dir"
            and not data.get("loaded")
            and not data.get("loading")
        ):
            data["loading"] = True
            await self.request("list", data["path"], event.node)

    async def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        data = event.node.data
        if not data:
            return
        if data["type"] == "page":
            try:
                page = await asyncio.to_thread(
                    service.inspect_request,
                    self.workspace,
                    "list",
                    data["path"],
                    offset=data["next_offset"],
                    token=data["token"],
                )
                self.display_file(page, event.node.parent, "list")
            except RemoteSlurmError as exc:
                self.notify(safe_text(exc), severity="error")
        elif data["type"] != "dir":
            await self.request("preview", data["path"])

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        tree = self.query_one("#files", Tree)
        if await self.request("list", event.value, tree.root):
            tree.focus()

    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        if event.pane.id == "files-tab" and self.focused is not self.query_one("#path", Input):
            self.call_after_refresh(self.query_one("#files", Tree).focus)

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.cursor_row < len(self.work):
            job = self.work[event.cursor_row]
            lines = [
                f"{key}: {job.get(key) or 'unavailable'}"
                for key in ("workdir", "script_path", "stdout_path", "stderr_path")
            ]
            lines.append("Identity: " + job.get("identity_basis", "local registration"))
            if job.get("stdout_path"):
                lines.append(
                    "Retrieve: "
                    + shlex.join(
                        [
                            "rslurm",
                            "get",
                            f"{self.workspace['host']}:{job['stdout_path']}",
                            ".",
                        ]
                    )
                )
            self.query_one("#details", Static).update(safe_text("\n".join(lines)))

    async def action_job_log(self) -> None:
        index = self.query_one("#jobs", DataTable).cursor_row
        if index < len(self.work) and self.work[index].get("stdout_path"):
            self.log_path = self.work[index]["stdout_path"]
            self.query_one("#tabs", TabbedContent).active = "logs-tab"
            await self.request("tail", self.log_path)
        else:
            self.notify("No recorded log path; inspect the Files tab.")

    async def action_refresh_files(self) -> None:
        tree = self.query_one("#files", Tree)
        node = (tree.cursor_node if self.focused is tree else None) or tree.root
        if node.data and node.data.get("type") == "dir":
            await self.request("list", node.data["path"], node)
        elif node.data and node.data.get("type") != "page":
            await self.request("preview", node.data["path"])
        await self.request("refresh", self.workspace["remote_root"])

    def action_focus_path(self) -> None:
        self.query_one("#tabs", TabbedContent).active = "files-tab"
        self.query_one("#path", Input).focus()

    def action_focus_files(self) -> None:
        self.query_one("#tabs", TabbedContent).active = "files-tab"
        self.query_one("#files", Tree).focus()

    def action_help(self) -> None:
        self.notify(
            "F: files · arrows: navigate · Enter: open · /: path · Esc: return to files. "
            "L: job log · R: refresh selection. "
            "Files are captured on demand. Q closes only this viewer.",
            timeout=10,
        )
