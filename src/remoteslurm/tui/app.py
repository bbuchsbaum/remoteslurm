"""A separate local terminal consuming workspace observations."""

from __future__ import annotations

import asyncio
import json
import posixpath
import shlex
import time
from collections import deque
from datetime import UTC, datetime
from typing import Any

from rich.rule import Rule
from rich.style import Style
from rich.syntax import Syntax
from rich.text import Span, Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, VerticalScroll
from textual.css.query import NoMatches
from textual.screen import ModalScreen
from textual.widgets import DataTable, Footer, Input, RichLog, Static, TabbedContent, TabPane, Tree
from textual.widgets.tree import TreeNode, UnknownNodeID

from ..activity.reader import safe_text
from ..errors import RemoteSlurmError
from ..workspace import service

TABS = ("work-tab", "files-tab", "logs-tab", "activity-tab")
NARROW = 100
# The observer fails requests older than 30s itself; this only covers an absent observer.
PENDING_TIMEOUT = 45.0
NO_OBSERVER = "the local observer did not answer; check `rslurm monitor status`"
# Events the view causes by reading files; hidden unless asked for.
VIEW_EVENTS = {
    "file.inspect.requested",
    "file.inspect.returned",
    "file.inspect.failed",
    "files.observed",
}

GOOD = {"COMPLETED", "PASSED", "PRESENT", "HEALTHY", "FRESH", "DONE"}
BAD = {
    "FAILED",
    "MISSING",
    "TIMEOUT",
    "CANCELLED",
    "OUT_OF_MEMORY",
    "NODE_FAIL",
    "BOOT_FAIL",
    "DEADLINE",
    "PREEMPTED",
    "ERROR",
    "CONFLICT",
    "INVALID",
    "UNAVAILABLE",
}
ACTIVE = {"RUNNING", "COMPLETING", "CONFIGURING", "STAGE_OUT", "SIGNALING"}
WAITING = {"PENDING", "REQUEUED", "SUSPENDED", "RESIZING", "QUEUED", "STALE", "PARTIAL"}

HELP = """\
[b]Tabs[/b]
  1-4            Work · Files · Logs · Activity
  Esc            back to the current tab's main list
  Tab            move focus within a tab

[b]Work[/b]
  ↑ ↓            select a job
  Enter, l       open its stdout log
  e              open its stderr log
  o              browse its working directory

[b]Files[/b]
  ↑ ↓            move
  → / ←          expand · enter folder / collapse · go to parent
  Enter          open file or toggle folder
  Backspace      show the parent of the current root
  /              type a path (relative to the workspace root)
  r              re-read the selected file or folder

[b]Activity[/b]
  v              show or hide the view's own file reads

Files and logs are captured on demand through the local observer.
q closes only this viewer; monitoring and jobs keep running.

[dim]Esc, ? or q closes this help.[/dim]"""


def state_text(value: Any) -> Text:
    text = safe_text(value or "—")
    word = text.split()[0].upper() if text.strip() else ""
    if word in GOOD:
        return Text(text, style="green")
    if word in BAD:
        return Text(text, style="bold red")
    if word in ACTIVE:
        return Text(text, style="yellow")
    if word in WAITING:
        return Text(text, style="cyan")
    return Text(text, style="dim")


def human_size(value: Any) -> str:
    if not isinstance(value, int | float):
        return "?"
    size = float(value)
    for unit in ("B", "K", "M", "G", "T"):
        if size < 1024 or unit == "T":
            return f"{size:.0f}{unit}" if unit == "B" else f"{size:.1f}{unit}"
        size /= 1024
    return "?"


def highlight(code: str, lexer: str) -> Text:
    """Syntax colours only: the theme's backgrounds and underlines clash with the app theme."""
    text = Syntax(code, lexer, theme="ansi_dark").highlight(code)
    text.style = ""
    text.spans = [
        Span(span.start, span.end, Style(color=style.color, bold=style.bold, italic=style.italic))
        for span in text.spans
        for style in [Style.parse(span.style) if isinstance(span.style, str) else span.style]
    ]
    text.rstrip()
    return text


def entry_label(entry: dict) -> Text:
    name, kind = safe_text(entry["name"]), entry.get("type")
    label = Text()
    if kind == "dir":
        label.append(name + "/", style="bold blue")
    elif kind == "link":
        label.append(name, style="cyan")
        label.append(" → " + safe_text(entry.get("target", "?")), style="dim cyan")
    else:
        label.append(name, style="dim" if name.startswith(".") else "")
        label.append(f"  {human_size(entry.get('size'))}", style="dim")
    if kind == "dir" and name.startswith("."):
        label.stylize("dim")
    return label


def node_data(node: TreeNode) -> dict[str, Any]:
    if node.data is None:
        node.data = {}
    return node.data


class FileTree(Tree[dict[str, Any]]):
    """Use familiar directory navigation in addition to Textual's Enter/Space controls."""

    BINDINGS = [
        Binding("right", "expand_or_child", show=False),
        Binding("left", "collapse_or_parent", show=False),
        Binding("backspace", "app.parent_dir", "Up", key_display="⌫"),
    ]

    def action_expand_or_child(self) -> None:
        node = self.cursor_node
        if node is None:
            if self.root.children:
                self.move_cursor(self.root.children[0])
        elif node.allow_expand:
            if not node.is_expanded:
                node.expand()
            elif node.children:
                self.move_cursor(node.children[0])

    def action_collapse_or_parent(self) -> None:
        node = self.cursor_node
        if node is None:
            return
        if node.allow_expand and node.is_expanded:
            node.collapse()
        elif node.parent is not None and (node.parent is not self.root or self.show_root):
            self.move_cursor(node.parent)


class HelpScreen(ModalScreen[None]):
    BINDINGS = [Binding("escape,question_mark,q", "dismiss", "Close")]
    CSS = """
    HelpScreen { align: center middle; }
    #help {
        width: 76; max-width: 100%; height: auto; max-height: 100%;
        padding: 0 2; border: round $primary; background: $surface;
    }
    """

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="help"):
            yield Static(HELP)


class WorkspaceApp(App[None]):
    TITLE = "remoteslurm"
    CSS = """
    #status {
        height: 2; padding: 0 1; background: $panel;
        text-wrap: nowrap; text-overflow: ellipsis;
    }
    TabbedContent { height: 1fr; }
    TabPane { padding: 0; }
    #jobs { height: 1fr; }
    #details { height: auto; max-height: 9; padding: 0 1; border-top: solid $panel; }
    #path { height: 3; }
    #file-row { height: 1fr; }
    #files { width: 1fr; min-width: 28; }
    #preview { width: 2fr; border-left: solid $panel; padding: 0 1; }
    #logs, #activity { height: 1fr; padding: 0 1; }
    .status-line {
        height: 1; padding: 0 1; color: $text-muted;
        text-wrap: nowrap; text-overflow: ellipsis;
    }
    .narrow #preview { display: none; width: 1fr; border-left: none; }
    .narrow.previewing #preview { display: block; }
    .narrow.previewing #files { display: none; }
    """
    BINDINGS = [
        Binding("ctrl+c", "quit", show=False, priority=True),
        Binding("q", "quit", "Close view"),
        Binding("1", "show_tab('work-tab')", "Tabs", key_display="1-4"),
        Binding("2", "show_tab('files-tab')", show=False),
        Binding("3", "show_tab('logs-tab')", show=False),
        Binding("4", "show_tab('activity-tab')", show=False),
        Binding("f", "focus_files", show=False),
        Binding("l", "job_log('stdout')", "Stdout"),
        Binding("e", "job_log('stderr')", "Stderr"),
        Binding("o", "open_workdir", "Workdir"),
        Binding("slash", "focus_path", "Path"),
        Binding("r", "refresh", "Refresh"),
        Binding("v", "toggle_view_events", "View reads"),
        Binding("escape", "escape", show=False),
        Binding("question_mark", "help", "Help"),
    ]

    def __init__(self, workspace: dict, *, offline: bool = False):
        super().__init__()
        self.workspace, self.offline = workspace, offline
        self.root_path: str = workspace["remote_root"]
        self.cursor: str | None = None
        # request_id -> what to do with the receipt once the observer answers.
        self.pending: dict[str, dict[str, Any]] = {}
        # Only the newest request per display slot may update the screen.
        self.generation: dict[str, int] = {}
        self.work: list[dict] = []
        self.work_signature = ""
        self.poll_lock = asyncio.Lock()
        self.log_job: dict | None = None
        self.log_stream = "stdout"
        self.log_at = 0.0
        self.shown: dict[str, str | None] = {"#preview": None, "#logs": None}
        self.events: deque[dict] = deque(maxlen=1000)
        self.show_view_events = False
        self.activity_empty = True
        self.new_events = 0
        self.seen: set[str] = set()

    # -- layout -------------------------------------------------------------------------

    def compose(self) -> ComposeResult:
        yield Static("Loading local observations…", id="status")
        with TabbedContent(initial="work-tab", id="tabs"):
            with TabPane("Work", id="work-tab"):
                yield DataTable(id="jobs", cursor_type="row")
                yield Static(
                    Text(
                        "Select a job to see its recorded paths. Enter opens its log.\n"
                        "Closing this view leaves monitoring and jobs running.",
                        style="dim",
                    ),
                    id="details",
                )
            with TabPane("Files", id="files-tab"):
                yield Input(value=self.root_path, placeholder="Remote path", id="path")
                with Horizontal(id="file-row"):
                    tree = FileTree(Text(safe_text(self.root_path)), id="files")
                    tree.show_root = False
                    yield tree
                    yield RichLog(
                        id="preview",
                        wrap=True,
                        markup=False,
                        max_lines=1000,
                        min_width=20,
                        auto_scroll=False,
                    )
                yield Static("", id="file-status", classes="status-line")
            with TabPane("Logs", id="logs-tab"):
                yield RichLog(id="logs", wrap=True, markup=False, max_lines=2000, min_width=20)
                yield Static("", id="log-status", classes="status-line")
            with TabPane("Activity", id="activity-tab"):
                yield RichLog(id="activity", wrap=True, markup=False, max_lines=1000, min_width=20)
                yield Static(
                    "v shows the view's own file reads", id="activity-status", classes="status-line"
                )
        yield Footer()

    async def on_mount(self) -> None:
        table = self.query_one("#jobs", DataTable)
        table.add_columns("Job", "Name", "Execution", "Outputs", "Validation")
        tree = self.query_one("#files", FileTree)
        tree.root.data = {"path": self.root_path, "type": "dir"}
        self.query_one("#preview", RichLog).write(Text("Select a file to preview it.", style="dim"))
        self.query_one("#logs", RichLog).write(
            Text("No log open. On the Work tab, press l (stdout) or e (stderr).", style="dim")
        )
        self.query_one("#activity", RichLog).write(Text("No activity yet.", style="dim"))
        self.set_interval(1, self.tick)
        await self.poll()
        await self.request("list", self.root_path, tree.root)
        self.call_after_refresh(table.focus)

    def on_resize(self, event: Any) -> None:
        self.set_class(event.size.width < NARROW, "narrow")

    @property
    def narrow(self) -> bool:
        return self.size.width < NARROW

    @property
    def active_tab(self) -> str:
        return self.query_one("#tabs", TabbedContent).active

    def main_widget(self, tab: str) -> Any:
        if tab == "files-tab":
            return self.query_one(
                "#preview" if self.narrow and self.has_class("previewing") else "#files"
            )
        return self.query_one({"work-tab": "#jobs", "logs-tab": "#logs"}.get(tab, "#activity"))

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        try:
            tab = self.active_tab
        except NoMatches:
            return True
        if action == "job_log":
            return tab in ("work-tab", "logs-tab")
        if action == "open_workdir":
            return tab == "work-tab"
        if action == "toggle_view_events":
            return tab == "activity-tab"
        return True

    # -- polling ------------------------------------------------------------------------

    async def tick(self) -> None:
        # The timer skips a beat while a poll runs; explicit polls wait their turn instead.
        if not self.poll_lock.locked():
            await self.poll()

    async def poll(self) -> None:
        async with self.poll_lock:
            try:
                data = await asyncio.to_thread(service.snapshot, self.workspace, after=self.cursor)
            except (RemoteSlurmError, OSError) as exc:
                # A cursor the journal no longer accepts would fail every poll; resync instead.
                self.cursor = None
                self.query_one("#status", Static).update(
                    Text.assemble(
                        ("● ", "red"),
                        ("local observation unavailable", "bold red"),
                        "\n",
                        (safe_text(exc), "dim"),
                    )
                )
            else:
                self.cursor = None if data["resync_required"] else data["next_cursor"]
                self.show_status(data)
                self.show_work(data)
                self.show_activity(data)
            await self.check_pending()
            if (
                self.log_job
                and not self.offline
                and self.active_tab == "logs-tab"
                and time.monotonic() - self.log_at >= 5
            ):
                await self.tail_log(quiet=True)

    def show_status(self, data: dict) -> None:
        health = data["monitor"]
        age = health["age_seconds"]
        heartbeat = health.get("heartbeat")
        if self.offline:
            dot, style, state = "○", "yellow", "offline cached view"
        elif health["fresh"]:
            dot, style, state = "●", "green", "live"
        else:
            dot, style = "●", "red"
            if not health.get("enabled"):
                state = "stale: workspace monitoring is off"
            elif heartbeat is None or time.time() - heartbeat > 90:
                state = "stale: observer not running"
            elif age is None:
                state = "stale: no observation yet"
            elif age > 90:
                state = "stale: last observation is old"
            else:
                state = f"stale: observer reports {health.get('state') or 'unknown'}"
        age_text = f"observed {age:.0f}s ago" if age is not None else "never observed"
        omitted = f"  ·  {data['work_omitted']} more jobs" if data["work_omitted"] else ""
        project = self.workspace.get("project") or "workspace"
        reason = health.get("action") or data["attention"].get("reason") or ""
        self.query_one("#status", Static).update(
            Text.assemble(
                (f"{dot} ", style),
                (state, f"bold {style}"),
                "   ",
                (safe_text(f"{self.workspace['host']} / {project}"), "bold"),
                (f"  ·  {age_text}{omitted}  ·  ", "dim"),
                (safe_text(self.workspace["remote_root"]), "dim"),
                "\n",
                (safe_text(reason), "yellow" if health.get("action") else "dim"),
            )
        )

    def show_work(self, data: dict) -> None:
        signature = json.dumps(
            [{k: v for k, v in w.items() if k != "observed_at"} for w in data["work"]],
            sort_keys=True,
        )
        if signature == self.work_signature:
            return
        table = self.query_one("#jobs", DataTable)
        row = table.cursor_row
        selected = self.work[row]["work_id"] if row < len(self.work) else None
        self.work = data["work"]
        table.clear()
        for job in self.work:
            table.add_row(
                Text(safe_text(job.get("job_id") or "—"), style="bold"),
                Text(safe_text(job.get("name") or "—")),
                *(state_text(job.get(key)) for key in ("execution", "artifacts", "validation")),
            )
        if self.work:
            row = next(
                (i for i, w in enumerate(self.work) if w["work_id"] == selected),
                min(row, len(self.work) - 1),
            )
            table.move_cursor(row=row)
            self.show_details(row)
        else:
            self.query_one("#details", Static).update(
                Text(
                    "Work snapshot exceeds the response budget. Use `rslurm jobs` for details."
                    if data.get("snapshot_omitted")
                    else "No recorded jobs under this root. Submit through remoteslurm with "
                    "this cwd.",
                    style="dim",
                )
            )
        self.work_signature = signature

    def show_details(self, row: int) -> None:
        if row >= len(self.work):
            return
        job = self.work[row]
        text = Text()
        for label, key in (
            ("Workdir", "workdir"),
            ("Script", "script_path"),
            ("Stdout", "stdout_path"),
            ("Stderr", "stderr_path"),
        ):
            text.append(f"{label:<10}", style="bold")
            value = job.get(key)
            text.append(safe_text(value) if value else "unavailable", style="" if value else "dim")
            text.append("\n")
        text.append(f"{'Identity':<10}", style="bold")
        text.append(safe_text(job.get("identity_basis", "local registration")) + "\n")
        if job.get("stdout_path"):
            text.append(f"{'Retrieve':<10}", style="bold")
            command = ["rslurm", "get", f"{self.workspace['host']}:{job['stdout_path']}", "."]
            text.append(safe_text(shlex.join(command)) + "\n", style="cyan")
        text.append("Enter/l stdout · e stderr · o browse workdir", style="dim")
        self.query_one("#details", Static).update(text)

    def show_activity(self, data: dict) -> None:
        activity = self.query_one("#activity", RichLog)
        at_end = activity.is_vertical_scroll_end
        if data["history_gap"]:
            self.write_activity(Text("History gap — reloading retained observations.", "yellow"))
        added = 0
        for event in data["events"]:
            # A resync re-reads retained events; show each one once.
            event_id = event.get("event_id")
            if event_id is not None:
                if event_id in self.seen:
                    continue
                self.seen.add(event_id)
            self.events.append(event)
            if self.show_view_events or event.get("kind") not in VIEW_EVENTS:
                self.write_activity(self.activity_line(event), scroll_end=at_end)
                added += 1
        self.new_events = 0 if at_end else self.new_events + added
        label = f"Activity ({self.new_events} new)" if self.new_events else "Activity"
        tab = self.query_one("#tabs", TabbedContent).get_tab("activity-tab")
        if str(tab.label) != label:
            tab.label = label

    def write_activity(self, line: Text, scroll_end: bool | None = None) -> None:
        activity = self.query_one("#activity", RichLog)
        if self.activity_empty:
            activity.clear()
            self.activity_empty = False
        activity.write(line, scroll_end=scroll_end)

    def activity_line(self, event: dict) -> Text:
        subject, payload = event.get("subject", {}), event.get("payload", {})
        try:
            stamp = datetime.fromisoformat(event["recorded_at"]).astimezone()
            today = stamp.date() == datetime.now().astimezone().date()
            when = stamp.strftime("%H:%M:%S" if today else "%m-%d %H:%M")
        except (KeyError, TypeError, ValueError):
            when = safe_text(event.get("recorded_at", "?"))[:19]
        kind = safe_text(event.get("kind", "?"))
        failed = any(word in kind for word in ("fail", "error", "lost"))
        line = Text.assemble((f"{when}  ", "dim"), (kind, "bold red" if failed else "bold"))
        for value in (subject.get("job_id"), payload.get("name")):
            if value is not None:
                line.append("  " + safe_text(value))
        if payload.get("state") is not None:
            line.append("  ")
            line.append_text(state_text(payload["state"]))
        if payload.get("path") is not None:
            line.append("  " + self.relative(str(payload["path"])), style="dim")
        if payload.get("summary") is not None:
            line.append("  " + safe_text(payload["summary"]).replace("\n", " "))
        return line

    def relative(self, path: str) -> str:
        root = self.workspace["remote_root"].rstrip("/")
        if path == root:
            return "."
        if path.startswith(root + "/"):
            return safe_text(path[len(root) + 1 :])
        return safe_text(path)

    async def check_pending(self) -> None:
        for rid, snapshot in list(self.pending.items()):
            # One unreadable receipt must not stop the others or the next poll.
            try:
                receipt = await asyncio.to_thread(
                    service.inspect_request, self.workspace, snapshot["kind"], request_id=rid
                )
            except (RemoteSlurmError, OSError) as exc:
                receipt = {"state": "ERROR", "error": str(exc)}
            # A key handler may have re-registered this receipt while we awaited.
            pending = self.pending.get(rid)
            if pending is None:
                continue
            if receipt["state"] not in ("DONE", "ERROR"):
                if time.time() - pending["created"] < PENDING_TIMEOUT:
                    continue
                receipt = {"state": "ERROR", "action": NO_OBSERVER}
            del self.pending[rid]
            if receipt["state"] == "DONE":
                self.apply(receipt["result"], pending)
            else:
                code = str(receipt.get("error") or "failed")
                message = receipt.get("action") or code.replace("_", " ")
                self.fail(pending, message, code=code)

    # -- inspection requests ------------------------------------------------------------

    def bump(self, slot: str) -> int:
        self.generation[slot] = self.generation.get(slot, 0) + 1
        return self.generation[slot]

    def current(self, pending: dict) -> bool:
        if self.generation.get(pending["slot"]) != pending["gen"]:
            return False
        node = pending["node"]
        if node is None:
            return True
        try:
            return self.query_one("#files", FileTree).get_node_by_id(node.id) is node
        except UnknownNodeID:
            return False

    async def request(
        self,
        kind: str,
        path: str,
        node: TreeNode | None = None,
        *,
        quiet: bool = False,
        reveal: str | None = None,
    ) -> bool:
        slot = f"list:{node.id}" if kind == "list" and node is not None else kind
        pending: dict[str, Any] = {"node": node, "kind": kind, "slot": slot, "reveal": reveal}
        pending.update(gen=self.bump(slot), created=time.time(), path=path, quiet=quiet)
        try:
            result = await asyncio.to_thread(
                service.inspect_request, self.workspace, kind, path, cached_only=self.offline
            )
        except (RemoteSlurmError, OSError) as exc:
            self.fail(pending, str(exc))
            return False
        if result["state"] == "DONE":
            self.apply(result["result"], pending)
        else:
            # The service hands back the same queued receipt for repeated reads of a path, so
            # time it from when it was queued, not from this (possibly repeated) request.
            pending["created"] = result.get("created_at", pending["created"])
            if time.time() - pending["created"] >= PENDING_TIMEOUT:
                self.fail(pending, NO_OBSERVER)
                return False
            self.pending[result["request_id"]] = pending
            if result.get("cached") and kind != "refresh":
                self.apply(result["cached"], pending, provisional=True)
            elif kind != "refresh":
                self.status_for(kind).update(Text("Inspecting " + safe_text(path) + " …"))
        return True

    def status_for(self, kind: str) -> Static:
        return self.query_one("#log-status" if kind == "tail" else "#file-status", Static)

    def fail(self, pending: dict, message: str, *, code: str = "") -> None:
        node = pending["node"]
        if node is not None and node.data:
            node.data["loading"] = False
        if not self.current(pending):
            return
        link = node is not None and node.data and node.data.get("type") == "link"
        if link and code == "invalid_arg":
            # Not a directory after all: show the link target as a file instead.
            node.allow_expand = False
            node.collapse()
            self.call_later(self.request, "preview", node.data["path"])
            return
        text = f"{self.relative(pending['path'])}: {safe_text(message)}"
        if not pending["quiet"]:
            self.notify(text, severity="error")
        self.status_for(pending["kind"]).update(Text(text, style="yellow"))

    def apply(self, result: dict, pending: dict, *, provisional: bool = False) -> None:
        if not self.current(pending):
            return
        kind = pending["kind"]
        if kind == "refresh":
            self.notify("Scheduler refresh requested.", timeout=3)
        elif kind == "list":
            self.show_listing(
                pending["node"], result, provisional=provisional, reveal=pending["reveal"]
            )
        elif kind == "tail":
            self.show_text("#logs", result, provisional=provisional)
        else:
            self.show_text("#preview", result, provisional=provisional)
            # Open the narrow preview once per request: a later refresh of the same read must
            # not reopen it after the user has gone back to the list.
            if self.narrow and not pending.get("opened"):
                pending["opened"] = True
                self.add_class("previewing")
                if self.active_tab == "files-tab" and self.focused in (
                    None,
                    self.query_one("#files"),
                ):
                    self.query_one("#preview", RichLog).focus()

    def show_listing(
        self,
        node: TreeNode,
        result: dict,
        *,
        provisional: bool = False,
        append: bool = False,
        reveal: str | None = None,
    ) -> None:
        data = node_data(node)
        entries = result.get("entries", [])
        if append:
            entries = data.get("entries", []) + entries
        next_page = (
            {"offset": result["next_offset"], "token": result.get("token")}
            if result.get("next_offset") is not None
            else None
        )
        data.update(
            path=result["path"],
            entries=entries,
            next=next_page,
            captured_count=result.get("captured_count", len(entries)),
            partial=result.get("partial", False),
            observed_at=result.get("observed_at"),
            loaded=True,
            loading=provisional,
        )
        tree = self.query_one("#files", FileTree)
        if node is tree.root:
            self.root_path = result["path"]
            node.set_label(Text(safe_text(result["path"])))
            path_input = self.query_one("#path", Input)
            if self.focused is not path_input:
                path_input.value = result["path"]
        # Respect a folder the user collapsed while its listing was on the way.
        self.rebuild(node, expand=node is tree.root or node.is_expanded, reveal=reveal)
        shown = len(entries)
        where = self.relative(result["path"])
        line = Text(where if where.endswith("/") else where + "/", style="bold")
        count = f"  ·  {shown} entries"
        if result.get("captured_count", shown) > shown:
            count = f"  ·  {shown} of {result['captured_count']} entries"
        line.append(count)
        if result.get("partial"):
            line.append("  ·  partial scan; unlisted files may exist", style="yellow")
        observed = result.get("observed_at")
        if observed is not None:
            line.append(f"  ·  captured {max(0, time.time() - observed):.0f}s ago", style="dim")
        if provisional:
            line.append("  ·  cached; refreshing…", style="yellow")
        self.status_for("list").update(line)

    def rebuild(self, node: TreeNode, *, expand: bool = True, reveal: str | None = None) -> None:
        """Rebuild a directory's children, keeping open subfolders and the cursor in place."""
        tree = self.query_one("#files", FileTree)
        cursor = tree.cursor_node
        cursor_path = cursor.data.get("path") if cursor and cursor.data else None
        if cursor is not None and cursor.data and cursor.data.get("type") == "page":
            cursor_path = None
        if reveal:
            cursor_path = reveal
        previous = {
            child.data["path"]: (child.data, child.is_expanded)
            for child in node.children
            if child.data and child.data.get("type") != "page"
        }
        node.remove_children()
        data = node_data(node)
        entries = sorted(
            data.get("entries", []),
            key=lambda e: (e.get("type") != "dir", str(e["name"]).lower()),
        )
        for entry in entries:
            path = posixpath.join(data["path"], entry["name"])
            old, expanded = previous.get(path, (None, False))
            keep = old is not None and old.get("type") == entry.get("type") and old.get("loaded")
            child_data = {**old, **entry, "path": path} if keep and old else {**entry, "path": path}
            child = node.add(
                entry_label(entry),
                child_data,
                allow_expand=entry.get("type") in ("dir", "link"),
            )
            if keep and old:
                self.rebuild(child, expand=expanded)
        if data.get("next"):
            remaining = data["captured_count"] - len(data.get("entries", []))
            node.add_leaf(
                Text(f"Load {min(remaining, 200)} more of {remaining} remaining…", "italic dim"),
                {"type": "page", "path": data["path"]},
            )
        if expand:
            node.expand()
        if cursor_path is not None:
            self.restore_cursor(cursor_path, cursor)

    def restore_cursor(self, path: str, fallback: TreeNode | None) -> None:
        tree = self.query_one("#files", FileTree)
        # Tree keeps its cursor as a line number and renumbers lazily; renumber now so the
        # cursor follows the node instead of whatever lands on its old line.
        tree._tree_lines  # noqa: B018
        stack = list(tree.root.children)
        while stack:
            candidate = stack.pop()
            if candidate.data and candidate.data.get("path") == path:
                if candidate.data.get("type") != "page" and candidate.line >= 0:
                    tree.move_cursor(candidate)
                    return
            stack.extend(candidate.children)
        if fallback is not None and fallback is tree.cursor_node:
            return
        try:
            if fallback is not None and tree.get_node_by_id(fallback.id) is fallback:
                tree.move_cursor(fallback)
        except UnknownNodeID:
            pass

    def show_text(self, target: str, result: dict, *, provisional: bool = False) -> None:
        log = self.query_one(target, RichLog)
        path = result.get("path", "")
        same = self.shown[target] == path
        scroll_y, was_at_end = log.scroll_y, log.is_vertical_scroll_end
        log.clear()
        modified = (
            datetime.fromtimestamp(result["mtime"], UTC).strftime("%Y-%m-%d %H:%M UTC")
            if result.get("mtime") is not None
            else "unknown"
        )
        log.write(Text(self.relative(path), style="bold"), scroll_end=False)
        log.write(
            Text(f"{human_size(result.get('size'))} · modified {modified}", style="dim"),
            scroll_end=False,
        )
        log.write(Rule(style="dim"), scroll_end=False, expand=True)
        content = result.get("content")
        if content is None:
            note = result.get("note") or json.dumps(result, indent=2, default=str)
            log.write(Text(safe_text(note), style="italic dim"), scroll_end=False)
        else:
            lexer = Syntax.guess_lexer(path, "") if target == "#preview" else "text"
            if lexer in ("default", "text", "text only"):
                log.write(Text(safe_text(content)), scroll_end=False)
            else:
                log.write(highlight(safe_text(content), lexer), scroll_end=False)
        if target == "#logs" and (not same or was_at_end):
            log.scroll_end(animate=False)
        elif same:
            log.scroll_to(y=scroll_y, animate=False)
        else:
            log.scroll_home(animate=False)
        self.shown[target] = path
        observed = result.get("observed_at")
        captured = f"captured {max(0, time.time() - observed):.0f}s ago" if observed else "cached"
        line = Text()
        if target == "#logs" and self.log_job:
            job = self.log_job
            line.append(f"{self.log_stream} of job {safe_text(job.get('job_id') or '?')}  ·  ")
        line.append(captured)
        if target == "#preview" and self.narrow:
            line.append("  ·  Esc returns to the list", style="bold")
        if result.get("gap"):
            line.append("  ·  " + safe_text(result["gap"]), style="yellow")
        elif result.get("truncated"):
            line.append("  ·  bounded preview; later bytes omitted", style="yellow")
        if provisional:
            line.append("  ·  cached; refreshing…", style="yellow")
        self.status_for("tail" if target == "#logs" else "preview").update(line)

    async def tail_log(self, *, quiet: bool = False) -> None:
        if not self.log_job:
            return
        self.log_at = time.monotonic()
        path = self.log_job.get(f"{self.log_stream}_path")
        if path:
            # Periodic re-reads report problems in the log status line, not as toasts.
            await self.request("tail", path, quiet=quiet)

    # -- events -------------------------------------------------------------------------

    async def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:
        data = event.node.data
        if (
            data
            and data.get("type") in ("dir", "link")
            and not data.get("loaded")
            and not data.get("loading")
        ):
            data["loading"] = True
            await self.request("list", data["path"], event.node, quiet=data["type"] == "link")

    async def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        node, data = event.node, event.node.data
        if not data:
            return
        if data["type"] == "page":
            await self.load_more(node)
        elif not node.allow_expand:
            await self.request("preview", data["path"])

    async def load_more(self, leaf: TreeNode) -> None:
        parent = leaf.parent
        if parent is None or not parent.data or not parent.data.get("next"):
            return
        page = parent.data["next"]
        slot = f"list:{parent.id}"
        guard = {"node": parent, "slot": slot, "gen": self.generation.get(slot)}
        try:
            result = await asyncio.to_thread(
                service.inspect_request,
                self.workspace,
                "list",
                parent.data["path"],
                offset=page["offset"],
                token=page["token"],
            )
        except RemoteSlurmError as exc:
            self.notify(safe_text(exc) + " Reloading the folder.", severity="warning")
            await self.request("list", parent.data["path"], parent)
            return
        # Drop the page if the folder was re-listed, replaced, or removed meanwhile.
        if not self.current(guard) or parent.data.get("next") != page:
            return
        reveal = None
        if result.get("entries"):
            reveal = posixpath.join(parent.data["path"], result["entries"][0]["name"])
        self.show_listing(parent, result, append=True, reveal=reveal)

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        tree = self.query_one("#files", FileTree)
        if await self.request("list", event.value or ".", tree.root):
            self.remove_class("previewing")
            tree.focus()

    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        self.refresh_bindings()
        focused = self.focused
        if focused is None or event.pane not in focused.ancestors_with_self:
            self.call_after_refresh(self.main_widget(event.pane.id or "").focus)

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        self.show_details(event.cursor_row)

    async def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        await self.action_job_log("stdout")

    # -- actions ------------------------------------------------------------------------

    def selected_job(self) -> dict | None:
        index = self.query_one("#jobs", DataTable).cursor_row
        return self.work[index] if 0 <= index < len(self.work) else None

    def switch(self, tab: str, widget: Any = None) -> None:
        # Focus synchronously: if focus stays in the pane being hidden, Textual hands it to a
        # sibling there, and TabbedContent then switches straight back to that pane.
        self.query_one("#tabs", TabbedContent).active = tab
        (widget or self.main_widget(tab)).focus()

    def action_show_tab(self, tab: str) -> None:
        self.switch(tab)

    def action_focus_files(self) -> None:
        self.remove_class("previewing")
        self.switch("files-tab", self.query_one("#files", FileTree))

    def action_focus_path(self) -> None:
        self.switch("files-tab", self.query_one("#path", Input))

    def action_escape(self) -> None:
        tab = self.active_tab
        if tab == "files-tab":
            self.action_focus_files()
        else:
            self.main_widget(tab).focus()

    async def action_job_log(self, stream: str) -> None:
        job = self.selected_job() if self.active_tab == "work-tab" else self.log_job
        if job is None:
            job = self.selected_job()
        if not job or not job.get(f"{stream}_path"):
            self.notify(f"No recorded {stream} path for this job; inspect the Files tab.")
            return
        self.log_job, self.log_stream = job, stream
        self.switch("logs-tab")
        await self.tail_log()

    async def action_open_workdir(self) -> None:
        job = self.selected_job()
        if not job or not job.get("workdir"):
            self.notify("No recorded working directory for this job.")
            return
        self.query_one("#path", Input).value = job["workdir"]
        self.action_focus_files()
        await self.request("list", job["workdir"], self.query_one("#files", FileTree).root)

    async def action_parent_dir(self) -> None:
        parent = posixpath.dirname(self.root_path.rstrip("/")) or "/"
        if parent == self.root_path:
            return
        root = self.query_one("#files", FileTree).root
        await self.request("list", parent, root, reveal=self.root_path)

    async def action_refresh(self) -> None:
        tab = self.active_tab
        if tab == "files-tab":
            tree = self.query_one("#files", FileTree)
            node = tree.cursor_node or tree.root
            if node.data and node.data.get("type") == "page":
                node = node.parent or tree.root
            data = node.data or {}
            if data.get("type") == "dir" or (data.get("type") == "link" and node.allow_expand):
                await self.request("list", data["path"], node, quiet=self.offline)
            elif data.get("path"):
                await self.request("preview", data["path"], quiet=self.offline)
        elif tab == "logs-tab":
            await self.tail_log()
        elif not self.offline:
            await self.request("refresh", self.workspace["remote_root"])
        await self.poll()

    def action_toggle_view_events(self) -> None:
        self.show_view_events = not self.show_view_events
        activity = self.query_one("#activity", RichLog)
        activity.clear()
        self.activity_empty = True
        for event in self.events:
            if self.show_view_events or event.get("kind") not in VIEW_EVENTS:
                self.write_activity(self.activity_line(event))
        if self.activity_empty:
            activity.write(Text("No activity yet.", style="dim"))
        self.query_one("#activity-status", Static).update(
            "v hides the view's own file reads"
            if self.show_view_events
            else "v shows the view's own file reads"
        )

    def action_help(self) -> None:
        self.push_screen(HelpScreen())
