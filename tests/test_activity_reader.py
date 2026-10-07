import json

import pytest

from remoteslurm import cli
from remoteslurm.activity.model import Subject
from remoteslurm.activity.reader import event_line, follow, parse_since
from remoteslurm.activity.store import ActivityStore
from remoteslurm.errors import InvalidArgument


def event(store, name):
    return store.append(
        subject=Subject("t", "site"), kind="test", source="fixture", payload={"name": name}
    )


def test_cli_is_local_and_cursors_are_independent(monkeypatch, capsys):
    def forbidden(*a, **k):
        pytest.fail("local activity opened remote connection")

    monkeypatch.setattr(cli, "get_cluster", forbidden)
    store = ActivityStore()
    event(store, "first")
    assert cli.main(["activity", "--json", "-H", "site"]) == 0
    page = json.loads(capsys.readouterr().out)
    event(store, "second")
    for _ in range(2):
        assert cli.main(["activity", "--json", "-H", "site", "--after", page["next_cursor"]]) == 0
        assert [e["payload"]["name"] for e in json.loads(capsys.readouterr().out)["events"]] == [
            "second",
        ]


def test_history_follow_handoff_includes_new_boundary_event(monkeypatch):
    store = ActivityStore()
    event(store, "before")
    monkeypatch.setattr("remoteslurm.activity.reader.time.sleep", lambda _: None)
    stream = follow(store)
    first = next(stream)
    event(store, "between")
    second = next(stream)
    assert [e["payload"]["name"] for p in (first, second) for e in p["events"]] == [
        "before",
        "between",
    ]
    stream.close()


def test_since_and_hostile_terminal_text():
    assert parse_since("2026-10-06T00:00:00Z") > 0
    assert parse_since("30m") > 0
    with pytest.raises(InvalidArgument):
        parse_since("2026-10-06T00:00:00")
    store = ActivityStore()
    line = event_line(event(store, "\x1b]52;c;payload\x07\nforged"))
    assert "\x1b" not in line and "\x07" not in line and "\n" not in line
    assert "\\x1b" in line


def test_jsonl_follow_is_parseable_and_interrupt_is_clean(monkeypatch, capsys):
    store = ActivityStore()
    event(store, "first")

    def pages(*a, **k):
        yield store.query()
        event(store, "second")
        yield store.query()
        raise KeyboardInterrupt

    monkeypatch.setattr("remoteslurm.activity.reader.follow", pages)
    assert cli.main(["activity", "--follow", "--json"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 2 and all(json.loads(line)["schema_version"] == 1 for line in lines)
