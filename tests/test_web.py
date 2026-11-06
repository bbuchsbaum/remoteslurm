"""The local dashboard server: access guard, state assembly, feed, and job-file reads."""

from __future__ import annotations

import json
import time

import pytest
from starlette.testclient import TestClient

from remoteslurm.activity.discovery import lease
from remoteslurm.activity.model import Subject
from remoteslurm.activity.producer import target_id
from remoteslurm.activity.store import ActivityStore
from remoteslurm.config import Config, HostConfig
from remoteslurm.web import files as jobfiles
from remoteslurm.web.provider import LiveProvider
from remoteslurm.web.server import COOKIE as BASE_COOKIE
from remoteslurm.web.server import create_app
from remoteslurm.web.state import Journal, feed_item, hint

PORT = 8765
TOKEN = "t0ken-for-tests"
COOKIE = f"{BASE_COOKIE}_{PORT}"


class Fixed:
    """A provider with canned answers, to exercise the HTTP layer alone."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def state(self) -> dict:
        return {"now": time.time(), "journal": "j", "demo": False, "hosts": [], "jobs": []}

    def activity(self, after, limit):
        return {"events": [], "cursor": "c", "resync": False}

    def keepalive(self) -> None:
        return None

    def tail(self, host, job, which, task):
        self.calls.append(("tail", host, job, which, task))
        return {"request": "abc", "state": "QUEUED"}

    def files(self, host, job):
        self.calls.append(("files", host, job))
        return {"request": "def", "state": "QUEUED"}

    def poll(self, rid, kind):
        return {"request": rid, "state": "DONE", "kind": kind}


def client(provider=None, host=f"127.0.0.1:{PORT}") -> TestClient:
    app = create_app(provider or Fixed(), token=TOKEN, port=PORT)
    return TestClient(app, base_url=f"http://{host}")


def signed_in(provider=None) -> TestClient:
    c = client(provider)
    response = c.get(f"/?token={TOKEN}", follow_redirects=False)
    assert response.status_code == 303
    return c


def test_token_is_exchanged_for_a_strict_httponly_cookie():
    c = client()
    response = c.get(f"/?token={TOKEN}", follow_redirects=False)
    assert response.status_code == 303 and response.headers["location"] == "/"
    cookie = response.headers["set-cookie"].lower()
    assert f"{COOKIE}=" in cookie and "httponly" in cookie and "samesite=strict" in cookie
    assert c.get("/api/state").status_code == 200


def test_everything_but_health_requires_the_cookie():
    c = client()
    assert c.get("/healthz").json() == {"ok": True}
    for path in ("/", "/api/state", "/api/activity", "/static/app.js", "/api/tail/abc"):
        assert c.get(path).status_code == 401, path
    assert c.get("/?token=wrong", follow_redirects=False).status_code == 403
    c.cookies.set(COOKIE, "wrong")
    assert c.get("/api/state").status_code == 401


def test_foreign_host_header_is_refused_to_block_dns_rebinding():
    for host in ("evil.example:8765", f"127.0.0.1:{PORT + 1}", "127.0.0.1"):
        assert client(host=host).get(f"/?token={TOKEN}").status_code == 421, host
    assert client(host=f"localhost:{PORT}").get("/healthz").status_code == 200


def test_security_headers_on_every_response():
    c = signed_in()
    for response in (c.get("/api/state"), c.get("/healthz"), client().get("/api/state")):
        csp = response.headers["content-security-policy"]
        assert "default-src 'self'" in csp and "frame-ancestors 'none'" in csp
        assert "unsafe-inline" not in csp
        assert response.headers["x-content-type-options"] == "nosniff"
        assert "access-control-allow-origin" not in response.headers


def test_posts_need_the_custom_header_and_json_and_name_only_a_job():
    provider = Fixed()
    c = signed_in(provider)
    body = {"host": "nibi", "job": "8830187", "which": "err", "task": None}
    assert c.post("/api/tail", json=body).status_code == 403  # no X-Rslurm header
    assert (
        c.post(
            "/api/tail",
            content=json.dumps(body),
            headers={"x-rslurm": "1", "content-type": "text/plain"},
        ).status_code
        == 403
    )
    ok = c.post("/api/tail", json=body, headers={"x-rslurm": "1"})
    assert ok.status_code == 200 and provider.calls == [("tail", "nibi", "8830187", "err", None)]
    for bad in (
        {**body, "job": "../../etc/passwd"},
        {**body, "which": "/etc/passwd"},
        {**body, "task": -1},
        {"host": "nibi", "path": "/etc/passwd"},
    ):
        assert c.post("/api/tail", json=bad, headers={"x-rslurm": "1"}).status_code == 400
    assert c.get("/api/tail/..%2Fx").status_code in (400, 404)
    assert c.get("/api/files/abc123").json()["kind"] == "list"


def test_stream_sends_state_first_as_server_sent_events(monkeypatch):
    import remoteslurm.web.server as server

    monkeypatch.setattr(server, "STATE_EVERY", 0.01)
    monkeypatch.setattr(server, "STREAM_ROUNDS", 2)
    c = signed_in()
    with c.stream("GET", "/api/stream") as response:
        assert response.headers["content-type"].startswith("text/event-stream")
        lines = response.iter_lines()
        seen = [next(lines) for _ in range(4)]
    assert "event: state" in seen
    data = next(line for line in seen if line.startswith("data: "))
    assert json.loads(data[6:])["journal"] == "j"


def test_demo_page_serves_index_and_example_state():
    from remoteslurm.web.provider import DemoProvider

    c = signed_in(DemoProvider())
    page = c.get("/")
    assert page.status_code == 200 and "text/html" in page.headers["content-type"]
    state = c.get("/api/state").json()
    assert state["demo"] is True and state["jobs"]
    now = state["now"]
    assert all(job["submit"] is None or job["submit"] <= now for job in state["jobs"])


# ----------------------------------------------------------------------------- state assembly
def host(name="nibi") -> HostConfig:
    return HostConfig(name=name, ssh=f"{name}-alias", mfa=False)


def seed(hc: HostConfig, jobs: dict, **doc) -> None:
    ident = target_id(hc)
    ActivityStore().save(
        "discovery",
        ident,
        {
            "host": hc.name,
            "target_id": ident,
            "jobs": jobs,
            "observed_at": time.time() - 10,
            "accounting": "ok",
            "accounting_at": time.time() - 20,
            **doc,
        },
    )


def journal(*hosts: HostConfig) -> Journal:
    return Journal(Config(hosts={h.name: h for h in hosts}), list(hosts))


def test_discovered_jobs_are_untracked_until_the_registry_knows_them():
    hc = host()
    seed(
        hc,
        {
            "8830330": {
                "id": "8830330",
                "name": "glm",
                "state": "PENDING",
                "reason": "Resources",
                "source": "squeue",
                "workdir": "/project/glm",
                "submit": 1.0,
            },
            "8830187": {
                "id": "8830187",
                "name": "fit",
                "state": "OUT_OF_MEMORY",
                "exit": "0:125",
                "maxrss": 34144990003,
                "mem": "32G",
                "source": "sacct",
                "workdir": "/project/fit",
                "end": time.time() - 60,
                "submit": 2.0,
            },
        },
    )
    ActivityStore().save(
        "work",
        "w1",
        {
            "work_id": "w1",
            "target_id": target_id(hc),
            "job_id": "8830187",
            "submit_time": 1,
            "stdout_path": "/project/fit/fit.out",
            "stderr_path": "/project/fit/fit.err",
        },
    )
    state = journal(hc).state()
    jobs = {j["id"]: j for j in state["jobs"]}
    assert jobs["8830330"]["tracked"] is False and jobs["8830187"]["tracked"] is True
    assert jobs["8830187"]["stderr"] == "/project/fit/fit.err"
    assert jobs["8830187"]["maxrss"] == "31.8G" and jobs["8830187"]["exitSource"] == "sacct"
    assert "31.8G" in jobs["8830187"]["hint"] and "32G" in jobs["8830187"]["hint"]
    # No recorded log: the documented default, labelled as a guess.
    assert jobs["8830330"]["stdout"] == "/project/glm/slurm-8830330.out"
    assert jobs["8830330"]["pathsGuessed"] is True
    assert "free resources" in jobs["8830330"]["hint"]
    assert state["hosts"][0]["coverage"] == "all"
    assert "nibi" in state["coverage"]


def test_unseen_host_is_never_reported_live_and_says_why():
    hc = host("fir")
    state = journal(hc).state()
    assert state["hosts"][0]["observedAt"] is None
    assert state["hosts"][0]["coverage"] == "tracked"
    assert state["jobs"] == []


def test_job_that_left_the_queue_is_unknown_not_running():
    hc = host()
    seed(
        hc,
        {
            "9": {
                "id": "9",
                "name": "x",
                "state": "UNKNOWN",
                "last_state": "RUNNING",
                "source": "retained",
                "vanished_at": time.time() - 30,
            }
        },
    )
    job = journal(hc).state()["jobs"][0]
    assert job["state"] == "UNKNOWN" and job["exit"] is None and job["maxrss"] is None
    assert "Left the queue" in job["hint"]


def test_array_rolls_up_with_failed_task_facts():
    hc = host("trillium")
    seed(
        hc,
        {
            "4412702": {
                "id": "4412702",
                "name": "fit_sweep",
                "array": True,
                "state": "RUNNING",
                "source": "squeue",
                "limit": 16,
                "reason": "JobArrayTaskLimit",
                "tasks": {"0": "COMPLETED", "1": "RUNNING", "2": "PENDING", "63": "FAILED"},
                "task_info": {
                    "63": {"exit": "1:0", "elapsed": 3120, "node": "c1", "maxrss": 1024**3}
                },
                "stdout": "/s/logs/fit_4412702_1.out",
                "paths_task": "1",
                "workdir": "/s",
            }
        },
    )
    job = journal(hc).state()["jobs"][0]
    array = job["array"]
    assert array["ids"] == [0, 1, 2, 63] and array["states"][3] == "FAILED"
    assert array["exits"] == {"63": "1:0"} and array["taskInfo"]["63"]["maxrss"] == "1G"
    assert array["limit"] == 16 and job["exit"] is None
    assert "1 of 4 tasks failed" in job["hint"]
    assert jobfiles.task_path(job["stdout"], job, 63) == "/s/logs/fit_4412702_63.out"


def test_log_patterns_expand_only_when_every_token_is_known():
    job = {"id": "77", "name": "fit", "user": "brad", "array": {"pathsTask": None}}
    assert jobfiles.expand("/l/%x_%A_%a.out", job, 5) == "/l/fit_77_5.out"
    assert jobfiles.expand("/l/%x_%A_%3a.out", job, 5) == "/l/fit_77_005.out"
    assert jobfiles.expand("/l/%N.out", job, 5) is None
    assert jobfiles.expand("/l/%j.out", job, 5) is None  # an array task's own id is unknown
    assert jobfiles.expand("/l/%j.out", {"id": "8", "name": "s"}, None) == "/l/8.out"


def test_hints_keep_the_key_fact_per_failure_kind():
    assert "2:00:00" in hint({"state": "TIMEOUT", "timelimit": 7200, "elapsed": 7200})
    assert "unknown" in hint({"state": "OUT_OF_MEMORY", "mem": "32G", "maxrss": None})
    assert "code 1:0" in hint({"state": "FAILED", "exit": "1:0"})
    assert "exit code is unknown" in hint({"state": "FAILED", "exit": None})
    assert hint({"state": "COMPLETED"}) is None


def event(kind: str, payload: dict, host_alias: str = "nibi", job: str | None = None) -> dict:
    return ActivityStore().append(
        subject=Subject("t", host_alias, job_id=job), kind=kind, payload=payload, source="test"
    )


def test_feed_maps_transitions_and_marks_finishes_as_alerts():
    hosts = {"nibi"}
    done = feed_item(
        event(
            "job.state.changed",
            {
                "job_id": "1",
                "name": "fit",
                "state": "COMPLETED",
                "from": "RUNNING",
                "exit_code": "0:0",
            },
        ),
        hosts,
    )
    assert done["kind"] == "ok" and done["alert"] and done["text"] == "fit completed"
    oom = feed_item(
        event(
            "job.state.changed",
            {"job_id": "2", "name": "fit7", "state": "OUT_OF_MEMORY", "exit_code": "0:125"},
        ),
        hosts,
    )
    assert oom["kind"] == "fail" and oom["alert"] and "memory" in oom["text"]
    started = feed_item(
        event("job.state.changed", {"job_id": "3", "name": "a", "state": "RUNNING"}), hosts
    )
    assert started["kind"] == "run" and not started["alert"]
    tasks = feed_item(
        event(
            "job.state.changed",
            {
                "job_id": "4",
                "name": "sweep",
                "state": "RUNNING",
                "new_failed_tasks": 2,
                "failed_tasks": 3,
                "tasks": 200,
            },
        ),
        hosts,
    )
    assert tasks["alert"] and tasks["text"] == "sweep: 2 tasks failed"
    assert feed_item(event("files.observed", {"path": "/x"}), hosts) is None
    assert (
        feed_item(event("job.state.changed", {"state": "FAILED"}, host_alias="other"), hosts)
        is None
    )


def test_activity_cursor_returns_only_new_items():
    hc = host()
    j = journal(hc)
    event("job.state.changed", {"job_id": "1", "name": "a", "state": "RUNNING"})
    first = j.activity()
    assert [e["text"] for e in first["events"]] == ["a started"]
    assert j.activity(first["cursor"])["events"] == []
    event("job.state.changed", {"job_id": "1", "name": "a", "state": "FAILED", "exit_code": "1:0"})
    news = j.activity(first["cursor"])["events"]
    assert [e["text"] for e in news] == ["a failed"] and news[0]["alert"]


def test_job_file_reads_queue_under_a_lease_and_never_take_a_path(monkeypatch):
    from remoteslurm.activity import monitor

    monkeypatch.setattr(monitor, "control", lambda enabled=None: {"enabled": True})
    hc = host()
    seed(
        hc,
        {
            "5": {
                "id": "5",
                "name": "x",
                "state": "RUNNING",
                "source": "squeue",
                "workdir": "/project/x",
                "stdout": "/project/x/x.out",
            }
        },
    )
    provider = LiveProvider(Config(hosts={"nibi": hc}), [hc])
    refused = provider.tail("nibi", "5", "out", None)
    assert refused["state"] == "UNAVAILABLE" and "not being observed" in refused["action"]
    lease([hc])
    queued = provider.tail("nibi", "5", "out", None)
    assert queued["state"] == "QUEUED" and queued["path"] == "/project/x/x.out"
    assert provider.tail("nibi", "5", "out", None)["request"] == queued["request"]  # coalesced
    request = ActivityStore().get("inspection", queued["request"])
    assert request["workspace_id"] is None and request["kind"] == "tail"
    assert provider.tail("nibi", "404", "out", None)["state"] == "UNAVAILABLE"
    assert provider.files("nibi", "5")["path"] == "/project/x"


def test_poll_shapes_a_finished_tail_with_its_gap_label():
    store = ActivityStore()
    store.save(
        "inspection",
        "r1",
        {
            "request_id": "r1",
            "kind": "tail",
            "state": "DONE",
            "path": "/p/x.out",
            "result": {
                "content": "a\nb\n",
                "observed_at": 5.0,
                "size": 4,
                "truncated": True,
                "gap": "bounded tail; earlier bytes are omitted",
            },
        },
    )
    shaped = jobfiles.poll("r1", kind="tail")
    assert shaped["result"]["lines"] == ["a", "b"]
    assert shaped["result"]["gap"].startswith("bounded tail")
    assert jobfiles.poll("r1", kind="list")["state"] == "UNAVAILABLE"


@pytest.mark.parametrize("hostile", ["<script>alert(1)</script>", "\x1b]52;c;evil\x07"])
def test_hostile_log_text_is_returned_as_data_only(hostile):
    store = ActivityStore()
    store.save(
        "inspection",
        "r2",
        {"request_id": "r2", "kind": "tail", "state": "DONE", "result": {"content": hostile}},
    )
    assert jobfiles.poll("r2", kind="tail")["result"]["lines"] == [hostile]


def test_non_ascii_token_is_refused_not_a_server_error():
    c = client()
    assert c.get("/?token=%C3%A9", follow_redirects=False).status_code == 403
