"""Consumer: ACID allocation per job against v2 schema.
"""

import threading
import time
import psycopg
import db

HEARTBEAT_SEC = 2
_active_lock = threading.Lock()
_active = 0

_graph = None
_graph_lock = threading.Lock()
_release_hook = None
_release_lock = threading.Lock()


def set_graph(graph):
    global _graph
    with _graph_lock:
        _graph = graph


def set_release_hook(fn):
    global _release_hook
    with _release_lock:
        _release_hook = fn


def _notify_release(job_id):
    hook = _release_hook
    if hook is not None:
        try:
            hook(job_id)
        except Exception:
            pass


def _graph_add_waits(conn, job_id, machine_id):
    if _graph is None:
        return []
    holders = [r[0] for r in conn.execute(
        "SELECT DISTINCT job_id FROM Allocations WHERE machine_id=%s AND released_at IS NULL",
        (machine_id,),
    ).fetchall()]
    for h in holders:
        _graph.add_wait(job_id, h)
    return holders


def _graph_clear(job_id):
    if _graph is not None:
        _graph.clear_job(job_id)


def active_count():
    with _active_lock:
        return _active


def _log(conn, job_id, level, message):
    try:
        conn.execute(
            "INSERT INTO Execution_Logs (job_id, log_level, message) VALUES (%s, %s, %s)",
            (job_id, level, message),
        )
    except psycopg.Error:
        pass


def pick_machine(conn, job, policy="best-fit"):
    rows = conn.execute(
        """SELECT m.machine_id, c.total_cores - c.allocated_cores AS free_cores,
                  r.total_ram_mb - r.allocated_ram_mb AS free_ram
           FROM Machines m
           JOIN CPU_Resources c ON c.machine_id = m.machine_id
           JOIN Memory_Resources r ON r.machine_id = m.machine_id
           WHERE m.status = 'ONLINE'
           ORDER BY m.machine_id"""
    ).fetchall()
    for mid, free_c, free_r in rows:
        if free_c >= job["req_cpus"] and free_r >= job["req_ram_mb"]:
            return mid
    return None


def try_allocate(conn, job, machine_id, worker_id):
    start = time.perf_counter()
    try:
        with conn.transaction():
            alloc_id = conn.execute(
                "SELECT AllocateJobResources(%s, %s, %s, %s)",
                (job["job_id"], machine_id, job["req_cpus"], job["req_ram_mb"]),
            ).fetchone()[0]
            _log(conn, job["job_id"], "INFO", f"allocated {job['req_cpus']}c/{job['req_ram_mb']}MB on m{machine_id}")
        dur = int((time.perf_counter() - start) * 1000)
        conn.execute(
            "INSERT INTO Txn_Log (job_id,machine_id,op,outcome,reason,duration_ms) VALUES (%s,%s,'ALLOCATE','COMMIT',NULL,%s)",
            (job["job_id"], machine_id, dur),
        )
        _graph_clear(job["job_id"])
        return True
    except psycopg.errors.RaiseException as exc:
        reason = str(exc).strip().splitlines()[0]
        dur = int((time.perf_counter() - start) * 1000)
        holders = _graph_add_waits(conn, job["job_id"], machine_id)
        _log(conn, job["job_id"], "ERROR", f"allocation rolled back on m{machine_id}: {reason} (held by {holders})")
        conn.execute(
            "INSERT INTO Txn_Log (job_id,machine_id,op,outcome,reason,duration_ms) VALUES (%s,%s,'ALLOCATE','ROLLBACK',%s,%s)",
            (job["job_id"], machine_id, reason[:240], dur),
        )
        return False


def _simulate(conn, job, machine_id, worker_id, run_for):
    elapsed = 0
    while elapsed < run_for:
        tick = min(HEARTBEAT_SEC, run_for - elapsed)
        time.sleep(tick)
        elapsed += tick
        _log(conn, job["job_id"], "INFO", f"heartbeat {elapsed}/{run_for}s on m{machine_id}")
    return elapsed, False


def _finish(conn, job, ran_for, policy, worker_id):
    start = time.perf_counter()
    with conn.transaction():
        conn.execute("SELECT ReleaseJobResources(%s)", (job["job_id"],))
        conn.execute(
            "UPDATE Jobs SET status='COMPLETED', remaining_sec=0, completed_at=NOW() WHERE job_id=%s",
            (job["job_id"],),
        )
        _log(conn, job["job_id"], "INFO", "completed, resources released")
    dur = int((time.perf_counter() - start) * 1000)
    conn.execute(
        "INSERT INTO Txn_Log (job_id,op,outcome,reason,duration_ms) VALUES (%s,'RELEASE','COMMIT',NULL,%s)",
        (job["job_id"], dur),
    )
    _graph_clear(job["job_id"])
    _notify_release(job["job_id"])


def _requeue(conn, job, worker_id):
    conn.execute("UPDATE Jobs SET status='QUEUED' WHERE job_id=%s", (job["job_id"],))
    _log(conn, job["job_id"], "WARN", "re-queued (no feasible placement now)")
    _notify_release(job["job_id"])


def execute_job(job, worker_id, policy="fcfs"):
    global _active
    with _active_lock:
        _active += 1
    try:
        with db.connect(autocommit=True) as conn:
            mid = pick_machine(conn, job, policy)
            if mid is None:
                _requeue(conn, job, worker_id)
                return
            if not try_allocate(conn, job, mid, worker_id):
                _requeue(conn, job, worker_id)
                return
            _simulate(conn, job, mid, worker_id, job["remaining_sec"])
            _finish(conn, job, job["remaining_sec"], policy, worker_id)
    finally:
        with _active_lock:
            _active -= 1
