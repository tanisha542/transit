"""Consumer: ACID allocation per job against v2 schema.

Each worker owns its own connection with autocommit=True + explicit
conn.transaction(): the row locks taken inside AllocateJobResources()
(SELECT ... FOR UPDATE on CPU/Memory aggregate rows and Memory_Blocks)
are genuinely contended and a raised exception produces a real ROLLBACK.
"""

import threading
import time

import psycopg

import db
import frag
import policies

HEARTBEAT_SEC = 2

_active_lock = threading.Lock()
_active = 0

# The engine installs its wait-for graph here so allocation failures can
# record "job X is waiting behind whoever holds cluster M" without the
# worker needing a reference to the engine.
_graph = None
_graph_lock = threading.Lock()

# The engine installs a callback here so a worker can tell it "this job left
# the engine" -- it finished, was preempted, or must be retried. The engine
# clears the job from its in-flight set and from the wait-for graph.
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


# The engine shares its stop flag so a running job can be abandoned promptly
# (and re-queued) when the engine is asked to stop.
_stop_event = None


def set_stop_event(evt):
    global _stop_event
    _stop_event = evt


def _stopping():
    return _stop_event is not None and _stop_event.is_set()


def _graph_add_waits(conn, job_id, machine_id):
    """Record edges job_id -> every job currently holding cluster machine_id."""
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
    # A reset can delete the job while this thread is still running it; a log
    # line is not worth crashing a worker over.
    try:
        conn.execute(
            "INSERT INTO Execution_Logs (job_id, log_level, message) VALUES (%s, %s, %s)",
            (job_id, level, message),
        )
    except psycopg.Error:
        pass


def machine_snapshot(conn):
    rows = conn.execute(
        """
        SELECT machine_id, total_cores, free_cores, total_ram_mb, free_ram_mb
          FROM v_cluster_status
         WHERE status = 'ONLINE'
         ORDER BY machine_id
        """
    ).fetchall()
    return [dict(machine_id=r[0], total_cores=r[1], free_cores=r[2], total_ram_mb=r[3], free_ram_mb=r[4]) for r in rows]


def pick_machine(conn, job, policy):
    machines = machine_snapshot(conn)
    # Placement is best-fit by default (tightest cluster that still fits), as
    # the design requires; "first-fit" is kept as an explicit alternative for
    # comparison runs.
    chooser = policies.first_fit if policy == "first-fit" else policies.best_fit
    return chooser(job, machines)


def try_allocate(conn, job, machine_id, worker_id):
    start = time.perf_counter()
    try:
        with conn.transaction():
            alloc_id = conn.execute(
                "SELECT AllocateJobResources(%s, %s, %s, %s)",
                (job["job_id"], machine_id, job["req_cpus"], job["req_ram_mb"]),
            ).fetchone()[0]
            _log(
                conn,
                job["job_id"],
                "INFO",
                f"allocated {job['req_cpus']}c/{job['req_ram_mb']}MB on m{machine_id} (alloc_id={alloc_id})",
            )
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
        if "FRAGMENTED" in reason:
            _log(conn, job["job_id"], "WARN", f"fragmented on m{machine_id}: {reason}")
            conn.execute(
                "INSERT INTO System_Events (ts,kind,severity,message) VALUES (NOW(),'DEFRAG','WARN',%s)",
                (f"job#{job['job_id']} needs defrag on m{machine_id}",),
            )
        else:
            # Blocked behind other jobs: this is the wait-for edge.
            holders = _graph_add_waits(conn, job["job_id"], machine_id)
            _log(conn, job["job_id"], "ERROR",
                 f"allocation rolled back on m{machine_id}: {reason} (held by {holders})")
        conn.execute(
            "INSERT INTO Txn_Log (job_id,machine_id,op,outcome,reason,duration_ms) VALUES (%s,%s,'ALLOCATE','ROLLBACK',%s,%s)",
            (job["job_id"], machine_id, reason[:240], dur),
        )
        return False



def _simulate(conn, job, machine_id, worker_id, run_for):
    """Run the job in HEARTBEAT_SEC ticks. Returns (elapsed, aborted)."""
    print(f"[WORKER-{worker_id}] Job #{job['job_id']} RUNNING on m{machine_id} for {run_for}s")
    elapsed = 0
    while elapsed < run_for:
        if _stopping():
            return elapsed, True
        tick = min(HEARTBEAT_SEC, run_for - elapsed)
        time.sleep(tick)
        elapsed += tick
        if _stopping():
            return elapsed, True
        _log(conn, job["job_id"], "INFO", f"heartbeat {elapsed}/{run_for}s on m{machine_id}")
    return elapsed, False


def _abort(conn, job, ran_for, worker_id):
    """Engine stopping: release resources and park the job as QUEUED to resume."""
    remaining = max(0, job["remaining_sec"] - ran_for)
    try:
        with conn.transaction():
            conn.execute("SELECT ReleaseJobResources(%s)", (job["job_id"],))
            conn.execute(
                "UPDATE Jobs SET status='QUEUED', remaining_sec=%s WHERE job_id=%s",
                (remaining, job["job_id"]),
            )
            _log(conn, job["job_id"], "WARN", f"engine stopping, {remaining}s remaining re-queued")
    except psycopg.Error as exc:
        # A reset may already have removed the job; the abort is best-effort.
        print(f"[WORKER-{worker_id}] abort of #{job['job_id']} skipped: {exc}")
    print(f"[WORKER-{worker_id}] Job #{job['job_id']} ABORTED (engine stop), {remaining}s left")
    _notify_release(job["job_id"])



def _finish(conn, job, ran_for, policy, worker_id):
    start = time.perf_counter()
    try:
        with conn.transaction():
            conn.execute("SELECT ReleaseJobResources(%s)", (job["job_id"],))
            remaining = max(0, job["remaining_sec"] - ran_for)
            if policy == "round-robin" and remaining > 0:
                conn.execute(
                    "UPDATE Jobs SET status='QUEUED', remaining_sec=%s WHERE job_id=%s",
                    (remaining, job["job_id"]),
                )
                _log(conn, job["job_id"], "INFO", f"preempted, {remaining}s remaining")
                print(f"[WORKER-{worker_id}] Job #{job['job_id']} PREEMPTED after {ran_for}s -> re-queued")
            else:
                conn.execute(
                    "UPDATE Jobs SET status='COMPLETED', remaining_sec=0, completed_at=NOW() WHERE job_id=%s",
                    (job["job_id"],),
                )
                _log(conn, job["job_id"], "INFO", "completed, resources released")
                print(f"[WORKER-{worker_id}] Job #{job['job_id']} COMPLETED")
        dur = int((time.perf_counter() - start) * 1000)
        conn.execute(
            "INSERT INTO Txn_Log (job_id,op,outcome,reason,duration_ms) VALUES (%s,'RELEASE','COMMIT',NULL,%s)",
            (job["job_id"], dur),
        )
        _graph_clear(job["job_id"])
        _notify_release(job["job_id"])
    except Exception as exc:
        dur = int((time.perf_counter() - start) * 1000)
        conn.execute(
            "INSERT INTO Txn_Log (job_id,op,outcome,reason,duration_ms) VALUES (%s,'RELEASE','ROLLBACK',%s,%s)",
            (job["job_id"], str(exc)[:240], dur),
        )
        raise


def _requeue(conn, job, worker_id):
    conn.execute("UPDATE Jobs SET status='QUEUED' WHERE job_id=%s", (job["job_id"],))
    _log(conn, job["job_id"], "WARN", "re-queued (no feasible placement now)")
    print(f"[WORKER-{worker_id}] Job #{job['job_id']} re-queued")
    # Hand it back to the engine so the reconciler retries it once blockers clear.
    _notify_release(job["job_id"])


def execute_job(job, worker_id, policy="fcfs"):
    global _active
    with _active_lock:
        _active += 1
    try:
        with db.connect(autocommit=True) as conn:
            exists = conn.execute("SELECT 1 FROM Jobs WHERE job_id=%s", (job["job_id"],)).fetchone()
            if not exists:
                _graph_clear(job["job_id"])
                _notify_release(job["job_id"])
                return
            machine_id = pick_machine(conn, job, policy)
            if machine_id is None:
                _requeue(conn, job, worker_id)
                return
            if not try_allocate(conn, job, machine_id, worker_id):
                # FRAGMENTED path: try auto-defrag this cluster then retry once
                # attempt defrag
                try:
                    with conn.transaction():
                        st = frag.cluster_status(conn, machine_id)
                        moves = frag.defrag_plan(conn, machine_id)
                        if moves:
                            frag.apply_defrag(conn, machine_id, moves, triggered_by="AUTO", reason="on-demand retry")
                except psycopg.Error:
                    pass
                # retry
                if try_allocate(conn, job, machine_id, worker_id):
                    pass  # proceed
                else:
                    _requeue(conn, job, worker_id)
                    return
            # simulate + finish
            run_for = (
                min(policies.QUANTUM_SEC, job["remaining_sec"])
                if policy == "round-robin"
                else job["remaining_sec"]
            )
            ran, aborted = _simulate(conn, job, machine_id, worker_id, run_for)
            if aborted:
                _abort(conn, job, ran, worker_id)
            else:
                _finish(conn, job, run_for, policy, worker_id)
    except psycopg.errors.ForeignKeyViolation:
        # The job (and its allocations) were deleted by a concurrent reset.
        _graph_clear(job["job_id"])
        _notify_release(job["job_id"])
    except psycopg.Error as exc:
        print(f"[WORKER-{worker_id}] Job #{job['job_id']} dropped: {exc}")
        _graph_clear(job["job_id"])
        _notify_release(job["job_id"])
    finally:
        with _active_lock:
            _active -= 1
