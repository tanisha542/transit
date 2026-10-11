"""Phase 3 self-check -- PROVES both deadlock layers work against the database.

Layer 1  AVOIDANCE  : the Banker refuses the grant that would create the cycle.
Layer 2  DETECTION  : with the Banker switched off, the cycle forms, the
                      wait-for graph finds it, the youngest job is rolled back,
                      and the survivor gets its resources.

Scenario is the one from report section 5:
    Job#121 holds 32 GB on node 2, wants 12 cores on node 1
    Job#122 holds 10 cores on node 1, wants 40 GB on node 2

    py -3 test_deadlock.py
"""
import banker
import db
import demo
import wfg

NODE1, NODE2 = demo.NODE1, demo.NODE2
JOB_A, JOB_B = demo.JOB_A, demo.JOB_B
SUBMITTED = {JOB_A: 1.0, JOB_B: 2.0}          # 122 is the younger transaction


def _fresh(conn):
    demo.reset_cluster(conn, restart_ids_at=JOB_A)
    demo.create_job(conn, JOB_A, "Tensor_Train", 12, 32768, priority=1)
    demo.create_job(conn, JOB_B, "Graph_Rank",   10, 40960, priority=1)


def _capacity_ok(conn):
    return conn.execute("""
        SELECT COUNT(*) FROM CPU_Resources c
        JOIN Memory_Resources r ON r.machine_id = c.machine_id
        WHERE c.allocated_cores > c.total_cores OR r.allocated_ram_mb > r.total_ram_mb
    """).fetchone()[0] == 0


def test_detection(conn):
    """Let the deadlock form, then detect and recover from it."""
    _fresh(conn)

    assert demo.request(conn, JOB_A, NODE2, 0, 32768), "121 should get its 32GB on node 2"
    assert demo.request(conn, JOB_B, NODE1, 10, 0), "122 should get its 10 cores on node 1"

    graph = wfg.WaitForGraph()

    # 121 wants 12 cores on node 1, but 122 is holding 10 of the 20.
    assert not demo.request(conn, JOB_A, NODE1, 12, 0), "121 must be refused"
    for holder in demo.holders_on(conn, NODE1):
        graph.add_wait(JOB_A, holder)

    # 122 wants 40GB on node 2, but 121 is holding 32 of the 64.
    assert not demo.request(conn, JOB_B, NODE2, 0, 40960), "122 must be refused"
    for holder in demo.holders_on(conn, NODE2):
        graph.add_wait(JOB_B, holder)

    cycle = graph.find_cycle()
    assert cycle is not None, "the circular wait must be detected"
    assert set(cycle) == {JOB_A, JOB_B}, cycle

    victim = wfg.youngest(cycle, SUBMITTED)
    assert victim == JOB_B, f"the youngest job must be the victim, got {victim}"

    demo.rollback_job(conn, victim)
    graph.clear_job(victim)
    assert graph.find_cycle() is None, "the rollback must break the cycle"

    # The survivor can now be placed -- which is what makes this a recovery
    # rather than merely a diagnosis.
    assert demo.request(conn, JOB_A, NODE1, 12, 0), "121 must be unblocked by the rollback"

    status = conn.execute("SELECT status FROM Jobs WHERE job_id = %s", (victim,)).fetchone()[0]
    assert status == "QUEUED", f"the victim must be re-queued, found {status}"
    assert _capacity_ok(conn), "capacity invariant broken during recovery"

    print(f"  detection OK: cycle {' -> '.join(f'Job#{j}' for j in cycle)}, "
          f"rolled back Job#{victim}, Job#{JOB_A} unblocked")


def test_avoidance(conn):
    """The Banker refuses the grant, so the cycle never forms."""
    _fresh(conn)

    free = demo.cluster_vector(conn)
    allocation = {JOB_A: [0, 0, 0, 0], JOB_B: [0, 0, 0, 0]}

    # 121 asking for its 32GB on node 2 is safe: it can then finish and free
    # everything for 122.
    grant_a = [0, 0, 0, 32768]
    assert banker.grant_is_safe(JOB_A, grant_a, free, demo.CLAIM, allocation)
    assert demo.request(conn, JOB_A, NODE2, 0, 32768)

    # 122 asking for 10 cores on node 1 FITS -- node 1 has all 20 free. A plain
    # allocator grants it and the cluster deadlocks. The Banker looks ahead and
    # sees that afterwards 121 needs 12 cores with only 10 left, while 122 needs
    # 40GB with only 32 left, so nobody could ever finish.
    free = demo.cluster_vector(conn)
    allocation = {job: demo.held_vector(conn, job) for job in (JOB_A, JOB_B)}
    grant_b = [10, 0, 0, 0]

    assert all(x <= y for x, y in zip(grant_b, free)), \
        "there IS enough free capacity -- that is exactly the trap"
    assert not banker.grant_is_safe(JOB_B, grant_b, free, demo.CLAIM, allocation), \
        "the Banker must refuse the request that leads to circular wait"

    # Holding 122 back keeps the state safe, and 121 runs to completion.
    order = banker.safe_sequence(free, demo.CLAIM, allocation)
    assert order is not None and order[0] == JOB_A, order
    assert demo.request(conn, JOB_A, NODE1, 12, 0), "121 must be placeable"
    assert _capacity_ok(conn)

    print(f"  avoidance OK: unsafe grant to Job#{JOB_B} refused, "
          f"safe order {order}, Job#{JOB_A} completed")


def main():
    with db.connect(autocommit=True) as conn:
        print("phase 3 deadlock self-check")
        test_detection(conn)
        test_avoidance(conn)
        demo.reset_cluster(conn)
    print("both layers OK "
          "(WFG detects and recovers; Banker prevents the same deadlock outright)")


if __name__ == "__main__":
    main()
