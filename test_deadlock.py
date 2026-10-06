"""Phase 3 self-check -- deadlock detection against the database.
"""
import db
import wfg
import demo

NODE1, NODE2 = demo.NODE1, demo.NODE2
JOB_A, JOB_B = demo.JOB_A, demo.JOB_B
SUBMITTED = {JOB_A: 1.0, JOB_B: 2.0}


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
    assert not demo.request(conn, JOB_A, NODE1, 12, 0), "121 must be refused"
    for holder in demo.holders_on(conn, NODE1):
        graph.add_wait(JOB_A, holder)

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
    assert demo.request(conn, JOB_A, NODE1, 12, 0), "121 must be unblocked by the rollback"
    assert _capacity_ok(conn)
    print(f"  detection OK: cycle resolved, rolled back Job#{victim}")


def main():
    with db.connect(autocommit=True) as conn:
        print("phase 3 deadlock self-check")
        test_detection(conn)
        demo.reset_cluster(conn)
    print("detection test passed")


if __name__ == "__main__":
    main()
