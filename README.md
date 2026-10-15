# TransactAlloc

An **adaptive, fragmentation-aware cloud resource manager** built on
PostgreSQL. Four clusters, a live dashboard, and a full DBMS treatment:
normalised schema, ACID allocation, external-fragmentation tracking with
defragmentation, wait-for-graph deadlock detection with rollback, and a
performance evaluation.

This is the merged phase-3 deliverable: the console deadlock work is folded
into a running web service with a real block-level memory model.

---

## What it does

- **Schedules** jobs with **FCFS / Priority / Round-Robin**, switching policy
  automatically as load changes (and logging *why* to `Policy_Changes`).
- **Places** each job on the **best-fit** cluster.
- **Allocates** through real SQL transactions (`AllocateJobResources`), with
  `FOR UPDATE` locking and `CHECK` constraints as the backstop. Every outcome
  is recorded in `Txn_Log` — the ACID evidence trail.
- **Models RAM as a block map**, not a counter, so external fragmentation is a
  *query*: `v_cluster_status.frag_index = 1 - largest_free / total_free`.
- **Defragments** on demand (compact a cluster's blocks, recording before/after
  in `Defrag_Events` / `Defrag_Moves`) and automatically when an allocation
  fails with `FRAGMENTED`.
- **Detects deadlocks** with a wait-for graph and rolls back the youngest
  transaction, re-queuing it. A scripted live demo reproduces the report's
  circular-wait scenario end to end.
- **Shows it all live**: cluster load bars, a memory fragmentation strip per
  cluster, job table, wait-for graph, defrag history, event feed.

---

## Stack

| Layer | Choice |
|---|---|
| Database | PostgreSQL 17 (Docker) |
| Backend | Python + FastAPI + uvicorn, `psycopg` 3 (psycopg3) |
| Frontend | Vanilla HTML / CSS / JS (no build step) |
| Tests | pytest (26 tests, including an in-process API end-to-end suite) |

---

## Setup

### 1. Database (Docker)

```powershell
docker run -d --name transactalloc-pg -e POSTGRES_PASSWORD=crm -e POSTGRES_DB=transactalloc_p3 -p 5432:5432 postgres:17
```

Connection defaults live in `db.py`: `host=localhost port=5432 user=postgres
password=crm dbname=transactalloc_p3`.

### 2. Python

```powershell
py -3 -m pip install "psycopg[binary,pool]" fastapi uvicorn jinja2 httpx pytest apscheduler
```

### 3. Schema + seed

```powershell
py -3 main.py init      # applies schema.sql then seed.sql
```

---

## Run it

```powershell
py -3 main.py serve     # http://127.0.0.1:8000
```

Open the **Cloud Management System** dashboard, sign in with one of the seeded
accounts (`admin`, `Archita`, `student` — demo authentication), then press
**Start** and **Batch**. Use the top navbar to move between Overview, Account,
Clusters, Jobs, Deadlock and Analytics; the Account tabs show each user's quota
and jobs. To watch a deadlock form and get rolled back live, press **Run
deadlock demo** in the Deadlock section.

Other entry points:

```powershell
py -3 main.py init                      # (re)create schema + seed
py -3 main.py reset                     # clear jobs, rebuild block maps
py -3 main.py list                      # cluster status
py -3 main.py submit --task X --cpus 4 --ram 8192
py -3 main.py batch --count 20
py -3 main.py dashboard                 # console node metrics

py -3 demo.py                           # report §5 walkthrough (detection)
py -3 demo.py --skip-batch --avoidance on   # Banker's avoidance instead
py -3 test_deadlock.py                  # proves both deadlock layers
py -3 wfg.py                            # wait-for-graph self-check (no DB)
py -3 banker.py                         # Banker's self-check (no DB)
```

---

## Tests

```powershell
py -3 -m pytest tests -q
```

- `tests/test_pure.py` — policies, adaptive selector, WFG, Banker (no DB).
- `tests/test_db.py` — allocation/release, block-map validity, deadlock scenario.
- `tests/test_frag.py` — fragmentation math, defrag plan/apply, block-map invariant.
- `tests/test_concurrency.py` — 64 threads racing the allocator: never overcommits.
- `tests/test_api_e2e.py` — the whole app in-process: submit → run → stats,
  policy switch, defrag, deadlock demo, graceful stop & resume.

---

## Performance evaluation

```powershell
py -3 evaluate.py --jobs 20 --workers 4 --out docs/EVALUATION.md
```

Runs the same synthetic workload under each policy (best-fit placement) and
writes throughput, average/p95 wait, turnaround, peak fragmentation,
defragmentation activity and allocation-txn latency. Current results:
[`docs/EVALUATION.md`](docs/EVALUATION.md).

---

## The two deadlock layers

**Avoidance** (`banker.py`) — before granting, test whether some completion
order still exists; refuse the grant that would make the cluster unsafe. The
report's scenario is a safe-looking grant that is actually fatal.

**Detection** (`wfg.py` + engine monitor) — let it happen, then recover. Each
blocked job gets an edge to whoever holds what it wants; a cycle is a deadlock;
the **youngest** job is rolled back. The demo:

```
[WFG] circular wait detected: Job#121 -> Job#122 -> Job#121
[DBMS] rolling back Job#122 (youngest in the cycle)
[TRANSACTION] ROLLBACK committed; resources released on cluster 1
[SCHEDULER] Job#121 unblocked and allocated; Job#122 re-queued
```

Nothing is printed theatre — every line follows a real transaction, and the
dashboard afterwards reads committed rows.

Why the graph is application-level, not `pg_locks`: these jobs are **not**
blocked inside PostgreSQL. `AllocateJobResources` raised `FRAGMENTED` /
`INSUFFICIENT_RESOURCES`, the transaction rolled back, and the job is parked in
the application queue while possibly holding resources on another cluster. No
database lock is held during that wait, so only the scheduler can see it.

---

## Project layout

```
schema.sql  seed.sql        database: schema + seed
db.py                       connection + reset helpers
engine.py                   queue, worker pool, adaptive policy, reconciler,
                            deadlock monitor, metrics sampler
worker.py                   one job's lifecycle over real transactions
frag.py adaptive.py         fragmentation + policy selection
policies.py wfg.py banker.py scheduling / placement / graph / avoidance
api.py app.py main.py       HTTP layer, ASGI app, CLI
evaluate.py demo.py         evaluation harness, report walkthrough
static/                     dashboard (index.html, styles.css, app.js)
tests/                      pytest suite
docs/                       ER_DIAGRAM.md, SCHEMA.md, ARCHITECTURE.md, EVALUATION.md
```

## Docs

- [`docs/ER_DIAGRAM.md`](docs/ER_DIAGRAM.md) — entity-relationship model.
- [`docs/SCHEMA.md`](docs/SCHEMA.md) — every table, constraint and function; normalisation notes.
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — components, job lifecycle, deadlock & fragmentation handling.
- [`docs/EVALUATION.md`](docs/EVALUATION.md) — measured performance per policy.
