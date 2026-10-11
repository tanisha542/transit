-- =====================================================================
-- TransactAlloc  -  adaptive cloud resource manager
-- Full schema: clusters, jobs, ACID allocation, fragmentation-aware
-- memory blocks, defragmentation history, deadlock and policy audit.
--
-- Normalized to 3NF. The two deliberate summary columns
-- (Memory_Resources.allocated_ram_mb, CPU_Resources.allocated_cores) are
-- updated by the SAME transaction that touches the detail rows they
-- summarise, so they can never disagree with committed state. They are the
-- rows AllocateJobResources() takes its FOR UPDATE lock on.
--
-- The modelled resources:
--   CPU  -- aggregate counters per cluster (cores do not fragment)
--   RAM  -- a block map per cluster: Memory_Blocks always PARTITIONS
--           [0, total_ram_mb). Allocation carves a contiguous hole,
--           release coalesces neighbours. External fragmentation is then
--           a query, not a guess.
-- =====================================================================

DROP VIEW  IF EXISTS v_cluster_status;
DROP TABLE IF EXISTS Execution_Logs   CASCADE;
DROP TABLE IF EXISTS Defrag_Moves     CASCADE;
DROP TABLE IF EXISTS Defrag_Events    CASCADE;
DROP TABLE IF EXISTS Memory_Blocks    CASCADE;
DROP TABLE IF EXISTS Metrics_Samples  CASCADE;
DROP TABLE IF EXISTS Engine_Sample    CASCADE;
DROP TABLE IF EXISTS Txn_Log          CASCADE;
DROP TABLE IF EXISTS System_Events    CASCADE;
DROP TABLE IF EXISTS Policy_Changes   CASCADE;
DROP TABLE IF EXISTS Allocations      CASCADE;
DROP TABLE IF EXISTS Jobs             CASCADE;
DROP TABLE IF EXISTS Memory_Resources CASCADE;
DROP TABLE IF EXISTS CPU_Resources    CASCADE;
DROP TABLE IF EXISTS Machines         CASCADE;
DROP TABLE IF EXISTS Users            CASCADE;

-- ---------------------------------------------------------------------
-- 1. Who submits work, and under what quota
-- ---------------------------------------------------------------------
CREATE TABLE Users (
    user_id      SERIAL PRIMARY KEY,
    username     TEXT NOT NULL UNIQUE,
    role         TEXT NOT NULL CHECK (role IN ('Admin', 'User')),
    quota_cpus   INT  NOT NULL CHECK (quota_cpus   >= 0),
    quota_ram_mb INT  NOT NULL CHECK (quota_ram_mb >= 0)
);

-- ---------------------------------------------------------------------
-- 2. The four clusters
-- ---------------------------------------------------------------------
CREATE TABLE Machines (
    machine_id SERIAL PRIMARY KEY,
    hostname   TEXT NOT NULL UNIQUE,
    ip_address INET NOT NULL,
    status     TEXT NOT NULL CHECK (status IN ('ONLINE', 'OFFLINE', 'MAINTENANCE'))
);

CREATE TABLE CPU_Resources (
    cpu_id          SERIAL PRIMARY KEY,
    machine_id      INT NOT NULL UNIQUE REFERENCES Machines(machine_id) ON DELETE CASCADE,
    total_cores     INT NOT NULL CHECK (total_cores > 0),
    allocated_cores INT NOT NULL DEFAULT 0 CHECK (allocated_cores >= 0),
    clock_speed_ghz NUMERIC(4,2) NOT NULL,
    CONSTRAINT cpu_not_overcommitted CHECK (allocated_cores <= total_cores)
);

CREATE TABLE Memory_Resources (
    mem_id           SERIAL PRIMARY KEY,
    machine_id       INT NOT NULL UNIQUE REFERENCES Machines(machine_id) ON DELETE CASCADE,
    total_ram_mb     INT NOT NULL CHECK (total_ram_mb > 0),
    allocated_ram_mb INT NOT NULL DEFAULT 0 CHECK (allocated_ram_mb >= 0),
    swap_mb          INT NOT NULL DEFAULT 0,
    CONSTRAINT ram_not_overcommitted CHECK (allocated_ram_mb <= total_ram_mb)
);

-- ---------------------------------------------------------------------
-- 3. Jobs
-- ---------------------------------------------------------------------
CREATE TABLE Jobs (
    job_id        SERIAL PRIMARY KEY,
    user_id       INT  NOT NULL REFERENCES Users(user_id),
    task_name     TEXT NOT NULL,
    req_cpus      INT  NOT NULL CHECK (req_cpus   > 0),
    req_ram_mb    INT  NOT NULL CHECK (req_ram_mb > 0),
    priority      INT  NOT NULL DEFAULT 5 CHECK (priority BETWEEN 1 AND 10),
    duration_sec  INT  NOT NULL CHECK (duration_sec > 0),
    -- Round Robin burns this down one quantum at a time; FCFS and Priority
    -- run a job to completion and never touch it.
    remaining_sec INT  NOT NULL DEFAULT 0 CHECK (remaining_sec >= 0),
    status        TEXT NOT NULL DEFAULT 'QUEUED'
                  CHECK (status IN ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED')),
    -- started_at / completed_at feed the wait-time and throughput numbers
    -- the evaluation report needs.
    submitted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ
);

-- A new job starts with its full duration left to run. The rule lives in
-- the database so no INSERT site can forget it.
CREATE OR REPLACE FUNCTION jobs_default_remaining() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.remaining_sec = 0 THEN
        NEW.remaining_sec := NEW.duration_sec;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_jobs_default_remaining
    BEFORE INSERT ON Jobs
    FOR EACH ROW EXECUTE FUNCTION jobs_default_remaining();

-- ---------------------------------------------------------------------
-- 4. What a job currently holds
-- ---------------------------------------------------------------------
CREATE TABLE Allocations (
    alloc_id     SERIAL PRIMARY KEY,
    job_id       INT NOT NULL REFERENCES Jobs(job_id)     ON DELETE CASCADE,
    machine_id   INT NOT NULL REFERENCES Machines(machine_id),
    alloc_cpus   INT NOT NULL CHECK (alloc_cpus   >= 0),
    alloc_ram_mb INT NOT NULL CHECK (alloc_ram_mb >= 0),
    allocated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    released_at  TIMESTAMPTZ,
    CONSTRAINT alloc_not_empty CHECK (alloc_cpus + alloc_ram_mb > 0)
);

-- ---------------------------------------------------------------------
-- 5. The block map -- external fragmentation made queryable
--
-- Invariant (checked by ValidateBlockMap): for every cluster the rows of
-- Memory_Blocks are ordered, contiguous and cover exactly [0, total_ram_mb)
-- with no gap and no overlap. state='ALLOCATED' iff alloc_id IS NOT NULL.
-- ---------------------------------------------------------------------
CREATE TABLE Memory_Blocks (
    block_id   SERIAL PRIMARY KEY,
    machine_id INT    NOT NULL REFERENCES Machines(machine_id) ON DELETE CASCADE,
    start_mb   INT    NOT NULL CHECK (start_mb >= 0),
    size_mb    INT    NOT NULL CHECK (size_mb   > 0),
    state      TEXT   NOT NULL CHECK (state IN ('FREE', 'ALLOCATED')),
    alloc_id   INT    UNIQUE REFERENCES Allocations(alloc_id) ON DELETE CASCADE,
    job_id     INT    REFERENCES Jobs(job_id) ON DELETE SET NULL,
    CONSTRAINT block_state_matches_alloc
        CHECK ((state = 'ALLOCATED') = (alloc_id IS NOT NULL)),
    CONSTRAINT block_no_overlap UNIQUE (machine_id, start_mb)
        -- Deferred: defrag rewrites block offsets in place, so two blocks can
        -- transiently share a start offset mid-transaction. The map is only
        -- required to be consistent at COMMIT, which is exactly what
        -- INITIALLY DEFERRED checks.
        DEFERRABLE INITIALLY DEFERRED
);

-- ---------------------------------------------------------------------
-- 6. Defragmentation: the migration plan and its outcome
--
-- Defrag_Events carries the before/after fragmentation numbers, so
-- "utilisation improved" is a row in the database, not a claim in a report.
-- ---------------------------------------------------------------------
CREATE TABLE Defrag_Events (
    event_id       SERIAL PRIMARY KEY,
    machine_id     INT NOT NULL REFERENCES Machines(machine_id),
    triggered_by   TEXT NOT NULL CHECK (triggered_by IN ('AUTO', 'MANUAL', 'ON_DEMAND')),
    status         TEXT NOT NULL CHECK (status IN ('RUNNING', 'COMMITTED', 'ROLLED_BACK')),
    reason         TEXT,
    holes_before   INT NOT NULL,
    largest_before_mb INT NOT NULL,
    frag_before    NUMERIC(5,4) NOT NULL,
    holes_after    INT NOT NULL,
    largest_after_mb  INT NOT NULL,
    frag_after     NUMERIC(5,4) NOT NULL,
    blocks_moved   INT NOT NULL DEFAULT 0,
    mb_moved       INT NOT NULL DEFAULT 0,
    duration_ms    INT NOT NULL DEFAULT 0,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at    TIMESTAMPTZ
);

CREATE TABLE Defrag_Moves (
    move_id      SERIAL PRIMARY KEY,
    event_id     INT NOT NULL REFERENCES Defrag_Events(event_id) ON DELETE CASCADE,
    block_id     INT NOT NULL REFERENCES Memory_Blocks(block_id)  ON DELETE CASCADE,
    from_start_mb INT NOT NULL,
    to_start_mb   INT NOT NULL,
    size_mb        INT NOT NULL,
    seq           INT NOT NULL
);

-- ---------------------------------------------------------------------
-- 7. Audit and observability
--
-- Txn_Log       one row per resource transaction: COMMIT or ROLLBACK with
--               the reason. This is the ACID evidence trail.
-- System_Events cluster-level narrative for the dashboard feed.
-- Policy_Changes why the adaptive scheduler switched policy.
-- ---------------------------------------------------------------------
CREATE TABLE Txn_Log (
    txn_id      SERIAL PRIMARY KEY,
    job_id      INT REFERENCES Jobs(job_id) ON DELETE SET NULL,
    machine_id  INT,
    op          TEXT NOT NULL CHECK (op IN ('ALLOCATE', 'RELEASE', 'DEFRAG',
                                            'DEADLOCK_ROLLBACK', 'RESET')),
    outcome     TEXT NOT NULL CHECK (outcome IN ('COMMIT', 'ROLLBACK')),
    reason      TEXT,
    duration_ms INT NOT NULL CHECK (duration_ms >= 0),
    ts          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE System_Events (
    event_id  SERIAL PRIMARY KEY,
    ts        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    kind      TEXT NOT NULL CHECK (kind IN ('ENGINE', 'POLICY', 'DEADLOCK',
                                            'DEFRAG', 'SUBMIT', 'QUOTA', 'ERROR')),
    severity  TEXT NOT NULL CHECK (severity IN ('INFO', 'WARN', 'ERROR')),
    message   TEXT NOT NULL
);

CREATE TABLE Policy_Changes (
    change_id   SERIAL PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    from_policy TEXT NOT NULL,
    to_policy   TEXT NOT NULL,
    reason      TEXT NOT NULL
);

CREATE TABLE Execution_Logs (
    log_id    SERIAL PRIMARY KEY,
    job_id    INT  NOT NULL REFERENCES Jobs(job_id) ON DELETE CASCADE,
    ts        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    log_level TEXT NOT NULL CHECK (log_level IN ('INFO', 'WARN', 'ERROR')),
    message   TEXT NOT NULL
);

-- ---------------------------------------------------------------------
-- 8. Time series for the dashboard sparklines and the evaluation
-- ---------------------------------------------------------------------
CREATE TABLE Metrics_Samples (
    sample_id      SERIAL PRIMARY KEY,
    ts             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    machine_id     INT NOT NULL REFERENCES Machines(machine_id) ON DELETE CASCADE,
    used_cores     INT NOT NULL,
    used_ram_mb    INT NOT NULL,
    free_ram_mb    INT NOT NULL,
    holes          INT NOT NULL,
    largest_free_mb INT NOT NULL,
    frag_index     NUMERIC(5,4) NOT NULL
);

CREATE TABLE Engine_Sample (
    sample_id    SERIAL PRIMARY KEY,
    ts           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    queue_depth  INT NOT NULL,
    running_jobs INT NOT NULL,
    policy       TEXT NOT NULL
);

CREATE INDEX idx_jobs_status_time ON Jobs(status, submitted_at);
CREATE INDEX idx_alloc_active     ON Allocations(machine_id) WHERE released_at IS NULL;
CREATE INDEX idx_blocks_lookup    ON Memory_Blocks(machine_id, state, size_mb);
CREATE INDEX idx_events_ts        ON System_Events(ts DESC);
CREATE INDEX idx_joblogs_ts       ON Execution_Logs(ts DESC);
CREATE INDEX idx_metrics_ts       ON Metrics_Samples(ts DESC);
CREATE INDEX idx_txn_ts           ON Txn_Log(ts DESC);

-- ---------------------------------------------------------------------
-- 9. Fragmentation as a query (one row per cluster)
--
-- frag_index = 1 - largest_free / total_free.  0.0 = all free RAM sits in
-- one block, nothing to defragment. It climbs toward 1.0 as free memory
-- shatters into pieces no job can use. total_free = 0 is defined as 0.0:
-- a full cluster is not a fragmented one.
-- ---------------------------------------------------------------------
CREATE VIEW v_cluster_status AS
SELECT m.machine_id,
       m.hostname,
       m.ip_address,
       m.status,
       c.total_cores,
       c.allocated_cores,
       c.total_cores - c.allocated_cores                    AS free_cores,
       c.clock_speed_ghz,
       r.total_ram_mb,
       r.allocated_ram_mb,
       r.total_ram_mb - r.allocated_ram_mb                  AS free_ram_mb,
       COALESCE(f.holes, 0)                                 AS holes,
       COALESCE(f.largest_free_mb, 0)                       AS largest_free_mb,
       CASE
           WHEN COALESCE(f.free_mb, 0) = 0 THEN 0
           ELSE ROUND(1.0 - COALESCE(f.largest_free_mb, 0)::NUMERIC
                          / f.free_mb, 4)
       END                                                  AS frag_index
FROM Machines m
JOIN CPU_Resources    c ON c.machine_id = m.machine_id
JOIN Memory_Resources r ON r.machine_id = m.machine_id
LEFT JOIN (
    SELECT machine_id,
           SUM(size_mb)              AS free_mb,
           COUNT(*)                  AS holes,
           MAX(size_mb)              AS largest_free_mb
    FROM Memory_Blocks
    WHERE state = 'FREE'
    GROUP BY machine_id
) f ON f.machine_id = m.machine_id;


-- =====================================================================
-- 10. The transactional core (report section 3.2, extended)
-- =====================================================================

-- Coalesce every pair of adjacent FREE blocks on a cluster until none
-- remain. Called after each release: freeing a block can make its
-- neighbours mergeable with each other, so this loops to a fixed point.
CREATE OR REPLACE FUNCTION CoalesceFreeBlocks(p_machine_id INT) RETURNS VOID AS $$
DECLARE
    v_block       INT;
    v_prev        INT;
    v_prev_start  INT;
    v_prev_sz     INT;
    v_merged      BOOLEAN := TRUE;
BEGIN
    WHILE v_merged LOOP
        v_merged := FALSE;
        SELECT b.block_id, p.block_id, p.start_mb, p.size_mb
          INTO v_block, v_prev, v_prev_start, v_prev_sz
          FROM Memory_Blocks b
          JOIN Memory_Blocks p
            ON p.machine_id = b.machine_id
           AND p.state = 'FREE'
           AND b.state = 'FREE'
           AND p.start_mb + p.size_mb = b.start_mb
         WHERE b.machine_id = p_machine_id
         ORDER BY b.start_mb
         LIMIT 1;

        IF FOUND THEN
            -- The merged span must take the PREDECESSOR's address: extending
            -- the child in place would leave the predecessor's space neither
            -- covered nor merged, i.e. an overlap and a hole at once. Delete
            -- the predecessor first so the UNIQUE(machine_id, start_mb)
            -- constraint never sees two rows at the same offset mid-update.
            DELETE FROM Memory_Blocks WHERE block_id = v_prev;
            UPDATE Memory_Blocks
               SET start_mb = v_prev_start,
                   size_mb  = size_mb + v_prev_sz
             WHERE block_id = v_block;
            v_merged := TRUE;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


-- ---------------------------------------------------------------------
-- Allocate -- one ACID unit of work.
--
-- 1. FOR UPDATE on both aggregate rows: a second transaction calling this
--    for the same cluster BLOCKS here, and at READ COMMITTED re-reads the
--    committed values when the lock clears. That closes the lost update.
-- 2. capacity check against the freshly locked values;
-- 3. best-fit contiguous hole, also locked FOR UPDATE;
-- 4. carve: the hole shrinks to the request, the remainder is inserted as
--    a new FREE block immediately after it;
-- 5. aggregates + allocation record + job state, all in the caller's
--    transaction -- commit everything or nothing.
--
-- Failure modes are raised, never returned as FALSE, so the caller's
-- transaction really does ROLLBACK:
--   INSUFFICIENT_RESOURCES -- not even aggregate capacity;
--   FRAGMENTED             -- aggregate capacity exists but no single hole
--                            fits: the caller must defragment and retry.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION AllocateJobResources(
    p_job_id     INT,
    p_machine_id INT,
    p_cpus       INT,
    p_ram        INT
) RETURNS INT AS $$
DECLARE
    v_avail_cpus  INT;
    v_avail_ram   INT;
    v_hole_id     INT;
    v_hole_start  INT;
    v_hole_size   INT;
    v_free_total  INT;
    v_holes       INT;
    v_alloc_id    INT;
BEGIN
    SELECT total_cores - allocated_cores INTO v_avail_cpus
      FROM CPU_Resources    WHERE machine_id = p_machine_id FOR UPDATE;
    SELECT total_ram_mb - allocated_ram_mb INTO v_avail_ram
      FROM Memory_Resources WHERE machine_id = p_machine_id FOR UPDATE;

    IF v_avail_cpus < p_cpus OR v_avail_ram < p_ram THEN
        RAISE EXCEPTION 'INSUFFICIENT_RESOURCES: cluster % has % cores / % MB free, job needs % cores / % MB',
            p_machine_id, v_avail_cpus, v_avail_ram, p_cpus, p_ram;
    END IF;

    -- best-fit: the SMALLEST hole that still fits, so larger holes stay
    -- available for larger jobs. Ties broken by address for determinism.
    IF p_ram > 0 THEN
        SELECT block_id, start_mb, size_mb
          INTO v_hole_id, v_hole_start, v_hole_size
          FROM Memory_Blocks
         WHERE machine_id = p_machine_id
           AND state = 'FREE'
           AND size_mb >= p_ram
         ORDER BY size_mb ASC, start_mb ASC
         LIMIT 1
         FOR UPDATE;

        IF NOT FOUND THEN
            SELECT COALESCE(SUM(size_mb), 0), COUNT(*)
              INTO v_free_total, v_holes
              FROM Memory_Blocks
             WHERE machine_id = p_machine_id AND state = 'FREE';
            RAISE EXCEPTION 'FRAGMENTED: cluster % holds % MB free across % hole(s), largest too small for % MB',
                p_machine_id, v_free_total, v_holes, p_ram;
        END IF;
    END IF;

    INSERT INTO Allocations (job_id, machine_id, alloc_cpus, alloc_ram_mb)
    VALUES (p_job_id, p_machine_id, p_cpus, p_ram)
    RETURNING alloc_id INTO v_alloc_id;

    IF p_ram > 0 THEN
        UPDATE Memory_Blocks
           SET state = 'ALLOCATED',
               alloc_id = v_alloc_id,
               job_id = p_job_id,
               size_mb = p_ram
         WHERE block_id = v_hole_id;

        IF v_hole_size > p_ram THEN
            INSERT INTO Memory_Blocks (machine_id, start_mb, size_mb, state)
            VALUES (p_machine_id, v_hole_start + p_ram, v_hole_size - p_ram, 'FREE');
        END IF;
    END IF;

    UPDATE CPU_Resources
       SET allocated_cores = allocated_cores + p_cpus
     WHERE machine_id = p_machine_id;
    UPDATE Memory_Resources
       SET allocated_ram_mb = allocated_ram_mb + p_ram
     WHERE machine_id = p_machine_id;

    UPDATE Jobs
       SET status = 'RUNNING',
           started_at = COALESCE(started_at, NOW())
     WHERE job_id = p_job_id;

    RETURN v_alloc_id;
END;
$$ LANGUAGE plpgsql;


-- Release -- the mirror image, same row locks, opposite direction.
-- Freeing RAM marks the allocation's blocks FREE and then coalesces the
-- neighbours, so free space heals itself instead of shattering further.
-- Job status is deliberately NOT touched here: completion, Round Robin
-- preemption and deadlock rollback all release resources but disagree
-- about what the job becomes next.
CREATE OR REPLACE FUNCTION ReleaseJobResources(p_job_id INT) RETURNS BOOLEAN AS $$
DECLARE
    r RECORD;
    m RECORD;
BEGIN
    -- Lock the aggregate rows FIRST -- CPU then Memory, in machine order --
    -- exactly the order AllocateJobResources takes them. Allocate holds the
    -- CPU row while it reaches for a block, so a release that grabbed a block
    -- first could deadlock against it; taking the aggregates up front puts
    -- every allocate and release on the same machine in one lock order.
    FOR m IN
        SELECT DISTINCT machine_id FROM Allocations
         WHERE job_id = p_job_id AND released_at IS NULL
         ORDER BY machine_id
    LOOP
        PERFORM 1 FROM CPU_Resources    WHERE machine_id = m.machine_id FOR UPDATE;
        PERFORM 1 FROM Memory_Resources WHERE machine_id = m.machine_id FOR UPDATE;
    END LOOP;

    FOR r IN
        SELECT alloc_id, machine_id, alloc_cpus, alloc_ram_mb
          FROM Allocations
         WHERE job_id = p_job_id AND released_at IS NULL
         FOR UPDATE
    LOOP
        UPDATE Memory_Blocks
           SET state = 'FREE', alloc_id = NULL, job_id = NULL
         WHERE alloc_id = r.alloc_id;

        UPDATE CPU_Resources
           SET allocated_cores = allocated_cores - r.alloc_cpus
         WHERE machine_id = r.machine_id;
        UPDATE Memory_Resources
           SET allocated_ram_mb = allocated_ram_mb - r.alloc_ram_mb
         WHERE machine_id = r.machine_id;
        UPDATE Allocations
           SET released_at = NOW()
         WHERE alloc_id = r.alloc_id;

        PERFORM CoalesceFreeBlocks(r.machine_id);
    END LOOP;
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


-- ---------------------------------------------------------------------
-- ValidateBlockMap -- the partition invariant, enforced on demand.
-- Tests, the API and the defragmenter all call this after mutating the
-- map. It raises BLOCK_MAP_INVALID rather than returning a boolean so a
-- broken map can never be logged as "fine".
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ValidateBlockMap(p_machine_id INT) RETURNS BOOLEAN AS $$
DECLARE
    v_total    INT;
    v_sum      INT;
    v_expected INT := 0;
    v_alloc    INT;
    rec        RECORD;
BEGIN
    SELECT total_ram_mb, allocated_ram_mb INTO v_total, v_alloc
      FROM Memory_Resources WHERE machine_id = p_machine_id FOR UPDATE;

    SELECT COALESCE(SUM(size_mb), 0) INTO v_sum
      FROM Memory_Blocks WHERE machine_id = p_machine_id;
    IF v_sum <> v_total THEN
        RAISE EXCEPTION 'BLOCK_MAP_INVALID: blocks total % MB, capacity is % MB',
            v_sum, v_total;
    END IF;

    FOR rec IN
        SELECT start_mb, size_mb FROM Memory_Blocks
         WHERE machine_id = p_machine_id
         ORDER BY start_mb
    LOOP
        IF rec.start_mb <> v_expected THEN
            RAISE EXCEPTION 'BLOCK_MAP_INVALID: gap or overlap at % MB (expected %)',
                rec.start_mb, v_expected;
        END IF;
        v_expected := v_expected + rec.size_mb;
    END LOOP;

    IF v_expected <> v_total THEN
        RAISE EXCEPTION 'BLOCK_MAP_INVALID: blocks end at %, capacity is %',
            v_expected, v_total;
    END IF;

    SELECT COALESCE(SUM(size_mb), 0) INTO v_sum
      FROM Memory_Blocks
     WHERE machine_id = p_machine_id AND state = 'ALLOCATED';
    IF v_sum <> v_alloc THEN
        RAISE EXCEPTION 'BLOCK_MAP_INVALID: allocated blocks sum % MB, aggregate says % MB',
            v_sum, v_alloc;
    END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


-- ---------------------------------------------------------------------
-- ResetCluster -- one call returns every cluster to a single free block.
-- Used by the CLI (`main.py reset`), the demo scripts and the API's
-- POST /api/reset, so all three can never drift apart.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ResetCluster(p_restart_jobs_at INT DEFAULT NULL)
RETURNS VOID AS $$
BEGIN
    -- blocks first: Memory_Blocks.alloc_id cascades off Allocations, so
    -- the map must be torn down before the allocation rows go.
    DELETE FROM Memory_Blocks;
    DELETE FROM Execution_Logs;
    DELETE FROM Defrag_Moves;
    DELETE FROM Defrag_Events;
    DELETE FROM Metrics_Samples;
    DELETE FROM Engine_Sample;
    DELETE FROM Txn_Log;
    DELETE FROM System_Events;
    DELETE FROM Policy_Changes;
    DELETE FROM Allocations;
    DELETE FROM Jobs;

    UPDATE CPU_Resources    SET allocated_cores  = 0;
    UPDATE Memory_Resources SET allocated_ram_mb = 0;

    INSERT INTO Memory_Blocks (machine_id, start_mb, size_mb, state)
    SELECT machine_id, 0, total_ram_mb, 'FREE' FROM Memory_Resources;

    IF p_restart_jobs_at IS NOT NULL THEN
        PERFORM setval('jobs_job_id_seq', p_restart_jobs_at, false);
    END IF;
END;
$$ LANGUAGE plpgsql;
