-- =====================================================================
-- TransactAlloc - adaptive cloud resource manager
-- Core schema: users, machines, compute/memory resources, and jobs
-- =====================================================================

DROP TABLE IF EXISTS Jobs             CASCADE;
DROP TABLE IF EXISTS Memory_Resources CASCADE;
DROP TABLE IF EXISTS CPU_Resources    CASCADE;
DROP TABLE IF EXISTS Machines         CASCADE;
DROP TABLE IF EXISTS Users            CASCADE;

CREATE TABLE Users (
    user_id      SERIAL PRIMARY KEY,
    username     TEXT NOT NULL UNIQUE,
    role         TEXT NOT NULL CHECK (role IN ('Admin', 'User')),
    quota_cpus   INT  NOT NULL CHECK (quota_cpus   >= 0),
    quota_ram_mb INT  NOT NULL CHECK (quota_ram_mb >= 0)
);

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

CREATE TABLE Jobs (
    job_id        SERIAL PRIMARY KEY,
    user_id       INT  NOT NULL REFERENCES Users(user_id),
    task_name     TEXT NOT NULL,
    req_cpus      INT  NOT NULL CHECK (req_cpus   > 0),
    req_ram_mb    INT  NOT NULL CHECK (req_ram_mb > 0),
    priority      INT  NOT NULL DEFAULT 5 CHECK (priority BETWEEN 1 AND 10),
    duration_sec  INT  NOT NULL CHECK (duration_sec > 0),
    remaining_sec INT  NOT NULL DEFAULT 0 CHECK (remaining_sec >= 0),
    status        TEXT NOT NULL DEFAULT 'QUEUED'
                  CHECK (status IN ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED')),
    submitted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ
);

CREATE INDEX idx_jobs_status_time ON Jobs(status, submitted_at);

CREATE TABLE Allocations (
    alloc_id     SERIAL PRIMARY KEY,
    job_id       INT NOT NULL REFERENCES Jobs(job_id) ON DELETE CASCADE,
    machine_id   INT NOT NULL REFERENCES Machines(machine_id),
    alloc_cpus   INT NOT NULL CHECK (alloc_cpus   >= 0),
    alloc_ram_mb INT NOT NULL CHECK (alloc_ram_mb >= 0),
    allocated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    released_at  TIMESTAMPTZ,
    CONSTRAINT alloc_not_empty CHECK (alloc_cpus + alloc_ram_mb > 0)
);

CREATE TABLE Memory_Blocks (
    block_id   SERIAL PRIMARY KEY,
    machine_id INT  NOT NULL REFERENCES Machines(machine_id) ON DELETE CASCADE,
    start_mb   INT  NOT NULL CHECK (start_mb >= 0),
    size_mb    INT  NOT NULL CHECK (size_mb   > 0),
    state      TEXT NOT NULL CHECK (state IN ('FREE', 'ALLOCATED')),
    alloc_id   INT  UNIQUE REFERENCES Allocations(alloc_id) ON DELETE CASCADE,
    job_id     INT  REFERENCES Jobs(job_id) ON DELETE SET NULL,
    CONSTRAINT block_state_matches_alloc CHECK ((state = 'ALLOCATED') = (alloc_id IS NOT NULL)),
    CONSTRAINT block_no_overlap UNIQUE (machine_id, start_mb) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX idx_alloc_active  ON Allocations(machine_id) WHERE released_at IS NULL;
CREATE INDEX idx_blocks_lookup ON Memory_Blocks(machine_id, state, size_mb);

CREATE TABLE Txn_Log (
    txn_id      SERIAL PRIMARY KEY,
    job_id      INT REFERENCES Jobs(job_id) ON DELETE SET NULL,
    machine_id  INT,
    op          TEXT NOT NULL CHECK (op IN ('ALLOCATE', 'RELEASE', 'DEFRAG', 'DEADLOCK_ROLLBACK', 'RESET')),
    outcome     TEXT NOT NULL CHECK (outcome IN ('COMMIT', 'ROLLBACK')),
    reason      TEXT,
    duration_ms INT NOT NULL CHECK (duration_ms >= 0),
    ts          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE System_Events (
    event_id  SERIAL PRIMARY KEY,
    ts        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    kind      TEXT NOT NULL CHECK (kind IN ('ENGINE', 'POLICY', 'DEADLOCK', 'DEFRAG', 'SUBMIT', 'QUOTA', 'ERROR')),
    severity  TEXT NOT NULL CHECK (severity IN ('INFO', 'WARN', 'ERROR')),
    message   TEXT NOT NULL
);

CREATE TABLE Execution_Logs (
    log_id    SERIAL PRIMARY KEY,
    job_id    INT  NOT NULL REFERENCES Jobs(job_id) ON DELETE CASCADE,
    ts        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    log_level TEXT NOT NULL CHECK (log_level IN ('INFO', 'WARN', 'ERROR')),
    message   TEXT NOT NULL
);

CREATE INDEX idx_events_ts  ON System_Events(ts DESC);
CREATE INDEX idx_joblogs_ts ON Execution_Logs(ts DESC);
CREATE INDEX idx_txn_ts     ON Txn_Log(ts DESC);

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
    v_alloc_id    INT;
BEGIN
    SELECT total_cores - allocated_cores INTO v_avail_cpus
      FROM CPU_Resources WHERE machine_id = p_machine_id FOR UPDATE;
    SELECT total_ram_mb - allocated_ram_mb INTO v_avail_ram
      FROM Memory_Resources WHERE machine_id = p_machine_id FOR UPDATE;

    IF v_avail_cpus < p_cpus OR v_avail_ram < p_ram THEN
        RAISE EXCEPTION 'INSUFFICIENT_RESOURCES: cluster % has % cores / % MB free, job needs % cores / % MB',
            p_machine_id, v_avail_cpus, v_avail_ram, p_cpus, p_ram;
    END IF;

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
            RAISE EXCEPTION 'FRAGMENTED: cluster % does not have a contiguous hole for % MB',
                p_machine_id, p_ram;
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

CREATE OR REPLACE FUNCTION ReleaseJobResources(p_job_id INT) RETURNS BOOLEAN AS $$
DECLARE
    r RECORD;
    m RECORD;
BEGIN
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
