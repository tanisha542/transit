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
