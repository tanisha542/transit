-- =====================================================================
-- TransactAlloc seed -- initial cluster configuration
-- =====================================================================

TRUNCATE Memory_Resources, CPU_Resources, Machines, Users RESTART IDENTITY CASCADE;

INSERT INTO Users (username, role, quota_cpus, quota_ram_mb) VALUES
    ('admin',   'Admin', 64, 262144),
    ('Archita', 'User',  32, 131072),
    ('student', 'User',  16,  65536);

INSERT INTO Machines (hostname, ip_address, status) VALUES
    ('cluster-alpha', '192.168.1.101', 'ONLINE'),
    ('cluster-beta',  '192.168.1.102', 'ONLINE'),
    ('cluster-gamma', '192.168.1.103', 'ONLINE'),
    ('cluster-delta', '192.168.1.104', 'ONLINE');

INSERT INTO CPU_Resources (machine_id, total_cores, allocated_cores, clock_speed_ghz) VALUES
    (1, 20, 0, 3.20),
    (2, 20, 0, 2.80),
    (3, 16, 0, 3.40),
    (4, 48, 0, 2.60);

INSERT INTO Memory_Resources (machine_id, total_ram_mb, allocated_ram_mb, swap_mb) VALUES
    (1,  65536, 0,  8192),
    (2,  65536, 0, 16384),
    (3,  32768, 0,  4096),
    (4, 131072, 0, 32768);

INSERT INTO Memory_Blocks (machine_id, start_mb, size_mb, state)
SELECT machine_id, 0, total_ram_mb, 'FREE' FROM Memory_Resources;
