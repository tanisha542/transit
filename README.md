# TransactAlloc

An **adaptive, fragmentation-aware cloud resource manager** built on PostgreSQL.

## Overview
TransactAlloc provides safe, deadlock-free cloud resource scheduling with:
- ACID resource allocation backed by PostgreSQL row-level locks
- External fragmentation tracking using block maps
- Wait-for-graph deadlock detection and Banker's algorithm avoidance
- Adaptive policy scheduling (FCFS, Priority, Round-Robin)

## Setup

### Database (Docker)
```powershell
docker run -d --name transactalloc-pg -e POSTGRES_PASSWORD=crm -e POSTGRES_DB=transactalloc_p3 -p 5432:5432 postgres:17
```

Connection defaults live in `db.py`: `host=localhost port=5432 user=postgres password=crm dbname=transactalloc_p3`.
