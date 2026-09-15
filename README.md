# TransactAlloc

An **adaptive, fragmentation-aware cloud resource manager** built on PostgreSQL.

## Overview
TransactAlloc provides safe, deadlock-free cloud resource scheduling with:
- ACID resource allocation backed by PostgreSQL row-level locks
- External fragmentation tracking using block maps
- Wait-for-graph deadlock detection and Banker's algorithm avoidance
- Adaptive policy scheduling (FCFS, Priority, Round-Robin)
