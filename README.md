# NoSQL Data Synchronization System

## Project Overview
This project implements a data synchronization system across three heterogeneous databases:
1. **PostgreSQL** (Relational)
2. **MongoDB** (Document)
3. **Apache Pig** (Data Processing Framework)

The system maintains consistency across these databases by:
- Tracking all operations in an operation log (oplog)
- Implementing merge functionality based on operation logs
- Supporting CRUD operations with conflict resolution

## Key Features
- **Operation Logging**: All GET/SET operations are recorded with timestamps
- **Merge Operations**: Synchronize data between any two systems
- **Conflict Resolution**: Last-write-wins strategy based on timestamps
- **Heterogeneous Support**: Works across different database technologies

## Prerequisites
- Python 3.8+
- PostgreSQL 13+
- MongoDB 4.4+
- Apache Pig 0.17+
- Required Python packages:
  ```bash
  pip install pandas psycopg2-binary pymongo