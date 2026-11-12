# Data Medallion

## Local database

The local PostgreSQL 16 database uses the `pgvector/pgvector` image and persists
its data in a named Docker volume. The checked-in `.env.example` contains local
development defaults; `.env` is ignored by Git.

1. Copy `.env.example` to `.env` if you do not already have a local `.env`.
2. Start the database and wait for its health check:

   ```sh
   make db-up
   docker compose ps
   ```

3. Install the Python dependencies and apply migrations:

   ```sh
   pip install -r requirements.txt
   make migrate
   ```

   Alembic is initialized with an empty metadata baseline; no application
   tables are defined yet. Create a migration after defining models with
   `make makemigration name=describe_change`.

Useful database commands:

| Command | Action |
| --- | --- |
| `make db-up` | Start PostgreSQL in the background |
| `make db-down` | Stop the database without removing its data |
| `make db-reset` | Stop the database and **delete its data volume** |
| `make db-logs` | Follow database logs |
| `make db-shell` | Open `psql` inside the database container |
| `make migrate` | Apply Alembic migrations |

The `db-reset` command is destructive. Run `make db-up` again afterward to
create a fresh database.

### DBeaver connection

- Database type: PostgreSQL
- Host: `localhost`
- Port: `5432` (or the value of `POSTGRES_PORT` in `.env`)
- Database: `appdb`
- Username: `dev`
- Password: `dev`

The database initializes the `vector` and `uuid-ossp` extensions on its first
startup.
