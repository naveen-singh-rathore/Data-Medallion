.PHONY: db-up db-down db-reset db-logs db-shell migrate makemigration

PYTHON ?= python

db-up:
	docker compose up -d

db-down:
	docker compose down

db-reset:
	docker compose down -v

db-logs:
	docker compose logs -f db

db-shell:
	docker compose exec db sh -c 'exec psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

migrate:
	$(PYTHON) -m alembic upgrade head

makemigration:
	@test -n "$(name)" || (echo "Usage: make makemigration name=describe_change"; exit 1)
	$(PYTHON) -m alembic revision --autogenerate -m "$(name)"
