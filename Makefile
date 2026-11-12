.PHONY: venv up down init-db test lint psql

PYTHON ?= $(CURDIR)/.venv/bin/python

venv:
	python3.12 -m venv .venv
	$(PYTHON) -m pip install -q -r backend/requirements.txt

up:
	docker compose up -d --wait

down:
	docker compose down

init-db:
	cd backend && $(PYTHON) -m pipeline.cli init-db

test:
	cd backend && $(PYTHON) -m pytest -q

lint:
	$(PYTHON) -m ruff check backend

psql:
	docker compose exec db sh -c 'exec psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'
