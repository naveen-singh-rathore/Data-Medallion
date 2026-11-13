.PHONY: venv up down init-db test lint psql ui demo fixtures

UI_PORT ?= 8000

PYTHON ?= $(CURDIR)/.venv/bin/python

venv:
	python3.12 -m venv .venv
	$(PYTHON) -m pip install -q -r backend/requirements.txt

up:
	docker compose up -d --build --wait

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

ui:
	cd backend && $(PYTHON) -m uvicorn pipeline.web.app:app --port $(UI_PORT)

# Fresh clone to gold in one command: no API key, no UI clicks (replay mode, recorded answers in fixtures/).
demo: up
	docker compose exec -T backend python -m pipeline.cli demo
	@echo "UI: http://localhost:$(UI_PORT)"

# Re-export recorded LLM answers, category labels and the approved-rules provenance from the database.
fixtures:
	cd backend && $(PYTHON) -m pipeline.cli export-fixtures
