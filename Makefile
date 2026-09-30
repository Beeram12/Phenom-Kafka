# Event ingestion service - developer workflow.
VENV       ?= .venv
PYTHON     ?= $(VENV)/bin/python
export PYTHONPATH := src

GENERATOR_ARGS ?= --rate 200 --duration 30 --batch-size 50
VERIFY_ARGS    ?= --report generator_report.json

.PHONY: help install proto up down topics run generate verify test test-unit test-integration lint format clean

help:
	@echo "install proto up down topics run generate verify test test-unit test-integration lint format clean"

install:
	uv venv --python 3.12 $(VENV) || python3 -m venv $(VENV)
	uv pip install --python $(PYTHON) -e ".[dev]" || $(PYTHON) -m pip install -e ".[dev]"

proto:
	$(PYTHON) scripts/generate_protos.py

up:
	docker compose up -d --wait

down:
	docker compose down -v

topics:
	$(PYTHON) scripts/create_topics.py

run:
	$(PYTHON) -m ingestion.main

generate:
	$(PYTHON) scripts/generate_dummy_events.py $(GENERATOR_ARGS)

verify:
	$(PYTHON) scripts/consume_and_verify.py $(VERIFY_ARGS)

test: test-unit test-integration

test-unit:
	$(PYTHON) -m pytest tests/unit

test-integration:
	$(PYTHON) -m pytest tests/integration -m integration

lint:
	$(PYTHON) -m ruff check src scripts tests
	$(PYTHON) -m ruff format --check src scripts tests
	$(PYTHON) -m mypy

format:
	$(PYTHON) -m ruff format src scripts tests
	$(PYTHON) -m ruff check --fix src scripts tests

clean:
	rm -rf .mypy_cache .ruff_cache .pytest_cache generator_report.json
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
