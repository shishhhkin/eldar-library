default:
    @just --list

sync:
    uv sync

run:
    uv run python -m src.main

test *args:
    uv run pytest {{args}}

lint:
    uv run ruff check src tests

fmt:
    uv run ruff format src tests

fmt-check:
    uv run ruff format --check src tests

types:
    uv run --with mypy mypy --explicit-package-bases src/ tests/

check: lint fmt-check types test

migrate:
    uv run alembic upgrade head

downgrade rev='-1':
    uv run alembic downgrade {{rev}}

revision message:
    uv run alembic revision --autogenerate -m "{{message}}"

up:
    docker compose up --build

down:
    docker compose down

db:
    docker compose up -d postgres
