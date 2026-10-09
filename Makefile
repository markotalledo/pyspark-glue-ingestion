JAVA_HOME ?= $(shell /usr/libexec/java_home -v 17 2>/dev/null || echo /opt/homebrew/opt/openjdk@17)
export JAVA_HOME

.PHONY: setup test lint run validate deploy destroy

setup:
	uv sync

test:
	uv run pytest -q

lint:
	uv run ruff check .
	uv run ruff format --check .
	terraform -chdir=terraform fmt -check -recursive

run:
	uv run python -m clean_events.job --raw_path tests/fixtures/raw --clean_path data/clean --run_date 2026-01-03 --lookback_days 2

validate:
	terraform -chdir=terraform init -backend=false -input=false
	terraform -chdir=terraform validate

deploy:
	terraform -chdir=terraform init -input=false
	terraform -chdir=terraform apply

destroy:
	terraform -chdir=terraform destroy
