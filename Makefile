# Connection string for the throwaway test database (docker-compose.test.yml).
# Override on the CLI if needed: make test TEST_DATABASE_URL=postgres://...
TEST_DATABASE_URL ?= postgres://postgres:postgres@127.0.0.1:55432/lend_test

.PHONY: help dev build production clean test test-unit test-db-up test-db-down

help:
	@echo ''
	@echo 'Usage: make [TARGET] [EXTRA_ARGUMENTS]'
	@echo 'Targets:'
	@echo 'make dev: make dev for development work'
	@echo 'make build: make build container'
	@echo 'make production: docker production build'
	@echo 'make test: spin up a throwaway Postgres, run the full suite, tear it down'
	@echo 'make test-unit: run the suite without a DB (the DB tests skip)'
	@echo 'make test-db-up / test-db-down: manage the throwaway test Postgres'
	@echo 'clean: clean for all clear docker images'

dev:
	docker compose -f docker-compose-dev.yml down
	if [ ! -f .env ]; then cp .env.example .env; fi;
	docker compose -f docker-compose-dev.yml up

build:
	docker compose -f docker-compose.yml build
	docker compose -f docker-compose-dev.yml down build

production:
	docker compose -f docker-compose.yml up -d --build

clean:
	docker compose -f docker-compose.yml down -v
	docker compose -f docker-compose-dev.yml down -v
	docker system prune -a --volumes -f

test-db-up:
	docker compose -f docker-compose.test.yml up -d --wait

test-db-down:
	docker compose -f docker-compose.test.yml down

test: test-db-up
	@TEST_DATABASE_URL="$(TEST_DATABASE_URL)" cargo test --workspace --locked; \
	status=$$?; \
	docker compose -f docker-compose.test.yml down >/dev/null 2>&1; \
	exit $$status

# Suite without a database: the repository_db tests skip themselves.
test-unit:
	cargo test --workspace --locked
