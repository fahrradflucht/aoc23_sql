PSQL = /opt/homebrew/opt/libpq/bin/psql

.PHONY: run-dependencies
run-dependencies:
	docker-compose up -d

.PHONY: stop-dependencies
stop-dependencies:
	docker-compose down

.PHONY: run-day-%
run-day-%:
	@cd day$* && $(PSQL) -f solution.sql postgres://postgres:postgres@localhost:5432/postgres
