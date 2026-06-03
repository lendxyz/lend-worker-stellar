# Lend on-chain worker for Stellar

This repository is an adaptation of the EVM lend chain indexer built for the Stellar ecosystem

## Testing

```bash
make test       # spin up a throwaway Postgres, run the full suite, tear it down
make test-unit  # run without a DB (the repository_db tests skip themselves)
```

`make test` starts the `test-db` service from `docker-compose.test.yml`
(Postgres on host port `55432`, in-memory data) and sets `TEST_DATABASE_URL`
for the run. The repository round-trip tests apply `tests/sql/schema.sql` to
that throwaway database; everything is torn down afterwards (the test exit code
is preserved). CI runs the same suite against a Postgres service container.

To manage the DB by hand (e.g. to debug a failing test):

```bash
make test-db-up
TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55432/lend_test cargo test
make test-db-down
```
