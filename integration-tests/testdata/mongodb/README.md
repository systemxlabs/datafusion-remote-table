# MongoDB Test Fixture

How to set up a MongoDB integration-test environment.

Everything is provided by `docker-compose.yaml` and the `mongodb_init.js` seed
script; no manual installation is needed beyond a working Docker daemon.

## Run the tests

```bash
# From the project root
cargo test --package integration-tests --test mongodb
```

`integration-tests::setup_mongodb_db()` starts the compose project, then waits
until the server answers `ping` on `mongodb://root:password@127.0.0.1:27017`
(`authSource=admin`, database `test`).

The project name is `mongodb` and the published port is `27017`; make sure both
are free before running the tests.

## Fixture

`mongodb_init.js` runs from `/docker-entrypoint-initdb.d` when the container
first initializes. The test setup always starts from a fresh volume
(`docker compose down -v` before `up`), so the seed data is deterministic.

| Collection | Contents |
|---|---|
| `simple_table` | `_id` / `id` / `name`, 3 documents |
| `supported_data_types` | one document with a value for every supported BSON type, one document that is null everywhere |
| `insert_supported_data_types` | empty; the insert tests declare the schema explicitly |

## Troubleshooting

### Port 27017 already in use

```bash
docker ps --filter "publish=27017"
```

Stop the conflicting container, or change the published port in
`docker-compose.yaml` together with `MONGODB_URI` in `integration-tests/src/lib.rs`.

### Container does not become healthy

```bash
cd integration-tests/testdata/mongodb
docker compose -p mongodb logs
```

The healthcheck runs `db.adminCommand('ping')` through `mongosh` inside the
container, so a failing healthcheck means the server itself did not start.
