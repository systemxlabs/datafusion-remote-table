# Access Test Fixture

How to set up an Access (`.accdb`, ACE engine) integration-test environment on
Linux.

The `.accdb` fixture is fetched automatically at test time. The ODBC driver is
the same `MDBTools` driver the `.mdb` tests use, so the driver setup is shared:
see [`../mdb/README.md`](../mdb/README.md) for installing unixODBC, building
mdbtools and registering the driver.

## Fixture

`integration-tests::setup_accdb()` downloads
[`data/ASampleDatabase.accdb`](https://github.com/mdbtools/mdbtestdata/blob/5ebf2d685ec628df72f4774b78abee96a866b837/data/ASampleDatabase.accdb)
(544,768 bytes) from the same pinned [`mdbtools/mdbtestdata`](https://github.com/mdbtools/mdbtestdata)
commit as `nwind.mdb`, and caches it at `target/ASampleDatabase.accdb`. The URL
is pinned to a specific commit SHA so the fixture is byte-stable across runs and
across upstream changes. Subsequent test runs reuse the cached file when its
size matches the expected 544,768 bytes.

The file is an Access 2007 database (`ACE12`), so it exercises the ACE code path
that the JET3 `nwind.mdb` does not. mdbtools' own test suite runs against it as
well, so the `MDBTools` ODBC driver is known to be compatible with it.

The tests use the `Asset Items` table (65 rows). ACE files have a real catalog
table, so listing tables and stored queries means querying `MSysObjects` like
any other source (`Type` 1 = local table, 5 = stored query); the file's stored
queries are `qryComputerHardwareInOwnerOrder`, `qryCostsSummedByOwner` and
`qryGSTCalculations`.

## Tests

`integration-tests/tests/access.rs` runs against this fixture through
`RemoteDbType::Access` / `ConnectionOptions::Access`: basic queries over table
and query sources, filter pushdown on a Currency column, listing the tables and
stored queries through the `MSysObjects` catalog table, a physical-plan
serialization round trip, and pool state.

## Troubleshooting

`SQLDriverConnect` failures, missing driver libraries and fixture download
problems are documented in [`../mdb/README.md`](../mdb/README.md); the checks
are the same, with `target/ASampleDatabase.accdb` (544,768 bytes) as the fixture
to verify.

One driver detail worth knowing when connecting through `odbc-api`:
`libmdbodbc.so` ignores the connection-string length passed to
`SQLDriverConnect` and reads the string as a NUL-terminated C string. A caller
that passes a non-terminated buffer (a Rust `&str`, for example) has the driver
read into adjacent memory and append garbage to the `DBQ` path, which surfaces
as `NoDiagnostics` plus a `File not found` line on stderr for a file that
exists. The backend passes a `CString` to avoid it.
