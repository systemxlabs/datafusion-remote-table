# MDB Test Fixture

How to set up an MDB (Microsoft Access) integration-test environment on Linux.

The `.mdb` fixture itself is fetched automatically at test time — only the
runtime ODBC driver needs to be installed manually.

## Fixture

`integration-tests::setup_mdb()` downloads
[`data/nwind.mdb`](https://github.com/mdbtools/mdbtestdata/blob/5ebf2d685ec628df72f4774b78abee96a866b837/data/nwind.mdb)
(3,002,368 bytes — the canonical Microsoft Northwind sample database) from the
[`mdbtools/mdbtestdata`](https://github.com/mdbtools/mdbtestdata) repo at a
pinned commit and caches it at `target/nwind.mdb`. The URL is pinned to a
specific commit SHA so the fixture is byte-stable across runs and across
upstream changes. Subsequent test runs reuse the cached file when its size
matches the expected 3,002,368 bytes.

mdbtools' own test suite runs against this same file, so the `MDBTools` ODBC
driver is known to be compatible with it.

The tests use only the `Shippers` (3 rows) and `Products` (77 rows) tables.

## 1. Install unixODBC

```bash
# Option A: pixi (recommended)
pixi global install unixodbc

# Option B: system package manager
sudo apt install unixodbc unixodbc-dev          # Debian/Ubuntu
sudo pacman -S unixodbc                          # Arch
```

Verify:

```bash
odbcinst -j
# Should print: unixODBC 2.3.x and config file paths
```

## 2. Install mdbtools (ODBC driver)

mdbtools provides `libmdbodbc.so`, the only MDB ODBC driver for Linux.

### Option A: apt (Debian / Ubuntu — easiest)

Recent Debian/Ubuntu ship the prebuilt ODBC driver:

```bash
sudo apt install odbc-mdbtools
# Installs the driver at:
#   /usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so
```

This is the path used by CI on `ubuntu-latest`.

### Option B: Build from source (other distros, or for a specific version)

```bash
# Clone
git clone https://github.com/mdbtools/mdbtools.git
cd mdbtools
git checkout v1.0.1

# Install build dependencies
sudo apt install autoconf automake libtool pkg-config libglib2.0-dev
# Or via pixi: pixi global install libgdal  # provides glib-2.0

# Configure (critical: point to your unixODBC installation)
autoreconf -i -f
./configure \
    --with-unixodbc=/path/to/unixodbc \    # e.g. /usr on Debian/Ubuntu
    --disable-man \
    --disable-gui \
    --disable-sql

# Build and install
make -j$(nproc)
sudo make install                          # installs to /usr/local/
sudo ldconfig                              # register /usr/local/lib with the dynamic loader
```

Build artifacts (must be on `LD_LIBRARY_PATH`, or registered via `ldconfig`):

| Library | Purpose |
|---------|---------|
| `libmdb.so.3` | MDB file reader |
| `libmdbsql.so.3` | SQL parser |
| `libmdbodbc.so` | ODBC driver (the key piece) |

## 3. Register the ODBC driver

Add to `~/.odbcinst.ini` (user) or `/etc/odbcinst.ini` (system):

```ini
[MDBTools]
Description = MDB Tools ODBC driver
Driver      = /usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so
FileUsage   = 1
UsageCount  = 1
```

The `Driver` path depends on how mdbtools was installed:

| Install method | Driver path |
|---|---|
| `apt install odbc-mdbtools` (Debian/Ubuntu) | `/usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so` |
| Built from source with default prefix       | `/usr/local/lib/libmdbodbc.so` |

Verify the driver is registered:

```bash
odbcinst -q -d
# Should list MDBTools
```

## 4. Run the tests

```bash
# From the project root
cargo test --package integration-tests --test mdb
```

What happens at test time:
1. `setup_mdb()` downloads `nwind.mdb` (~3 MB) to `target/nwind.mdb` on first
   call — subsequent calls reuse the cached file.
2. Tests connect via the `MDBTools` ODBC driver.
3. Each test registers the database as a DataFusion remote table and runs
   queries against it.

## Troubleshooting

### `SQLDriverConnect` fails with `NoDiagnostics`

1. Check that the `MDBTools` driver path in `~/.odbcinst.ini` is correct.
2. Check for missing library dependencies:
   ```bash
   ldd /usr/local/lib/libmdbodbc.so | grep "not found"
   ```
3. Verify `target/nwind.mdb` exists and is exactly 3,002,368 bytes. Delete it
   to force `setup_mdb()` to re-download.

### `libmdb.so.3: cannot open shared object file`

```bash
sudo ldconfig
# Or temporarily:
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
```

### Fixture download fails

`setup_mdb()` shells out to `curl --retry 3`. Make sure `curl` is installed and
the runner has network access to `raw.githubusercontent.com`. A broken or
partial download is auto-detected by the size check and retried on the next
test invocation.
