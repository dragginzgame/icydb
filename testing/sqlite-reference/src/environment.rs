//! Module: sqlite_reference::environment
//! Responsibility: bundled SQLite version, source, and compile-option identity.
//! Does not own: connection PRAGMAs or result mapping.
//! Boundary: fails required reference execution before scenarios run on environment drift.

use crate::{SqliteAdapterError, SqliteAdapterErrorKind};
use rusqlite::Connection;

///
/// SqliteEnvironmentContract
///
/// Checked-in identity required from the lockfile-pinned bundled SQLite source.
/// Dependency updates must deliberately refresh this complete contract.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SqliteEnvironmentContract {
    version: &'static str,
    version_number: i32,
    source_id: &'static str,
    compile_options: &'static [&'static str],
}

impl SqliteEnvironmentContract {
    /// Return the required SQLite runtime version string.
    #[must_use]
    pub const fn version(self) -> &'static str {
        self.version
    }

    /// Return the required SQLite numeric version identity.
    #[must_use]
    pub const fn version_number(self) -> i32 {
        self.version_number
    }

    /// Return the required SQLite source identifier and source checksum.
    #[must_use]
    pub const fn source_id(self) -> &'static str {
        self.source_id
    }

    /// Borrow the required sorted compile-option set.
    #[must_use]
    pub const fn compile_options(self) -> &'static [&'static str] {
        self.compile_options
    }
}

///
/// SqliteEnvironmentIdentity
///
/// Observed bundled SQLite runtime, compile-time numeric version, source ID,
/// and sorted compile options recorded by correctness evidence.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqliteEnvironmentIdentity {
    runtime_version: String,
    runtime_version_number: i32,
    compile_version_number: i32,
    source_id: String,
    compile_options: Vec<String>,
}

impl SqliteEnvironmentIdentity {
    /// Borrow the observed runtime version.
    #[must_use]
    pub const fn runtime_version(&self) -> &str {
        self.runtime_version.as_str()
    }

    /// Return the observed runtime numeric version.
    #[must_use]
    pub const fn runtime_version_number(&self) -> i32 {
        self.runtime_version_number
    }

    /// Return the bindings' compile-time numeric version.
    #[must_use]
    pub const fn compile_version_number(&self) -> i32 {
        self.compile_version_number
    }

    /// Borrow the observed source identifier and source checksum.
    #[must_use]
    pub const fn source_id(&self) -> &str {
        self.source_id.as_str()
    }

    /// Borrow the observed sorted compile-option set.
    #[must_use]
    pub const fn compile_options(&self) -> &[String] {
        self.compile_options.as_slice()
    }
}

// This complete sorted identity comes from the exact bundled source resolved by
// Cargo.lock. Dependency or build-environment drift must be reviewed explicitly.
const REQUIRED_SQLITE_COMPILE_OPTIONS: &[&str] = &[
    "ATOMIC_INTRINSICS=1",
    "COMPILER=gcc-13.3.0",
    "DEFAULT_AUTOVACUUM",
    "DEFAULT_CACHE_SIZE=-2000",
    "DEFAULT_FILE_FORMAT=4",
    "DEFAULT_FOREIGN_KEYS",
    "DEFAULT_JOURNAL_SIZE_LIMIT=-1",
    "DEFAULT_MMAP_SIZE=0",
    "DEFAULT_PAGE_SIZE=4096",
    "DEFAULT_PCACHE_INITSZ=20",
    "DEFAULT_RECURSIVE_TRIGGERS",
    "DEFAULT_SECTOR_SIZE=4096",
    "DEFAULT_SYNCHRONOUS=2",
    "DEFAULT_WAL_AUTOCHECKPOINT=1000",
    "DEFAULT_WAL_SYNCHRONOUS=2",
    "DEFAULT_WORKER_THREADS=0",
    "DIRECT_OVERFLOW_READ",
    "ENABLE_API_ARMOR",
    "ENABLE_COLUMN_METADATA",
    "ENABLE_DBSTAT_VTAB",
    "ENABLE_FTS3",
    "ENABLE_FTS3_PARENTHESIS",
    "ENABLE_FTS5",
    "ENABLE_LOAD_EXTENSION",
    "ENABLE_MEMORY_MANAGEMENT",
    "ENABLE_RTREE",
    "ENABLE_STAT4",
    "HAVE_ISNAN",
    "MALLOC_SOFT_LIMIT=1024",
    "MAX_ATTACHED=10",
    "MAX_COLUMN=2000",
    "MAX_COMPOUND_SELECT=500",
    "MAX_DEFAULT_PAGE_SIZE=8192",
    "MAX_EXPR_DEPTH=1000",
    "MAX_FUNCTION_ARG=1000",
    "MAX_LENGTH=1000000000",
    "MAX_LIKE_PATTERN_LENGTH=50000",
    "MAX_MMAP_SIZE=0x7fff0000",
    "MAX_PAGE_COUNT=0xfffffffe",
    "MAX_PAGE_SIZE=65536",
    "MAX_SQL_LENGTH=1000000000",
    "MAX_TRIGGER_DEPTH=1000",
    "MAX_VARIABLE_NUMBER=32766",
    "MAX_VDBE_OP=250000000",
    "MAX_WORKER_THREADS=8",
    "MUTEX_PTHREADS",
    "SOUNDEX",
    "SYSTEM_MALLOC",
    "TEMP_STORE=1",
    "THREADSAFE=1",
    "USE_URI",
];
const REQUIRED_SQLITE_ENVIRONMENT: SqliteEnvironmentContract = SqliteEnvironmentContract {
    version: "3.53.2",
    version_number: 3_053_002,
    source_id: "2026-06-03 19:12:13 d6e03d8c777cfa2d35e3b60d8ec3e0187f3e9f99d8e2ee9cac695fd6fcdf1a24",
    compile_options: REQUIRED_SQLITE_COMPILE_OPTIONS,
};

/// Return the checked-in bundled SQLite environment contract.
#[must_use]
pub const fn current_sqlite_environment_contract() -> SqliteEnvironmentContract {
    REQUIRED_SQLITE_ENVIRONMENT
}

/// Observe the bundled SQLite identity without accepting it as current.
///
/// # Errors
///
/// Returns a typed connection or environment error when SQLite cannot expose
/// its complete runtime identity.
pub fn observe_sqlite_environment() -> Result<SqliteEnvironmentIdentity, SqliteAdapterError> {
    let connection = Connection::open_in_memory().map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Connection,
            "failed to open bundled in-memory SQLite",
            source,
        )
    })?;
    observe_sqlite_environment_from_connection(&connection)
}

pub(crate) fn verify_sqlite_environment(
    connection: &Connection,
) -> Result<SqliteEnvironmentIdentity, SqliteAdapterError> {
    let identity = observe_sqlite_environment_from_connection(connection)?;
    let contract = current_sqlite_environment_contract();

    if identity.runtime_version != contract.version
        || identity.runtime_version_number != contract.version_number
        || identity.compile_version_number != contract.version_number
        || identity.source_id != contract.source_id
        || identity.compile_options
            != contract
                .compile_options
                .iter()
                .map(|option| (*option).to_string())
                .collect::<Vec<_>>()
    {
        return Err(SqliteAdapterError::new(
            SqliteAdapterErrorKind::Environment,
            format!("bundled SQLite environment drifted: {identity:#?}"),
        ));
    }

    Ok(identity)
}

fn observe_sqlite_environment_from_connection(
    connection: &Connection,
) -> Result<SqliteEnvironmentIdentity, SqliteAdapterError> {
    let source_id = connection
        .query_row("SELECT sqlite_source_id()", [], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Environment,
                "failed to read bundled SQLite source identity",
                source,
            )
        })?;
    let mut statement = connection
        .prepare("PRAGMA compile_options")
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Environment,
                "failed to prepare bundled SQLite compile-option query",
                source,
            )
        })?;
    let mut compile_options = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Environment,
                "failed to query bundled SQLite compile options",
                source,
            )
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Environment,
                "failed to decode bundled SQLite compile options",
                source,
            )
        })?;
    compile_options.sort();

    Ok(SqliteEnvironmentIdentity {
        runtime_version: rusqlite::version().to_string(),
        runtime_version_number: rusqlite::version_number(),
        compile_version_number: rusqlite::ffi::SQLITE_VERSION_NUMBER,
        source_id,
        compile_options,
    })
}
