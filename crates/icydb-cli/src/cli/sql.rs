//! Module: CLI SQL command arguments.
//! Responsibility: define SQL shell and one-shot SQL clap surface.
//! Does not own: SQL execution, shell rendering, or command dispatch.
//! Boundary: exposes parsed SQL command fields to the shell owner.

use std::path::PathBuf;

use clap::{Args, ValueHint};

use crate::cli::{DEFAULT_ENVIRONMENT, ICP_ENVIRONMENT_ENV};

const SQL_HISTORY_FILE: &str = ".cache/sql_history";

pub(crate) struct SqlShellFields {
    pub(crate) canister: String,
    pub(crate) environment: String,
    pub(crate) history_file: PathBuf,
    pub(crate) sql: Option<String>,
    pub(crate) trailing_sql: Vec<String>,
}

///
/// SqlArgs
///
/// SqlArgs owns the SQL shell command surface. It preserves the interactive
/// shell, explicit `--sql`, ICP environment defaults, and trailing SQL
/// convenience form while keeping SQL-specific flags under the `sql` keyword.
///

#[derive(Args, Debug)]
#[command(
    trailing_var_arg = true,
    after_help = "Examples:
  icydb sql -c demo_rpg
  icydb sql -c demo_rpg --sql \"SELECT name FROM character LIMIT 5\"
  icydb sql -c demo_rpg --sql \"CREATE INDEX character_renown_idx ON character (renown)\"
  icydb sql -c demo_rpg --sql \"DROP INDEX character_renown_idx ON character\""
)]
pub(crate) struct SqlArgs {
    /// Target ICP canister name.
    #[arg(short, long, value_name = "CANISTER")]
    canister: String,

    /// Target icp-cli environment.
    #[arg(
        short,
        long,
        env = ICP_ENVIRONMENT_ENV,
        default_value = DEFAULT_ENVIRONMENT,
        value_name = "ENV"
    )]
    environment: String,

    /// Interactive shell history file.
    #[arg(long, default_value = SQL_HISTORY_FILE, value_hint = ValueHint::FilePath)]
    history_file: PathBuf,

    /// Execute one SQL statement, including supported DDL, and exit.
    #[arg(long, conflicts_with = "trailing_sql", value_name = "SQL")]
    sql: Option<String>,

    /// SQL statement, including supported DDL, passed without --sql.
    #[arg(value_name = "SQL", allow_hyphen_values = true)]
    trailing_sql: Vec<String>,
}

impl SqlArgs {
    pub(crate) fn into_shell_fields(self) -> SqlShellFields {
        SqlShellFields {
            canister: self.canister,
            environment: self.environment,
            history_file: self.history_file,
            sql: self.sql,
            trailing_sql: self.trailing_sql,
        }
    }
}
