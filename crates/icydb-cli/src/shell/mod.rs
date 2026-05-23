mod call;
mod input;
mod interactive;
mod perf;
mod render;
mod route;

use std::path::PathBuf;

use candid::Decode;
use icydb::db::sql::SqlQueryResult;

use crate::{
    cli::SqlArgs,
    config::{SQL_DDL_ENDPOINT, SQL_QUERY_ENDPOINT, require_configured_endpoint},
    icp::require_created_canister,
    shell::render::{ShellSqlQueryPerfResult, render_shell_text_from_perf_result},
};

#[cfg(test)]
pub(crate) use crate::shell::{
    perf::{
        ShellPerfAttribution, ShellPerfAttributionInput, normalize_grouped_next_cursor_json,
        parse_perf_result, render_perf_suffix,
    },
    render::{
        finalize_successful_command_output, render_grouped_shell_text, render_projection_shell_text,
    },
    route::SqlShellCallKind,
};

#[cfg(test)]
pub(crate) fn drain_complete_shell_statements(
    statement: &mut String,
) -> std::collections::VecDeque<String> {
    input::drain_complete_shell_statements(statement)
}

#[cfg(test)]
pub(crate) fn is_shell_help_command(input: &str) -> bool {
    input::is_shell_help_command(input)
}

#[cfg(test)]
pub(crate) fn normalize_shell_statement_line(line: &str) -> String {
    input::normalize_shell_statement_line(line)
}

#[cfg(test)]
pub(crate) const fn shell_help_text() -> &'static str {
    input::shell_help_text()
}

#[cfg(test)]
pub(crate) fn sql_shell_call_kind(sql: &str) -> SqlShellCallKind {
    route::sql_shell_call_kind(sql)
}

#[cfg(test)]
pub(crate) fn sql_error_with_recovery_hint(
    error: &str,
    environment: &str,
    canister: &str,
) -> String {
    call::sql_error_with_recovery_hint(error, environment, canister)
}

///
/// ShellConfig
///
/// ShellConfig carries the small amount of runtime configuration needed by the
/// dev SQL shell binary.
///

pub(crate) struct ShellConfig {
    canister: String,
    environment: String,
    history_file: PathBuf,
    sql: Option<String>,
}

impl ShellConfig {
    pub(crate) fn from_sql_args(args: SqlArgs) -> Self {
        let sql = args
            .sql
            .or_else(|| (!args.trailing_sql.is_empty()).then(|| args.trailing_sql.join(" ")));
        Self {
            canister: args.canister,
            environment: args.environment,
            history_file: args.history_file,
            sql,
        }
    }

    #[cfg(test)]
    pub(crate) fn canister(&self) -> &str {
        self.canister.as_str()
    }

    #[cfg(test)]
    pub(crate) fn environment(&self) -> &str {
        self.environment.as_str()
    }

    #[cfg(test)]
    pub(crate) fn history_file(&self) -> &std::path::Path {
        self.history_file.as_path()
    }

    #[cfg(test)]
    pub(crate) fn sql(&self) -> Option<&str> {
        self.sql.as_deref()
    }
}

/// Run a one-shot SQL statement or the interactive SQL shell.
pub(crate) fn run_sql_command(args: SqlArgs) -> Result<(), String> {
    let config = ShellConfig::from_sql_args(args);

    if let Some(sql) = config.sql {
        let output = execute_sql(
            config.environment.as_str(),
            config.canister.as_str(),
            sql.as_str(),
        )?;
        print!(
            "{}",
            render::finalize_successful_command_output(output.as_str())
        );
    } else {
        require_created_canister(config.environment.as_str(), config.canister.as_str())?;
        interactive::run_interactive_shell(&config)?;
    }

    Ok(())
}

fn execute_sql(environment: &str, canister: &str, sql: &str) -> Result<String, String> {
    let call_kind = route::sql_shell_call_kind(sql);
    let endpoint = match call_kind {
        route::SqlShellCallKind::Query => SQL_QUERY_ENDPOINT,
        route::SqlShellCallKind::Ddl => SQL_DDL_ENDPOINT,
    };
    require_configured_endpoint(canister, endpoint)?;
    require_created_canister(environment, canister)?;

    let escaped_sql = call::candid_escape_string(sql);
    match call_kind {
        route::SqlShellCallKind::Query => {
            let candid_bytes = call::icp_query(
                environment,
                canister,
                endpoint.method(),
                escaped_sql.as_str(),
            )?;
            let response = Decode!(
                candid_bytes.as_slice(),
                Result<ShellSqlQueryPerfResult, icydb::Error>
            )
            .map_err(|err| err.to_string())?;

            match response {
                Ok(result) => Ok(render_shell_text_from_perf_result(result)),
                Err(err) => Ok(format!(
                    "ERROR: {}",
                    call::sql_error_with_recovery_hint(&err.to_string(), environment, canister)
                )),
            }
        }
        route::SqlShellCallKind::Ddl => {
            let candid_bytes = call::icp_update(
                environment,
                canister,
                endpoint.method(),
                escaped_sql.as_str(),
            )?;
            let response = Decode!(candid_bytes.as_slice(), Result<SqlQueryResult, icydb::Error>)
                .map_err(|err| err.to_string())?;

            match response {
                Ok(result) => Ok(result.render_text()),
                Err(err) => Ok(format!(
                    "ERROR: {}",
                    call::sql_error_with_recovery_hint(&err.to_string(), environment, canister)
                )),
            }
        }
    }
}
