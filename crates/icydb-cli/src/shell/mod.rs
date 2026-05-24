//! Module: SQL shell command integration.
//! Responsibility: run one-shot SQL and interactive shell flows against configured canisters.
//! Does not own: CLI parsing, endpoint configuration persistence, or SQL execution semantics.
//! Boundary: routes SQL to configured query/update endpoints and renders shell-facing output.

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
    cli::{SqlArgs, SqlShellFields},
    config::{
        ConfiguredEndpoint, SQL_DDL_ENDPOINT, SQL_QUERY_ENDPOINT, require_configured_endpoint,
    },
    icp::require_created_canister,
    shell::render::{ShellSqlQueryPerfResult, render_shell_text_from_perf_result},
};

///
/// ShellConfig
///
/// ShellConfig carries the small amount of runtime configuration needed by the
/// dev SQL shell binary.
///

struct ShellConfig {
    canister: String,
    environment: String,
    history_file: PathBuf,
    sql: Option<String>,
}

impl ShellConfig {
    fn from_sql_args(args: SqlArgs) -> Self {
        let SqlShellFields {
            canister,
            environment,
            history_file,
            sql,
            trailing_sql,
        } = args.into_shell_fields();
        let sql = sql.or_else(|| (!trailing_sql.is_empty()).then(|| trailing_sql.join(" ")));
        Self {
            canister,
            environment,
            history_file,
            sql,
        }
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
    let call_kind = route::sql_shell_call_kind(sql)?;
    let endpoint = sql_endpoint(call_kind);
    require_configured_endpoint(canister, endpoint)?;
    require_created_canister(environment, canister)?;

    let escaped_sql = call::candid_escape_string(sql);
    match call_kind {
        route::SqlShellCallKind::Query => {
            execute_sql_query(environment, canister, endpoint, &escaped_sql)
        }
        route::SqlShellCallKind::Ddl => {
            execute_sql_ddl(environment, canister, endpoint, &escaped_sql)
        }
    }
}

const fn sql_endpoint(call_kind: route::SqlShellCallKind) -> ConfiguredEndpoint {
    match call_kind {
        route::SqlShellCallKind::Query => SQL_QUERY_ENDPOINT,
        route::SqlShellCallKind::Ddl => SQL_DDL_ENDPOINT,
    }
}

fn execute_sql_query(
    environment: &str,
    canister: &str,
    endpoint: ConfiguredEndpoint,
    escaped_sql: &str,
) -> Result<String, String> {
    let candid_bytes = call::icp_query(environment, canister, endpoint.method(), escaped_sql)?;
    let response = Decode!(
        candid_bytes.as_slice(),
        Result<ShellSqlQueryPerfResult, icydb::Error>
    )
    .map_err(|err| err.to_string())?;

    match response {
        Ok(result) => Ok(render_shell_text_from_perf_result(result)),
        Err(err) => Ok(render_sql_error(err, environment, canister)),
    }
}

fn execute_sql_ddl(
    environment: &str,
    canister: &str,
    endpoint: ConfiguredEndpoint,
    escaped_sql: &str,
) -> Result<String, String> {
    let candid_bytes = call::icp_update(environment, canister, endpoint.method(), escaped_sql)?;
    let response = Decode!(candid_bytes.as_slice(), Result<SqlQueryResult, icydb::Error>)
        .map_err(|err| err.to_string())?;

    match response {
        Ok(result) => Ok(result.render_text()),
        Err(err) => Ok(render_sql_error(err, environment, canister)),
    }
}

fn render_sql_error(err: icydb::Error, environment: &str, canister: &str) -> String {
    format!(
        "ERROR: {}",
        call::sql_error_with_recovery_hint(&err.to_string(), environment, canister)
    )
}

#[cfg(test)]
pub(crate) mod test_support {
    pub(crate) use super::{perf::ShellPerfAttribution, route::SqlShellCallKind};

    pub(crate) type SqlConfigParts = (String, String, std::path::PathBuf, Option<String>);

    pub(crate) fn drain_complete_shell_statements(
        statement: &mut String,
    ) -> std::collections::VecDeque<String> {
        super::input::drain_complete_shell_statements(statement)
    }

    pub(crate) fn is_shell_help_command(input: &str) -> bool {
        super::input::is_shell_help_command(input)
    }

    pub(crate) fn interactive_start_message(environment: &str, canister: &str) -> String {
        super::interactive::interactive_start_message(environment, canister)
    }

    pub(crate) fn normalize_shell_statement_line(line: &str) -> String {
        super::input::normalize_shell_statement_line(line)
    }

    pub(crate) fn normalize_grouped_next_cursor_json(value: &mut serde_json::Value) {
        super::perf::normalize_grouped_next_cursor_json(value);
    }

    pub(crate) fn parse_perf_result(
        value: &serde_json::Value,
    ) -> Result<(icydb::db::sql::SqlQueryResult, ShellPerfAttribution), String> {
        super::perf::parse_perf_result(value)
    }

    pub(crate) fn render_perf_suffix(attribution: Option<&ShellPerfAttribution>) -> Option<String> {
        super::perf::render_perf_suffix(attribution)
    }

    pub(crate) const fn shell_perf_attribution(
        total: u64,
        compiler: u64,
        planner: u64,
        store: u64,
        executor: u64,
        decode: u64,
    ) -> ShellPerfAttribution {
        super::perf::ShellPerfAttribution::new(super::perf::ShellPerfAttributionInput {
            total,
            planner,
            store,
            executor,
            pure_covering_decode: 0,
            pure_covering_row_assembly: 0,
            decode,
            compiler,
        })
    }

    pub(crate) const fn shell_help_text() -> &'static str {
        super::input::shell_help_text()
    }

    pub(crate) fn sql_shell_call_kind(sql: &str) -> Result<SqlShellCallKind, String> {
        super::route::sql_shell_call_kind(sql)
    }

    pub(crate) fn sql_error_with_recovery_hint(
        error: &str,
        environment: &str,
        canister: &str,
    ) -> String {
        super::call::sql_error_with_recovery_hint(error, environment, canister)
    }

    pub(crate) fn candid_escape_string(sql: &str) -> String {
        super::call::candid_escape_string(sql)
    }

    pub(crate) fn finalize_successful_command_output(rendered: &str) -> String {
        super::render::finalize_successful_command_output(rendered)
    }

    pub(crate) fn render_grouped_shell_text(
        rows: icydb::db::sql::SqlGroupedRowsOutput,
        attribution: Option<ShellPerfAttribution>,
    ) -> String {
        super::render::render_grouped_shell_text(rows, attribution, None)
    }

    pub(crate) fn render_projection_shell_text(
        rows: icydb::db::sql::SqlQueryRowsOutput,
        attribution: Option<ShellPerfAttribution>,
    ) -> String {
        super::render::render_projection_shell_text(rows, attribution, None)
    }

    pub(crate) fn sql_config_parts(args: super::SqlArgs) -> SqlConfigParts {
        let config = super::ShellConfig::from_sql_args(args);

        (
            config.canister,
            config.environment,
            config.history_file,
            config.sql,
        )
    }
}
