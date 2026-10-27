//! Module: SQL shell command integration.
//! Responsibility: run one-shot SQL and interactive shell flows against deployed canisters.
//! Does not own: CLI parsing, endpoint publication, or SQL execution semantics.
//! Boundary: routes SQL to fixed deployed method names and renders shell-facing output.

mod call;
mod input;
mod interactive;
pub(crate) mod render;
mod route;

use std::path::PathBuf;

use canic_icydb::wire::Endpoint;
use icydb::db::sql::SqlQueryResult;

use crate::{
    cli::{SqlArgs, SqlShellFields},
    diagnostic::render_error,
    icp::{IcpCallKind, require_created_canister},
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
    require_created_canister(environment, canister)?;

    let (wire_bytes, kind) = match call_kind {
        route::SqlShellCallKind::Query => (
            call::icp_query(environment, canister, endpoint, sql)?,
            IcpCallKind::Query,
        ),
        route::SqlShellCallKind::Ddl | route::SqlShellCallKind::Update => (
            call::icp_update(environment, canister, endpoint, sql)?,
            IcpCallKind::Update,
        ),
    };

    render_sql_response(endpoint, wire_bytes.as_slice(), kind)
}

const fn sql_endpoint(call_kind: route::SqlShellCallKind) -> Endpoint {
    match call_kind {
        route::SqlShellCallKind::Query => Endpoint::Query,
        route::SqlShellCallKind::Ddl => Endpoint::Ddl,
        route::SqlShellCallKind::Update => Endpoint::Update,
    }
}

// Both call lanes decode the same public envelope and retain typed values until
// the CLI composes a terminal report from SQL display cells.
fn render_sql_response(
    endpoint: Endpoint,
    wire_bytes: &[u8],
    kind: IcpCallKind,
) -> Result<String, String> {
    let response = endpoint
        .decode_response::<Result<SqlQueryResult, icydb::Error>>(
            wire_bytes,
            icydb::wire::limits(icydb::wire::MAX_REPLY_BYTES),
        )
        .map_err(|error| {
            kind.reply_error(crate::diagnostic::render_wire_decode_error(endpoint, error))
        })?;

    match response {
        Ok(result) => Ok(render::render_text(&result)),
        // The one-shot entrypoint and interactive loop own the error prefix and
        // output stream; endpoint failures remain errors until that boundary.
        Err(err) => Err(render_error(&err)),
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    pub(crate) use super::route::SqlShellCallKind;

    pub(crate) type SqlShellConfigInputs = (String, String, std::path::PathBuf, Option<String>);

    pub(crate) fn drain_complete_shell_statements(
        statement: &mut String,
    ) -> std::collections::VecDeque<String> {
        super::input::drain_complete_shell_statements(statement)
    }

    pub(crate) fn is_shell_help_command(input: &str) -> bool {
        super::input::is_shell_help_command(input)
    }

    pub(crate) fn is_shell_exit_command(input: &str) -> bool {
        super::input::is_shell_exit_command(input)
    }

    pub(crate) fn interactive_start_message(environment: &str, canister: &str) -> String {
        super::interactive::interactive_start_message(environment, canister)
    }

    pub(crate) const fn shell_help_text() -> &'static str {
        super::input::shell_help_text()
    }

    pub(crate) fn sql_shell_call_kind(sql: &str) -> Result<SqlShellCallKind, String> {
        super::route::sql_shell_call_kind(sql)
    }

    pub(crate) fn finalize_successful_command_output(rendered: &str) -> String {
        super::render::finalize_successful_command_output(rendered)
    }

    pub(crate) fn render_sql_response(wire_bytes: &[u8]) -> Result<String, String> {
        super::render_sql_response(
            super::Endpoint::Query,
            wire_bytes,
            super::IcpCallKind::Query,
        )
    }

    pub(crate) fn sql_shell_config_inputs(args: super::SqlArgs) -> SqlShellConfigInputs {
        let config = super::ShellConfig::from_sql_args(args);

        (
            config.canister,
            config.environment,
            config.history_file,
            config.sql,
        )
    }
}
