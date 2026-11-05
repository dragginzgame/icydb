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
        super::RpcMethod::Query,
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
