//! Module: default `icydb.toml` rendering.
//! Responsibility: render the config template produced by `icydb config init`.
//! Does not own: placement, file writes, or config validation.
//! Boundary: converts parsed init flags into TOML text.

use crate::cli::ConfigInitArgs;

pub(super) fn render_default_config(args: &ConfigInitArgs) -> Result<String, String> {
    validate_canister_key(args.canister_name())?;
    let update = args.update_config_value();
    Ok(format!(
        "\
[canisters.{canister}.sql]
readonly = {readonly}
ddl = {ddl}
fixtures = {fixtures}
update = {update}

[canisters.{canister}.sql.introspection]
local = true
ic = false

[canisters.{canister}.metrics]
enabled = {metrics}
extended = {metrics_extended}

[canisters.{canister}.snapshot]
enabled = {snapshot}

[canisters.{canister}.schema]
enabled = {schema}
",
        canister = args.canister_name(),
        readonly = args.readonly(),
        ddl = args.ddl(),
        fixtures = args.fixtures(),
        update = update,
        metrics = args.metrics(),
        metrics_extended = args.metrics_extended(),
        snapshot = args.snapshot(),
        schema = args.schema(),
    ))
}

fn validate_canister_key(canister: &str) -> Result<(), String> {
    if !canister.is_empty()
        && canister
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Ok(());
    }

    Err(format!(
        "canister '{canister}' cannot be rendered as an icydb.toml bare key; use ASCII letters, digits, '_' or '-'"
    ))
}
