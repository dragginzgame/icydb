//! Module: default `icydb.toml` rendering.
//! Responsibility: render the config template produced by `icydb config init`.
//! Does not own: placement, file writes, or config validation.
//! Boundary: converts parsed init flags into TOML text.

use crate::cli::ConfigInitArgs;

pub(super) fn render_default_config(args: &ConfigInitArgs) -> Result<String, String> {
    validate_canister_key(args.canister_name())?;
    let update = args.update_config_value();
    Ok(format!(
        r#"[canisters.{canister}.sql]
readonly = {readonly}
ddl = {ddl}
fixtures = {fixtures}
update = {update}

[canisters.{canister}.sql.introspection]
local = true
ic = false

[canisters.{canister}.metrics]
local = "{metrics_local}"
ic = "{metrics_ic}"

[canisters.{canister}.snapshot]
enabled = {snapshot}

[canisters.{canister}.schema]
enabled = {schema}
"#,
        canister = args.canister_name(),
        readonly = args.readonly(),
        ddl = args.ddl(),
        fixtures = args.fixtures(),
        update = update,
        metrics_local = args.metrics_local_config_value(),
        metrics_ic = args.metrics_ic_config_value(),
        snapshot = args.snapshot(),
        schema = args.schema(),
    ))
}

fn validate_canister_key(canister: &str) -> Result<(), String> {
    if is_snake_canister_name(canister) {
        return Ok(());
    }

    Err(format!(
        "canister '{canister}' cannot be rendered in icydb.toml; use lower snake_case ASCII"
    ))
}

fn is_snake_canister_name(canister: &str) -> bool {
    let mut bytes = canister.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };

    first.is_ascii_lowercase()
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}
