use serde_json::Value;

use crate::{
    cli::DEFAULT_CANISTER,
    dfx::process::{canister_id, unreachable_daemon_hint},
};

const DFX_JSON_PATH: &str = "dfx.json";

/// Fail with IcyDB-specific setup guidance when dfx has no local canister id.
pub(crate) fn require_created_canister(canister: &str) -> Result<(), String> {
    match canister_id(canister) {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(missing_canister_message(canister)),
        Err(err) => Err(unreachable_daemon_hint(err.as_str())
            .map(str::to_string)
            .unwrap_or(err)),
    }
}

/// Read canister names from the local dfx project configuration.
pub(crate) fn known_canisters() -> Result<Vec<String>, String> {
    let contents = std::fs::read_to_string(DFX_JSON_PATH)
        .map_err(|err| format!("read {DFX_JSON_PATH}: {err}"))?;
    let value = serde_json::from_str::<Value>(contents.as_str())
        .map_err(|err| format!("parse {DFX_JSON_PATH}: {err}"))?;
    let Some(canisters) = value.get("canisters").and_then(Value::as_object) else {
        return Ok(Vec::new());
    };
    let mut names = canisters.keys().cloned().collect::<Vec<_>>();
    names.sort();

    Ok(names)
}

fn missing_canister_message(canister: &str) -> String {
    let mut message = format!("canister '{canister}' is not created in the local dfx environment.");
    if canister == DEFAULT_CANISTER {
        message.push_str(" `icydb sql` defaults to '");
        message.push_str(DEFAULT_CANISTER);
        message.push_str("' when --canister is omitted.");
    }
    if canister == DEFAULT_CANISTER {
        message.push_str(
            "\nRun `icydb demo fresh` to reinstall the default demo canister and load demo data.",
        );
    } else {
        message.push_str("\nRun `icydb demo fresh --canister ");
        message.push_str(canister);
        message.push_str("` to reinstall that canister and load demo data.");
    }
    message.push_str("\nRun `icydb canister list` to see known local canisters.");
    message.push_str(
        "\nThe CLI never starts or stops dfx; keep `dfx start` running in another terminal.",
    );

    if let Ok(canisters) = known_canisters()
        && !canisters.is_empty()
    {
        message.push_str("\nKnown canisters from dfx.json: ");
        message.push_str(canisters.join(", ").as_str());
    }

    message
}
