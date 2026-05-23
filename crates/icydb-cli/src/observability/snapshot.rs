//! Module: snapshot command handling.
//! Responsibility: call the generated storage snapshot endpoint and render storage reports.
//! Does not own: stable-memory inspection, config surface gating, or generic ICP command construction.
//! Boundary: exposes the snapshot command and test-covered report rendering through observability.

use candid::Decode;
use icydb::db::StorageReport;

use crate::{
    cli::CanisterTarget,
    config::{SNAPSHOT_ENDPOINT, require_configured_endpoint},
    icp::require_created_canister,
};

use super::{call_query, render::table_width};

/// Read and print the generated storage snapshot endpoint.
pub(crate) fn run_snapshot_command(target: CanisterTarget) -> Result<(), String> {
    require_configured_endpoint(target.canister_name(), SNAPSHOT_ENDPOINT)?;
    require_created_canister(target.environment(), target.canister_name())?;
    let candid_bytes = call_query(
        target.environment(),
        target.canister_name(),
        SNAPSHOT_ENDPOINT.method(),
        "()",
    )?;
    let response = Decode!(
        candid_bytes.as_slice(),
        Result<icydb::db::StorageReport, icydb::Error>
    )
    .map_err(|err| err.to_string())?;

    match response {
        Ok(report) => {
            print!("{}", render_snapshot_report(&report));

            Ok(())
        }
        Err(err) => Err(format!(
            "IcyDB snapshot method '{}' failed on canister '{}' in environment '{}': {err}",
            SNAPSHOT_ENDPOINT.method(),
            target.canister_name(),
            target.environment(),
        )),
    }
}

pub(super) fn render_snapshot_report(report: &StorageReport) -> String {
    let mut output = String::new();
    let data_rows = report
        .storage_data()
        .iter()
        .map(|row| {
            (
                row.path(),
                row.entries().to_string(),
                row.memory_bytes().to_string(),
            )
        })
        .collect::<Vec<_>>();
    let index_rows = report
        .storage_index()
        .iter()
        .map(|row| {
            (
                row.path(),
                row.entries().to_string(),
                row.user_entries().to_string(),
                row.system_entries().to_string(),
                row.memory_bytes().to_string(),
                format!("{:?}", row.state()),
            )
        })
        .collect::<Vec<_>>();
    let entity_rows = report
        .entity_storage()
        .iter()
        .map(|row| {
            (
                row.path(),
                row.store(),
                row.entries().to_string(),
                row.memory_bytes().to_string(),
            )
        })
        .collect::<Vec<_>>();

    output.push_str("IcyDB storage snapshot\n");
    output.push_str(
        format!(
            "  data stores: {}\n  index stores: {}\n  entities: {}\n  corrupted keys: {}\n  corrupted entries: {}\n",
            report.storage_data().len(),
            report.storage_index().len(),
            report.entity_storage().len(),
            report.corrupted_keys(),
            report.corrupted_entries(),
        )
        .as_str(),
    );
    output.push('\n');
    append_data_store_table(&mut output, data_rows.as_slice());
    output.push('\n');
    append_index_store_table(&mut output, index_rows.as_slice());
    output.push('\n');
    append_entity_table(&mut output, entity_rows.as_slice());

    output
}

fn append_data_store_table(output: &mut String, rows: &[(&str, String, String)]) {
    output.push_str("data stores\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    let path_width = table_width("path", rows.iter().map(|(path, _, _)| *path));
    let entries_width = table_width(
        "entries",
        rows.iter().map(|(_, entries, _)| entries.as_str()),
    );
    let bytes_width = table_width("bytes", rows.iter().map(|(_, _, bytes)| bytes.as_str()));
    output.push_str(
        format!(
            "  {path:<path_width$}  {entries:>entries_width$}  {bytes:>bytes_width$}\n",
            path = "path",
            entries = "entries",
            bytes = "bytes",
        )
        .as_str(),
    );
    for (path, entries, bytes) in rows {
        output.push_str(
            format!("  {path:<path_width$}  {entries:>entries_width$}  {bytes:>bytes_width$}\n")
                .as_str(),
        );
    }
}

fn append_index_store_table(
    output: &mut String,
    rows: &[(&str, String, String, String, String, String)],
) {
    output.push_str("index stores\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    let path_width = table_width("path", rows.iter().map(|(path, _, _, _, _, _)| *path));
    let entries_width = table_width(
        "entries",
        rows.iter().map(|(_, entries, _, _, _, _)| entries.as_str()),
    );
    let user_width = table_width(
        "user",
        rows.iter().map(|(_, _, user, _, _, _)| user.as_str()),
    );
    let system_width = table_width(
        "system",
        rows.iter().map(|(_, _, _, system, _, _)| system.as_str()),
    );
    let bytes_width = table_width(
        "bytes",
        rows.iter().map(|(_, _, _, _, bytes, _)| bytes.as_str()),
    );
    let state_width = table_width(
        "state",
        rows.iter().map(|(_, _, _, _, _, state)| state.as_str()),
    );
    output.push_str(
        format!(
            "  {path:<path_width$}  {entries:>entries_width$}  {user:>user_width$}  {system:>system_width$}  {bytes:>bytes_width$}  {state:<state_width$}\n",
            path = "path",
            entries = "entries",
            user = "user",
            system = "system",
            bytes = "bytes",
            state = "state",
        )
        .as_str(),
    );
    for (path, entries, user, system, bytes, state) in rows {
        output.push_str(
            format!(
                "  {path:<path_width$}  {entries:>entries_width$}  {user:>user_width$}  {system:>system_width$}  {bytes:>bytes_width$}  {state:<state_width$}\n"
            )
            .as_str(),
        );
    }
}

fn append_entity_table(output: &mut String, rows: &[(&str, &str, String, String)]) {
    output.push_str("entities\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    let entity_width = table_width("entity", rows.iter().map(|(entity, _, _, _)| *entity));
    let store_width = table_width("store", rows.iter().map(|(_, store, _, _)| *store));
    let entries_width = table_width(
        "entries",
        rows.iter().map(|(_, _, entries, _)| entries.as_str()),
    );
    let bytes_width = table_width("bytes", rows.iter().map(|(_, _, _, bytes)| bytes.as_str()));
    output.push_str(
        format!(
            "  {entity:<entity_width$}  {store:<store_width$}  {entries:>entries_width$}  {bytes:>bytes_width$}\n",
            entity = "entity",
            store = "store",
            entries = "entries",
            bytes = "bytes",
        )
        .as_str(),
    );
    for (entity, store, entries, bytes) in rows {
        output.push_str(
            format!(
                "  {entity:<entity_width$}  {store:<store_width$}  {entries:>entries_width$}  {bytes:>bytes_width$}\n"
            )
            .as_str(),
        );
    }
}
