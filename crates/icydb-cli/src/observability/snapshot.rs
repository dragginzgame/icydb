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
    table::{ColumnAlign, append_indented_table},
};

use super::call_query;

/// Read and print the generated storage snapshot endpoint.
pub(super) fn run_snapshot_command(target: CanisterTarget) -> Result<(), String> {
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
            [
                row.path().to_string(),
                row.entries().to_string(),
                row.memory_bytes().to_string(),
            ]
        })
        .collect::<Vec<_>>();
    let index_rows = report
        .storage_index()
        .iter()
        .map(|row| {
            [
                row.path().to_string(),
                row.entries().to_string(),
                row.user_entries().to_string(),
                row.system_entries().to_string(),
                row.memory_bytes().to_string(),
                format!("{:?}", row.state()),
            ]
        })
        .collect::<Vec<_>>();
    let entity_rows = report
        .entity_storage()
        .iter()
        .map(|row| {
            [
                row.path().to_string(),
                row.store().to_string(),
                row.entries().to_string(),
                row.memory_bytes().to_string(),
            ]
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

fn append_data_store_table(output: &mut String, rows: &[[String; 3]]) {
    append_snapshot_table(
        output,
        "data stores",
        &["path", "entries", "bytes"],
        rows,
        &[ColumnAlign::Left, ColumnAlign::Right, ColumnAlign::Right],
    );
}

fn append_index_store_table(output: &mut String, rows: &[[String; 6]]) {
    append_snapshot_table(
        output,
        "index stores",
        &["path", "entries", "user", "system", "bytes", "state"],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Left,
        ],
    );
}

fn append_entity_table(output: &mut String, rows: &[[String; 4]]) {
    append_snapshot_table(
        output,
        "entities",
        &["entity", "store", "entries", "bytes"],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Right,
        ],
    );
}

fn append_snapshot_table<const N: usize>(
    output: &mut String,
    title: &str,
    headers: &[&str; N],
    rows: &[[String; N]],
    alignments: &[ColumnAlign; N],
) {
    output.push_str(title);
    output.push('\n');
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    append_indented_table(output, "  ", headers, rows, alignments);
}
