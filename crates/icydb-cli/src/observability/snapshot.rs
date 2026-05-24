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

use super::{call_query, endpoint_result_error};

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
    let response = decode_snapshot_report(candid_bytes.as_slice())?;

    match response {
        Ok(report) => {
            print!("{}", render_snapshot_report(&report));

            Ok(())
        }
        Err(err) => Err(endpoint_result_error(
            "snapshot",
            &target,
            SNAPSHOT_ENDPOINT.method(),
            err,
        )),
    }
}

pub(super) fn decode_snapshot_report(
    candid_bytes: &[u8],
) -> Result<Result<icydb::db::StorageReport, icydb::Error>, String> {
    Decode!(
        candid_bytes,
        Result<icydb::db::StorageReport, icydb::Error>
    )
    .map_err(|err| err.to_string())
}

pub(super) fn render_snapshot_report(report: &StorageReport) -> String {
    let mut output = String::new();
    let data_rows = report
        .storage_data()
        .iter()
        .map(|row| data_store_row(row.path(), row.entries(), row.memory_bytes()))
        .collect::<Vec<_>>();
    let index_rows = report
        .storage_index()
        .iter()
        .map(|row| {
            index_store_row(
                row.path(),
                row.entries(),
                row.user_entries(),
                row.system_entries(),
                row.memory_bytes(),
                format!("{:?}", row.state()),
            )
        })
        .collect::<Vec<_>>();
    let entity_rows = report
        .entity_storage()
        .iter()
        .map(|row| entity_storage_row(row.path(), row.store(), row.entries(), row.memory_bytes()))
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

fn data_store_row(path: &str, entries: u64, memory_bytes: u64) -> [String; 3] {
    [
        path.to_string(),
        entries.to_string(),
        memory_bytes.to_string(),
    ]
}

fn index_store_row(
    path: &str,
    entries: u64,
    user_entries: u64,
    system_entries: u64,
    memory_bytes: u64,
    state: String,
) -> [String; 6] {
    [
        path.to_string(),
        entries.to_string(),
        user_entries.to_string(),
        system_entries.to_string(),
        memory_bytes.to_string(),
        state,
    ]
}

fn entity_storage_row(path: &str, store: &str, entries: u64, memory_bytes: u64) -> [String; 4] {
    [
        path.to_string(),
        store.to_string(),
        entries.to_string(),
        memory_bytes.to_string(),
    ]
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
