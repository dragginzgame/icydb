//! Module: snapshot report rendering.
//! Responsibility: render generated storage snapshot reports into CLI tables.
//! Does not own: endpoint calls, Candid decoding, or endpoint publication.
//! Boundary: receives decoded storage reports and returns user-facing text.

use icydb::db::{
    DataStoreSnapshot, IndexStoreSnapshot, MemoryAllocationBinding, MemoryAllocations,
    SchemaStoreSnapshot, StorageReport,
};
use std::fmt::Write as _;

use crate::table::{ColumnAlign, append_indented_table};

pub(super) fn render_snapshot_report(report: &StorageReport) -> String {
    let mut output = String::new();
    let data_rows = report
        .storage_data()
        .iter()
        .map(data_store_row)
        .collect::<Vec<_>>();
    let index_rows = report
        .storage_index()
        .iter()
        .map(index_store_row)
        .collect::<Vec<_>>();
    let schema_rows = report
        .schema_storage()
        .iter()
        .map(schema_store_row)
        .collect::<Vec<_>>();
    let entity_rows = report
        .entity_storage()
        .iter()
        .map(|row| entity_storage_row(row.path(), row.store(), row.entries(), row.memory_bytes()))
        .collect::<Vec<_>>();

    output.push_str("IcyDB storage snapshot\n");
    output.push_str(
        format!(
            "  data stores: {}\n  index stores: {}\n  schema stores: {}\n  entities: {}\n  corrupted keys: {}\n  corrupted entries: {}\n",
            report.storage_data().len(),
            report.storage_index().len(),
            report.schema_storage().len(),
            report.entity_storage().len(),
            report.corrupted_keys(),
            report.corrupted_entries(),
        )
        .as_str(),
    );
    output.push('\n');
    match report.memory_allocations() {
        Some(allocations) => append_memory_allocations(&mut output, allocations),
        None => output.push_str(
            "physical allocations (canister-wide)\n  unavailable: no default memory runtime\n",
        ),
    }
    output.push('\n');
    append_data_store_table(&mut output, data_rows.as_slice());
    output.push('\n');
    append_index_store_table(&mut output, index_rows.as_slice());
    output.push('\n');
    append_schema_store_table(&mut output, schema_rows.as_slice());
    output.push('\n');
    append_entity_table(&mut output, entity_rows.as_slice());

    output
}

fn append_memory_allocations(output: &mut String, report: &MemoryAllocations) {
    output.push_str("physical allocations (canister-wide; includes ledger)\n");
    // Formatting integers into a String is infallible and needs no temporary buffer.
    let _ = writeln!(
        output,
        "  physical bytes: {}\n  virtual bytes: {}\n  manager metadata bytes: {}\n  allocated bucket bytes: {}\n  bucket slack bytes: {}\n  known binding bytes: {}\n  unknown binding bytes: {}\n  unmanaged bytes: {}\n  bucket pages: {}\n  buckets remaining: {} / {}\n  payload occupancy: unavailable",
        report.physical_extent.bytes,
        report.virtual_extent.bytes,
        report.manager_metadata_bytes,
        report.allocated_bucket_bytes,
        report.bucket_slack_bytes,
        report.known_binding_bytes,
        report.unknown_binding_bytes,
        report.unmanaged_bytes,
        report.bucket_size_pages,
        report.remaining_buckets,
        report.bucket_capacity,
    );
    let rows = report
        .memories
        .iter()
        .map(|memory| {
            let (binding, owner, key) = match &memory.binding {
                MemoryAllocationBinding::Current { stable_key, owner } => {
                    ("current", owner.as_str(), stable_key.as_str())
                }
                MemoryAllocationBinding::Ledger { stable_key, owner } => {
                    ("ledger", owner.as_str(), stable_key.as_str())
                }
                MemoryAllocationBinding::Unknown => ("unknown", "-", "-"),
            };
            [
                memory.memory_manager_id.to_string(),
                binding.to_string(),
                owner.to_string(),
                key.to_string(),
                memory.allocated_bytes.to_string(),
                memory.virtual_extent.bytes.to_string(),
                memory.bucket_slack_bytes.to_string(),
                memory
                    .payload_bytes
                    .map_or_else(|| "unavailable".to_string(), |bytes| bytes.to_string()),
            ]
        })
        .collect::<Vec<_>>();
    append_indented_table(
        output,
        "  ",
        &[
            "id",
            "binding",
            "owner",
            "stable key",
            "allocated bytes",
            "virtual bytes",
            "bucket slack",
            "payload bytes",
        ],
        &rows,
        &[
            ColumnAlign::Right,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
        ],
    );
}

fn data_store_row(row: &DataStoreSnapshot) -> [String; 13] {
    [
        row.path().to_string(),
        row.storage().as_str().to_string(),
        row.allocation().as_str().to_string(),
        row.durability().as_str().to_string(),
        row.commit().as_str().to_string(),
        row.recovery().as_str().to_string(),
        row.schema_metadata().as_str().to_string(),
        format_optional_u8(row.memory_id()),
        row.stable_key().unwrap_or("-").to_string(),
        format_optional_u32(row.schema_version()),
        row.schema_fingerprint().unwrap_or("-").to_string(),
        row.entries().to_string(),
        row.memory_bytes().to_string(),
    ]
}

fn index_store_row(row: &IndexStoreSnapshot) -> [String; 16] {
    [
        row.path().to_string(),
        row.storage().as_str().to_string(),
        row.allocation().as_str().to_string(),
        row.durability().as_str().to_string(),
        row.commit().as_str().to_string(),
        row.recovery().as_str().to_string(),
        row.schema_metadata().as_str().to_string(),
        format_optional_u8(row.memory_id()),
        row.stable_key().unwrap_or("-").to_string(),
        format_optional_u32(row.schema_version()),
        row.schema_fingerprint().unwrap_or("-").to_string(),
        row.entries().to_string(),
        row.user_entries().to_string(),
        row.system_entries().to_string(),
        row.memory_bytes().to_string(),
        format!("{:?}", row.state()),
    ]
}

fn schema_store_row(row: &SchemaStoreSnapshot) -> [String; 12] {
    [
        row.path().to_string(),
        row.storage().as_str().to_string(),
        row.allocation().as_str().to_string(),
        row.durability().as_str().to_string(),
        row.commit().as_str().to_string(),
        row.recovery().as_str().to_string(),
        row.schema_metadata().as_str().to_string(),
        format_optional_u8(row.memory_id()),
        row.stable_key().unwrap_or("-").to_string(),
        row.schema_version()
            .map_or_else(|| "-".to_string(), |version| version.to_string()),
        row.schema_fingerprint().unwrap_or("-").to_string(),
        row.entity_count().to_string(),
    ]
}

fn format_optional_u32(value: Option<u32>) -> String {
    value.map_or_else(|| "-".to_string(), |value| value.to_string())
}

fn format_optional_u8(value: Option<u8>) -> String {
    value.map_or_else(|| "-".to_string(), |value| value.to_string())
}

fn entity_storage_row(path: &str, store: &str, entries: u64, memory_bytes: u64) -> [String; 4] {
    [
        path.to_string(),
        store.to_string(),
        entries.to_string(),
        memory_bytes.to_string(),
    ]
}

fn append_data_store_table(output: &mut String, rows: &[[String; 13]]) {
    append_snapshot_table(
        output,
        "data stores",
        &[
            "path",
            "storage",
            "alloc",
            "durability",
            "commit",
            "recovery",
            "schema meta",
            "mem",
            "stable key",
            "version",
            "fingerprint",
            "entries",
            "bytes",
        ],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Right,
        ],
    );
}

fn append_index_store_table(output: &mut String, rows: &[[String; 16]]) {
    append_snapshot_table(
        output,
        "index stores",
        &[
            "path",
            "storage",
            "alloc",
            "durability",
            "commit",
            "recovery",
            "schema meta",
            "mem",
            "stable key",
            "version",
            "fingerprint",
            "entries",
            "user",
            "system",
            "bytes",
            "state",
        ],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Left,
        ],
    );
}

fn append_schema_store_table(output: &mut String, rows: &[[String; 12]]) {
    append_snapshot_table(
        output,
        "schema stores",
        &[
            "path",
            "storage",
            "alloc",
            "durability",
            "commit",
            "recovery",
            "schema meta",
            "mem",
            "stable key",
            "version",
            "fingerprint",
            "entities",
        ],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Left,
            ColumnAlign::Right,
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
