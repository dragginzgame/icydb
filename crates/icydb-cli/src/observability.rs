use std::process::Stdio;

use candid::Decode;
use icydb::{
    db::{EntitySchemaDescription, StorageReport},
    metrics::{EventCounters, EventReport},
};

use crate::{
    cli::{CanisterTarget, MetricsArgs},
    config::{
        METRICS_ENDPOINT, METRICS_RESET_ENDPOINT, SCHEMA_ENDPOINT, SNAPSHOT_ENDPOINT,
        require_configured_endpoint,
    },
    icp::require_created_canister,
    shell::{hex_response_bytes, icp_query_command, icp_update_command},
    table::{ColumnAlign, append_indented_table},
};

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

/// Read or reset the generated metrics endpoints.
pub(crate) fn run_metrics_command(args: MetricsArgs) -> Result<(), String> {
    let target = args.target();
    let endpoint = if args.reset() {
        METRICS_RESET_ENDPOINT
    } else {
        METRICS_ENDPOINT
    };
    require_configured_endpoint(target.canister_name(), endpoint)?;
    require_created_canister(target.environment(), target.canister_name())?;

    if args.reset() {
        return run_metrics_reset(target);
    }

    let candid_arg = metrics_candid_arg(args.window_start_ms());
    let candid_bytes = call_query(
        target.environment(),
        target.canister_name(),
        endpoint.method(),
        candid_arg.as_str(),
    )?;
    let response = Decode!(
        candid_bytes.as_slice(),
        Result<icydb::metrics::EventReport, icydb::Error>
    )
    .map_err(|err| err.to_string())?;

    match response {
        Ok(report) => {
            print!("{}", render_metrics_report(&report));

            Ok(())
        }
        Err(err) => Err(format!(
            "IcyDB metrics method '{}' failed on canister '{}' in environment '{}': {err}",
            endpoint.method(),
            target.canister_name(),
            target.environment(),
        )),
    }
}

fn run_metrics_reset(target: &CanisterTarget) -> Result<(), String> {
    let candid_bytes = call_update(
        target.environment(),
        target.canister_name(),
        METRICS_RESET_ENDPOINT.method(),
        "()",
    )?;
    let response = Decode!(candid_bytes.as_slice(), Result<(), icydb::Error>)
        .map_err(|err| err.to_string())?;

    match response {
        Ok(()) => {
            println!(
                "Reset metrics on canister '{}' in environment '{}'.",
                target.canister_name(),
                target.environment(),
            );

            Ok(())
        }
        Err(err) => Err(format!(
            "IcyDB metrics reset method '{}' failed on canister '{}' in environment '{}': {err}",
            METRICS_RESET_ENDPOINT.method(),
            target.canister_name(),
            target.environment(),
        )),
    }
}

/// Read and print the generated accepted-schema endpoint.
pub(crate) fn run_schema_command(target: CanisterTarget) -> Result<(), String> {
    require_configured_endpoint(target.canister_name(), SCHEMA_ENDPOINT)?;
    require_created_canister(target.environment(), target.canister_name())?;
    let candid_bytes = call_query(
        target.environment(),
        target.canister_name(),
        SCHEMA_ENDPOINT.method(),
        "()",
    )?;
    let response = Decode!(
        candid_bytes.as_slice(),
        Result<Vec<icydb::db::EntitySchemaDescription>, icydb::Error>
    )
    .map_err(|err| err.to_string())?;

    match response {
        Ok(report) => {
            print!("{}", render_schema_report(report.as_slice()));

            Ok(())
        }
        Err(err) => Err(format!(
            "IcyDB schema method '{}' failed on canister '{}' in environment '{}': {err}",
            SCHEMA_ENDPOINT.method(),
            target.canister_name(),
            target.environment(),
        )),
    }
}

pub(crate) fn metrics_candid_arg(window_start_ms: Option<u64>) -> String {
    match window_start_ms {
        Some(value) => format!("(opt ({value} : nat64))"),
        None => "(null)".to_string(),
    }
}

pub(crate) fn render_schema_report(report: &[EntitySchemaDescription]) -> String {
    let mut output = String::new();
    let rows = report
        .iter()
        .map(|entity| {
            [
                entity.entity_name().to_string(),
                entity.fields().len().to_string(),
                entity.indexes().len().to_string(),
                entity.relations().len().to_string(),
                entity.primary_key().to_string(),
                entity.entity_path().to_string(),
            ]
        })
        .collect::<Vec<_>>();

    output.push_str("IcyDB schema\n");
    output.push_str(format!("  entities: {}\n\n", report.len()).as_str());
    append_schema_entity_table(&mut output, rows.as_slice());

    output
}

pub(crate) fn render_snapshot_report(report: &StorageReport) -> String {
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

pub(crate) fn render_metrics_report(report: &EventReport) -> String {
    let mut output = String::new();

    output.push_str("IcyDB metrics\n");
    output.push_str(
        format!(
            "  active window start ms: {}\n  requested window start ms: {}\n  window filter matched: {}\n  entities: {}\n",
            report.active_window_start_ms(),
            optional_u64(report.requested_window_start_ms()),
            yes_no(report.window_filter_matched()),
            report.entity_counters().len(),
        )
        .as_str(),
    );

    if let Some(counters) = report.counters() {
        append_metrics_counters(&mut output, counters);
    } else {
        output.push_str("  counters: none\n");
    }
    output.push('\n');

    let entity_rows = report
        .entity_counters()
        .iter()
        .map(|entity| {
            (
                entity.path(),
                entity.load_calls().to_string(),
                entity.save_calls().to_string(),
                entity.delete_calls().to_string(),
                entity.exec_success().to_string(),
                entity_exec_errors(entity).to_string(),
            )
        })
        .collect::<Vec<_>>();
    append_metrics_entity_table(&mut output, entity_rows.as_slice());

    output
}

fn append_metrics_counters(output: &mut String, counters: &EventCounters) {
    let ops = counters.ops();
    output.push_str(
        format!(
            "  window: {}..{} ({} ms)\n  calls: load={} save={} delete={}\n  execution: success={} errors={} aborted={}\n  rows: loaded={} saved={} deleted={} scanned={} filtered={} emitted={}\n  sql writes: insert={} insert_select={} update={} delete={} matched={} mutated={} returning={}\n  cache: query_plan_hits={} query_plan_misses={} sql_hits={} sql_misses={}\n",
            counters.window_start_ms(),
            counters.window_end_ms(),
            counters.window_duration_ms(),
            ops.load_calls(),
            ops.save_calls(),
            ops.delete_calls(),
            ops.exec_success(),
            ops_exec_errors(ops),
            ops.exec_aborted(),
            ops.rows_loaded(),
            ops.rows_saved(),
            ops.rows_deleted(),
            ops.rows_scanned(),
            ops.rows_filtered(),
            ops.rows_emitted(),
            ops.sql_insert_calls(),
            ops.sql_insert_select_calls(),
            ops.sql_update_calls(),
            ops.sql_delete_calls(),
            ops.sql_write_matched_rows(),
            ops.sql_write_mutated_rows(),
            ops.sql_write_returning_rows(),
            ops.cache_shared_query_plan_hits(),
            ops.cache_shared_query_plan_misses(),
            ops.cache_sql_compiled_command_hits(),
            ops.cache_sql_compiled_command_misses(),
        )
        .as_str(),
    );
}

fn append_schema_entity_table(output: &mut String, rows: &[[String; 6]]) {
    output.push_str("entities\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    append_indented_table(
        output,
        "  ",
        &[
            "entity",
            "fields",
            "indexes",
            "relations",
            "primary key",
            "path",
        ],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Left,
            ColumnAlign::Left,
        ],
    );
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

fn append_metrics_entity_table(
    output: &mut String,
    rows: &[(&str, String, String, String, String, String)],
) {
    output.push_str("entities\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    let entity_width = table_width("entity", rows.iter().map(|(entity, _, _, _, _, _)| *entity));
    let load_width = table_width(
        "load",
        rows.iter().map(|(_, load, _, _, _, _)| load.as_str()),
    );
    let save_width = table_width(
        "save",
        rows.iter().map(|(_, _, save, _, _, _)| save.as_str()),
    );
    let delete_width = table_width(
        "delete",
        rows.iter().map(|(_, _, _, delete, _, _)| delete.as_str()),
    );
    let success_width = table_width(
        "success",
        rows.iter().map(|(_, _, _, _, success, _)| success.as_str()),
    );
    let errors_width = table_width(
        "errors",
        rows.iter().map(|(_, _, _, _, _, errors)| errors.as_str()),
    );
    output.push_str(
        format!(
            "  {entity:<entity_width$}  {load:>load_width$}  {save:>save_width$}  {delete:>delete_width$}  {success:>success_width$}  {errors:>errors_width$}\n",
            entity = "entity",
            load = "load",
            save = "save",
            delete = "delete",
            success = "success",
            errors = "errors",
        )
        .as_str(),
    );
    for (entity, load, save, delete, success, errors) in rows {
        output.push_str(
            format!(
                "  {entity:<entity_width$}  {load:>load_width$}  {save:>save_width$}  {delete:>delete_width$}  {success:>success_width$}  {errors:>errors_width$}\n"
            )
            .as_str(),
        );
    }
}

const fn ops_exec_errors(ops: &icydb::metrics::EventOps) -> u64 {
    ops.exec_error_corruption()
        .saturating_add(ops.exec_error_incompatible_persisted_format())
        .saturating_add(ops.exec_error_not_found())
        .saturating_add(ops.exec_error_internal())
        .saturating_add(ops.exec_error_conflict())
        .saturating_add(ops.exec_error_unsupported())
        .saturating_add(ops.exec_error_invariant_violation())
}

const fn entity_exec_errors(entity: &icydb::metrics::EntitySummary) -> u64 {
    entity
        .exec_error_corruption()
        .saturating_add(entity.exec_error_incompatible_persisted_format())
        .saturating_add(entity.exec_error_not_found())
        .saturating_add(entity.exec_error_internal())
        .saturating_add(entity.exec_error_conflict())
        .saturating_add(entity.exec_error_unsupported())
        .saturating_add(entity.exec_error_invariant_violation())
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "none".to_string(), |value| value.to_string())
}

const fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn table_width<'a>(heading: &str, values: impl Iterator<Item = &'a str>) -> usize {
    values.map(str::len).max().unwrap_or(0).max(heading.len())
}

fn call_query(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
) -> Result<Vec<u8>, String> {
    let output = icp_query_command(environment, canister, method, candid_arg)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| err.to_string())?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "IcyDB query method '{method}' failed on canister '{canister}' in environment '{environment}': {}",
            stderr.trim(),
        ));
    }

    let stdout = String::from_utf8(output.stdout).map_err(|err| err.to_string())?;

    hex_response_bytes(stdout.as_str())
}

fn call_update(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
) -> Result<Vec<u8>, String> {
    let output = icp_update_command(environment, canister, method, candid_arg)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| err.to_string())?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "IcyDB update method '{method}' failed on canister '{canister}' in environment '{environment}': {}",
            stderr.trim(),
        ));
    }

    let stdout = String::from_utf8(output.stdout).map_err(|err| err.to_string())?;

    hex_response_bytes(stdout.as_str())
}
