use super::*;
use std::{
    hint::black_box,
    time::{Duration, Instant},
};

const STORAGE_BACKEND_PERF_ROWS: u64 = 256;
const STORAGE_BACKEND_INDEX_LOW: u64 = 96;
const STORAGE_BACKEND_INDEX_HIGH: u64 = 128;

#[derive(Clone, Copy)]
struct StorageBackendTiming {
    label: &'static str,
    single_row_write: Duration,
    atomic_batch_write: Duration,
    full_sql_read: Duration,
    indexed_sql_read: Duration,
    checksum: usize,
}

// Run one ignored native timing report over the same indexed row shape on the
// heap and journaled session stores. The timings are informational only;
// correctness assertions only prove that each backend did the work.
#[test]
#[ignore = "native timing report: run explicitly with --ignored --nocapture"]
fn storage_backend_timing_report() {
    println!();
    println!("IcyDB storage backend timing report");
    println!(
        "rows={STORAGE_BACKEND_PERF_ROWS} indexed_range=[{STORAGE_BACKEND_INDEX_LOW}, \
         {STORAGE_BACKEND_INDEX_HIGH})"
    );
    println!("metrics: wall-clock native test runtime; no speed ordering is asserted");
    println!("shape: u64 primary key, name secondary index, name-range SQL query");
    println!(
        "writes: per-row insert loop and one insert_many_atomic batch are measured separately"
    );
    println!();

    let heap = measure_storage_backend::<HeapSessionSqlEntity>(
        "heap",
        reset_heap_session_sql_store,
        heap_sql_session(),
        "HeapSessionSqlEntity",
        |id| HeapSessionSqlEntity {
            id,
            name: perf_name(id),
            age: perf_age(id),
        },
    );
    let journaled = measure_storage_backend::<JournaledSessionSqlEntity>(
        "journaled",
        reset_journaled_session_sql_store,
        journaled_sql_session(),
        "JournaledSessionSqlEntity",
        |id| JournaledSessionSqlEntity {
            id,
            name: perf_name(id),
            age: perf_age(id),
        },
    );

    println!();
    println!("Heap backend audit");
    print_timing(heap, heap, "heap");

    println!();
    println!("Journaled backend audit");
    print_timing(journaled, heap, "heap");
}

fn measure_storage_backend<E>(
    label: &'static str,
    mut reset: impl FnMut(),
    session: DbSession<SessionSqlCanister>,
    entity_name: &'static str,
    build: impl Fn(u64) -> E,
) -> StorageBackendTiming
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    println!("measuring {label}...");
    reset();

    let single_write_started_at = Instant::now();
    for id in 0..STORAGE_BACKEND_PERF_ROWS {
        session
            .insert(black_box(build(id)))
            .unwrap_or_else(|err| panic!("{label} timing insert should succeed: {err}"));
    }
    let single_row_write = single_write_started_at.elapsed();

    reset();

    let entities = (0..STORAGE_BACKEND_PERF_ROWS)
        .map(&build)
        .collect::<Vec<_>>();
    let batch_write_started_at = Instant::now();
    let batch = session
        .insert_many_atomic(black_box(entities))
        .unwrap_or_else(|err| panic!("{label} timing atomic batch insert should succeed: {err}"));
    let atomic_batch_write = batch_write_started_at.elapsed();
    assert_eq!(
        batch.len(),
        expected_full_rows(),
        "{label} atomic batch insert should save every row"
    );

    let full_sql = format!("SELECT id, name, age FROM {entity_name} ORDER BY id ASC");
    let indexed_sql = format!(
        "SELECT name, age FROM {entity_name} \
         WHERE name >= '{}' AND name < '{}' \
         ORDER BY name ASC",
        perf_name(STORAGE_BACKEND_INDEX_LOW),
        perf_name(STORAGE_BACKEND_INDEX_HIGH),
    );

    let warm_full_rows = black_box(sql_projection_row_count::<E>(&session, &full_sql));
    assert_eq!(
        warm_full_rows,
        expected_full_rows(),
        "{label} full SQL warm-up should read every row"
    );
    let full_sql_started_at = Instant::now();
    let full_rows = black_box(sql_projection_row_count::<E>(&session, &full_sql));
    let full_sql_read = full_sql_started_at.elapsed();
    assert_eq!(
        full_rows,
        expected_full_rows(),
        "{label} full SQL read should read every row"
    );

    let expected_index_rows = expected_index_rows();
    let warm_index_rows = black_box(sql_projection_row_count::<E>(&session, &indexed_sql));
    assert_eq!(
        warm_index_rows, expected_index_rows,
        "{label} indexed SQL warm-up should read the selected range"
    );
    let indexed_sql_started_at = Instant::now();
    let indexed_rows = black_box(sql_projection_row_count::<E>(&session, &indexed_sql));
    let indexed_sql_read = indexed_sql_started_at.elapsed();
    assert_eq!(
        indexed_rows, expected_index_rows,
        "{label} indexed SQL read should read the selected range"
    );

    let timing = StorageBackendTiming {
        label,
        single_row_write,
        atomic_batch_write,
        full_sql_read,
        indexed_sql_read,
        checksum: full_rows.saturating_add(indexed_rows),
    };
    println!(
        "{label:<9} per_row_write={:>12} ms batch_write={:>12} ms full_sql={:>12} us \
         indexed_sql={:>12} us checksum={}",
        millis_text(timing.single_row_write),
        millis_text(timing.atomic_batch_write),
        micros_text(timing.full_sql_read),
        micros_text(timing.indexed_sql_read),
        timing.checksum,
    );

    timing
}

fn sql_projection_row_count<E>(session: &DbSession<SessionSqlCanister>, sql: &str) -> usize
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    statement_projection_rows::<E>(session, sql)
        .expect("storage backend timing SQL projection should succeed")
        .len()
}

fn perf_name(id: u64) -> String {
    format!("bench-{id:05}")
}

const fn perf_age(id: u64) -> u64 {
    id % 100
}

fn expected_full_rows() -> usize {
    usize::try_from(STORAGE_BACKEND_PERF_ROWS)
        .expect("storage backend perf row count should fit usize")
}

fn expected_index_rows() -> usize {
    usize::try_from(STORAGE_BACKEND_INDEX_HIGH - STORAGE_BACKEND_INDEX_LOW)
        .expect("storage backend perf index row count should fit usize")
}

fn print_timing(
    timing: StorageBackendTiming,
    baseline: StorageBackendTiming,
    baseline_label: &str,
) {
    println!(
        "{:<9} per_row_write={:>12} ms ({:>7} {baseline_label}) batch_write={:>12} ms \
         ({:>7} {baseline_label}) full_sql={:>12} us ({:>7} {baseline_label}) indexed_sql={:>12} us \
         ({:>7} {baseline_label}) checksum={}",
        timing.label,
        millis_text(timing.single_row_write),
        ratio_text(timing.single_row_write, baseline.single_row_write),
        millis_text(timing.atomic_batch_write),
        ratio_text(timing.atomic_batch_write, baseline.atomic_batch_write),
        micros_text(timing.full_sql_read),
        ratio_text(timing.full_sql_read, baseline.full_sql_read),
        micros_text(timing.indexed_sql_read),
        ratio_text(timing.indexed_sql_read, baseline.indexed_sql_read),
        timing.checksum,
    );
}

fn ratio_text(value: Duration, baseline: Duration) -> String {
    let value_ns = value.as_nanos();
    let baseline_ns = baseline.as_nanos();
    if baseline_ns == 0 {
        return "n/a".to_string();
    }

    let hundredths = value_ns.saturating_mul(100) / baseline_ns;
    format!("{}.{:02}x", hundredths / 100, hundredths % 100)
}

fn millis_text(duration: Duration) -> String {
    let ns = duration.as_nanos();
    let millis = ns / 1_000_000;
    let frac = (ns % 1_000_000) / 1_000;

    format!("{millis}.{frac:03}")
}

fn micros_text(duration: Duration) -> String {
    let ns = duration.as_nanos();
    let micros = ns / 1_000;
    let frac = ns % 1_000;

    format!("{micros}.{frac:03}")
}
