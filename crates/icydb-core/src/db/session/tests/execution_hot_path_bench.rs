use super::*;
use crate::{
    db::data::{
        decode_persisted_custom_many_slot_payload, decode_structural_value_storage_bytes,
        encode_persisted_custom_many_slot_payload, encode_structural_value_storage_bytes,
    },
    value::Value,
};
use std::{
    cmp::Reverse,
    hint::black_box,
    time::{Duration, Instant},
};

const READ_ROWS: usize = 320;
const GROUP_ROWS: usize = 640;
const DELETE_ROWS: usize = 192;
const READ_ITERATIONS: usize = 24;
const GROUP_ITERATIONS: usize = 16;
const AGGREGATE_ITERATIONS: usize = 32;
const DELETE_ITERATIONS: usize = 8;
const CODEC_ITERATIONS: usize = 1_000;

// Run one ignored native microbenchmark report over representative execution
// shapes. The timings are informational only; correctness assertions stay
// limited to ensuring each benchmark actually exercises the intended path.
#[test]
#[ignore = "native microbenchmark: run explicitly with --ignored --nocapture"]
fn execution_hot_path_microbenchmarks_report() {
    let mut results = Vec::new();

    println!();
    println!("IcyDB execution hot-path microbenchmarks");
    println!(
        "metrics: wall-clock runtime; allocations/instructions/memory are not sampled in this dependency-free native harness"
    );
    println!("note: each read scenario warms the SQL/query caches once before timing");
    println!();

    bench_sql_select_simple_projection(&mut results);
    bench_sql_distinct_wide_projection(&mut results);
    bench_sql_order_by_limit(&mut results);
    bench_grouped_high_cardinality(&mut results);
    bench_grouped_low_cardinality(&mut results);
    bench_grouped_multi_field_keys(&mut results);
    bench_global_count_sum_avg(&mut results);
    bench_global_min_max(&mut results);
    bench_grouped_aggregates(&mut results);
    bench_delete_returning_large_rows(&mut results);
    bench_delete_returning_many_rows(&mut results);
    bench_codec_value_collections_and_maps(&mut results);
    bench_codec_nested_structured_many(&mut results);

    print_ranked_hotspots(&mut results);
}

// Measure a warmed read-only scenario and record its average wall time.
fn record_warmed_benchmark(
    results: &mut Vec<(String, u128, usize)>,
    label: &'static str,
    iterations: usize,
    mut run: impl FnMut() -> usize,
) {
    let warm_rows = black_box(run());
    assert!(warm_rows > 0, "{label} warm-up should exercise real rows");

    let started_at = Instant::now();
    let mut checksum = 0usize;
    for _ in 0..iterations {
        checksum = checksum.saturating_add(black_box(run()));
    }
    let elapsed = started_at.elapsed();

    record_result(results, label, iterations, elapsed, checksum);
}

// Measure a write scenario whose store must be reset and reseeded before every
// timed DELETE so statement side effects do not contaminate the next iteration.
fn record_isolated_benchmark(
    results: &mut Vec<(String, u128, usize)>,
    label: &'static str,
    iterations: usize,
    mut setup: impl FnMut(),
    mut run: impl FnMut() -> usize,
) {
    setup();
    let warm_rows = black_box(run());
    assert!(warm_rows > 0, "{label} warm-up should exercise real rows");

    let mut checksum = 0usize;
    let started_at = Instant::now();
    for _ in 0..iterations {
        setup();
        checksum = checksum.saturating_add(black_box(run()));
    }
    let elapsed = started_at.elapsed();

    record_result(results, label, iterations, elapsed, checksum);
}

// Store one result row and print the raw per-scenario timing line as soon as it
// is available so long-running reports still show progress.
fn record_result(
    results: &mut Vec<(String, u128, usize)>,
    label: &'static str,
    iterations: usize,
    elapsed: Duration,
    checksum: usize,
) {
    let total_ns = elapsed.as_nanos();
    let avg_ns = total_ns / iterations as u128;
    println!(
        "{label:<42} total={:>14} ms avg={:>14} us checksum={checksum}",
        millis_text(total_ns),
        micros_text(avg_ns),
    );
    results.push((label.to_string(), avg_ns, checksum));
}

// Print the highest average-time scenarios first so the report points at the
// next likely optimization targets instead of only showing raw samples.
fn print_ranked_hotspots(results: &mut [(String, u128, usize)]) {
    results.sort_by_key(|result| Reverse(result.1));

    println!();
    println!("Top measured hot paths by average wall time");
    for (rank, (label, avg_ns, checksum)) in results.iter().take(10).enumerate() {
        println!(
            "{:>2}. {label:<42} avg={:>14} us checksum={checksum}",
            rank + 1,
            micros_text(*avg_ns),
        );
    }
}

// Format nanoseconds as a fixed three-decimal millisecond string without
// precision-loss casts, keeping clippy clean in test builds.
fn millis_text(ns: u128) -> String {
    let millis = ns / 1_000_000;
    let frac = (ns % 1_000_000) / 1_000;

    format!("{millis}.{frac:03}")
}

// Format nanoseconds as a fixed three-decimal microsecond string without
// precision-loss casts, keeping clippy clean in test builds.
fn micros_text(ns: u128) -> String {
    let micros = ns / 1_000;
    let frac = ns % 1_000;

    format!("{micros}.{frac:03}")
}

// Benchmark a narrow scalar SQL projection over the normal session store.
fn bench_sql_select_simple_projection(results: &mut Vec<(String, u128, usize)>) {
    reset_session_sql_store();
    let session = sql_session();
    seed_generated_session_sql_entities(&session, READ_ROWS, 16);

    record_warmed_benchmark(
        results,
        "sql select simple projection",
        READ_ITERATIONS,
        || {
            statement_projection_rows::<SessionSqlEntity>(
                &session,
                "SELECT name, age FROM SessionSqlEntity",
            )
            .expect("simple SQL projection benchmark should succeed")
            .len()
        },
    );
}

// Benchmark DISTINCT with several projected fields to keep the distinct-key
// path representative of wide row projection costs.
fn bench_sql_distinct_wide_projection(results: &mut Vec<(String, u128, usize)>) {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_generated_filtered_entities(&session, READ_ROWS, 96);

    record_warmed_benchmark(
        results,
        "sql distinct wide projection",
        READ_ITERATIONS,
        || {
            statement_projection_rows::<FilteredIndexedSessionSqlEntity>(
                &session,
                "SELECT DISTINCT name, active, tier, handle, age \
                 FROM FilteredIndexedSessionSqlEntity ORDER BY name ASC LIMIT 160",
            )
            .expect("wide DISTINCT SQL projection benchmark should succeed")
            .len()
        },
    );
}

// Benchmark an indexed ORDER BY + LIMIT shape over the indexed session store.
fn bench_sql_order_by_limit(results: &mut Vec<(String, u128, usize)>) {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_generated_indexed_entities(&session, READ_ROWS, 24);

    record_warmed_benchmark(results, "sql order by limit", READ_ITERATIONS, || {
        statement_projection_rows::<IndexedSessionSqlEntity>(
            &session,
            "SELECT name, age FROM IndexedSessionSqlEntity ORDER BY name ASC LIMIT 64",
        )
        .expect("ORDER BY LIMIT SQL benchmark should succeed")
        .len()
    });
}

// Benchmark grouped execution when nearly every input row creates a group.
fn bench_grouped_high_cardinality(results: &mut Vec<(String, u128, usize)>) {
    reset_session_sql_store();
    let session = sql_session();
    seed_generated_session_sql_entities_with_age_cardinality(&session, GROUP_ROWS, GROUP_ROWS, 16);

    record_warmed_benchmark(
        results,
        "grouped high cardinality",
        GROUP_ITERATIONS,
        || {
            execute_grouped_select_for_tests::<SessionSqlEntity>(
                &session,
                "SELECT age, COUNT(*) FROM SessionSqlEntity \
                 GROUP BY age ORDER BY age ASC LIMIT 640",
                None,
            )
            .expect("high-cardinality grouped benchmark should succeed")
            .rows()
            .len()
        },
    );
}

// Benchmark grouped execution when many input rows fold into a small number of
// groups, which emphasizes reducer ingestion rather than group creation.
fn bench_grouped_low_cardinality(results: &mut Vec<(String, u128, usize)>) {
    reset_session_sql_store();
    let session = sql_session();
    seed_generated_session_sql_entities_with_age_cardinality(&session, GROUP_ROWS, 8, 16);

    record_warmed_benchmark(results, "grouped low cardinality", GROUP_ITERATIONS, || {
        execute_grouped_select_for_tests::<SessionSqlEntity>(
            &session,
            "SELECT age, COUNT(*) FROM SessionSqlEntity \
             GROUP BY age ORDER BY age ASC LIMIT 64",
            None,
        )
        .expect("low-cardinality grouped benchmark should succeed")
        .rows()
        .len()
    });
}

// Benchmark grouped execution with composite group keys so key materialization,
// hashing, and ordering all use multi-value keys.
fn bench_grouped_multi_field_keys(results: &mut Vec<(String, u128, usize)>) {
    reset_session_sql_store();
    let session = sql_session();
    seed_generated_session_sql_entities_with_age_cardinality(&session, GROUP_ROWS, 32, 24);

    record_warmed_benchmark(
        results,
        "grouped multi-field keys",
        GROUP_ITERATIONS,
        || {
            execute_grouped_select_for_tests::<SessionSqlEntity>(
                &session,
                "SELECT name, age, COUNT(*) FROM SessionSqlEntity \
             GROUP BY name, age ORDER BY name ASC, age ASC LIMIT 512",
                None,
            )
            .expect("multi-field grouped benchmark should succeed")
            .rows()
            .len()
        },
    );
}

// Benchmark global arithmetic aggregate reducers over one scalar field.
fn bench_global_count_sum_avg(results: &mut Vec<(String, u128, usize)>) {
    reset_session_sql_store();
    let session = sql_session();
    seed_generated_session_sql_entities(&session, GROUP_ROWS, 16);

    record_warmed_benchmark(
        results,
        "aggregate count/sum/avg",
        AGGREGATE_ITERATIONS,
        || {
            statement_projection_rows::<SessionSqlEntity>(
                &session,
                "SELECT COUNT(age), SUM(age), AVG(age) FROM SessionSqlEntity",
            )
            .expect("COUNT/SUM/AVG benchmark should succeed")
            .len()
        },
    );
}

// Benchmark global extrema reducers, which are sensitive to comparison and
// state replacement boundaries.
fn bench_global_min_max(results: &mut Vec<(String, u128, usize)>) {
    reset_session_sql_store();
    let session = sql_session();
    seed_generated_session_sql_entities(&session, GROUP_ROWS, 16);

    record_warmed_benchmark(results, "aggregate min/max", AGGREGATE_ITERATIONS, || {
        statement_projection_rows::<SessionSqlEntity>(
            &session,
            "SELECT MIN(age), MAX(age) FROM SessionSqlEntity",
        )
        .expect("MIN/MAX benchmark should succeed")
        .len()
    });
}

// Benchmark grouped aggregate execution with multiple reducer families active
// for each group.
fn bench_grouped_aggregates(results: &mut Vec<(String, u128, usize)>) {
    reset_session_sql_store();
    let session = sql_session();
    seed_generated_session_sql_entities_with_age_cardinality(&session, GROUP_ROWS, 32, 16);

    record_warmed_benchmark(results, "grouped aggregates", GROUP_ITERATIONS, || {
        execute_grouped_select_for_tests::<SessionSqlEntity>(
            &session,
            "SELECT age, COUNT(age), SUM(age), AVG(age), MIN(age), MAX(age) \
             FROM SessionSqlEntity GROUP BY age ORDER BY age ASC LIMIT 128",
            None,
        )
        .expect("grouped aggregate benchmark should succeed")
        .rows()
        .len()
    });
}

// Benchmark DELETE RETURNING over large row payloads while excluding fixture
// setup from the timed window.
fn bench_delete_returning_large_rows(results: &mut Vec<(String, u128, usize)>) {
    let session = sql_session();

    record_isolated_benchmark(
        results,
        "delete returning large rows",
        DELETE_ITERATIONS,
        || {
            reset_session_sql_store();
            seed_generated_session_sql_entities(&session, 48, 2_048);
        },
        || {
            statement_projection_rows::<SessionSqlEntity>(
                &session,
                "DELETE FROM SessionSqlEntity WHERE age < 1000 RETURNING id, name, age",
            )
            .expect("large-row DELETE RETURNING benchmark should succeed")
            .len()
        },
    );
}

// Benchmark DELETE RETURNING over a larger number of ordinary-sized rows while
// excluding fixture setup from the timed window.
fn bench_delete_returning_many_rows(results: &mut Vec<(String, u128, usize)>) {
    let session = sql_session();

    record_isolated_benchmark(
        results,
        "delete returning many rows",
        DELETE_ITERATIONS,
        || {
            reset_session_sql_store();
            seed_generated_session_sql_entities(&session, DELETE_ROWS, 24);
        },
        || {
            statement_projection_rows::<SessionSqlEntity>(
                &session,
                "DELETE FROM SessionSqlEntity WHERE age < 1000 RETURNING id, name, age",
            )
            .expect("many-row DELETE RETURNING benchmark should succeed")
            .len()
        },
    );
}

// Benchmark the structural `Value` storage lane over nested collection and map
// payloads without routing through SQL/session execution.
fn bench_codec_value_collections_and_maps(results: &mut Vec<(String, u128, usize)>) {
    let value = nested_codec_value_fixture(32);
    let encoded = encode_structural_value_storage_bytes(&value)
        .expect("codec benchmark value fixture should encode");
    let decoded = decode_structural_value_storage_bytes(encoded.as_slice())
        .expect("codec benchmark value fixture should decode");
    assert_eq!(decoded, value, "codec benchmark fixture should round-trip");

    record_warmed_benchmark(
        results,
        "codec value collections/maps",
        CODEC_ITERATIONS,
        || {
            let encoded = encode_structural_value_storage_bytes(black_box(&value))
                .expect("codec value benchmark encode should succeed");
            let decoded = decode_structural_value_storage_bytes(black_box(encoded.as_slice()))
                .expect("codec value benchmark decode should succeed");
            value_weight(&decoded)
        },
    );
}

// Benchmark the persisted custom-many structured codec over nested runtime
// values, which exercises collection traversal from the persisted row boundary.
fn bench_codec_nested_structured_many(results: &mut Vec<(String, u128, usize)>) {
    let values = (0..48)
        .map(|index| nested_codec_value_fixture((index % 8) + 4))
        .collect::<Vec<_>>();
    let encoded = encode_persisted_custom_many_slot_payload(values.as_slice(), "values")
        .expect("custom-many codec benchmark fixture should encode");
    let decoded = decode_persisted_custom_many_slot_payload::<Value>(encoded.as_slice(), "values")
        .expect("custom-many codec benchmark fixture should decode");
    assert_eq!(
        decoded.len(),
        values.len(),
        "custom-many codec benchmark fixture should round-trip item count",
    );

    record_warmed_benchmark(
        results,
        "codec nested structured many",
        CODEC_ITERATIONS,
        || {
            let encoded =
                encode_persisted_custom_many_slot_payload(black_box(values.as_slice()), "values")
                    .expect("custom-many codec benchmark encode should succeed");
            let decoded = decode_persisted_custom_many_slot_payload::<Value>(
                black_box(encoded.as_slice()),
                "values",
            )
            .expect("custom-many codec benchmark decode should succeed");
            decoded.iter().map(value_weight).sum()
        },
    );
}

// Seed generated rows for plain SQL SELECT and DELETE scenarios.
fn seed_generated_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: usize,
    name_width: usize,
) {
    for index in 0..rows {
        session
            .insert(SessionSqlEntity {
                id: Ulid::from_u128(index as u128 + 1),
                name: generated_text("entity", index, name_width),
                age: (index % 97) as u64,
            })
            .expect("generated SQL fixture insert should succeed");
    }
}

// Seed plain SQL rows with an explicit age cardinality so grouped benchmarks can
// switch between group-creation-heavy and reducer-ingestion-heavy shapes.
fn seed_generated_session_sql_entities_with_age_cardinality(
    session: &DbSession<SessionSqlCanister>,
    rows: usize,
    age_cardinality: usize,
    name_width: usize,
) {
    for index in 0..rows {
        session
            .insert(SessionSqlEntity {
                id: Ulid::from_u128(index as u128 + 1),
                name: generated_text("grouped", index, name_width),
                age: (index % age_cardinality) as u64,
            })
            .expect("generated grouped SQL fixture insert should succeed");
    }
}

// Seed generated rows for indexed ORDER BY scenarios.
fn seed_generated_indexed_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: usize,
    name_width: usize,
) {
    for index in 0..rows {
        session
            .insert(IndexedSessionSqlEntity {
                id: Ulid::from_u128(index as u128 + 1),
                name: generated_text("indexed", rows - index, name_width),
                age: (index % 97) as u64,
            })
            .expect("generated indexed SQL fixture insert should succeed");
    }
}

// Seed generated rows for wide DISTINCT projection scenarios.
fn seed_generated_filtered_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: usize,
    text_width: usize,
) {
    for index in 0..rows {
        session
            .insert(FilteredIndexedSessionSqlEntity {
                id: Ulid::from_u128(index as u128 + 1),
                name: generated_text("distinct-name", index % 96, text_width),
                active: index % 2 == 0,
                tier: generated_text("tier", index % 8, 16),
                handle: generated_text("handle", index % 128, text_width),
                age: (index % 64) as u64,
            })
            .expect("generated filtered SQL fixture insert should succeed");
    }
}

// Build one deterministic text payload with a minimum width. Setup-time text
// allocation is intentionally outside timed benchmark loops.
fn generated_text(prefix: &str, index: usize, width: usize) -> String {
    let mut value = format!("{prefix}-{index:05}");
    let padding = width.saturating_sub(value.len());
    value.push_str(&"x".repeat(padding));

    value
}

// Build a nested `Value` payload with list and map shapes that exercise the
// structural codec traversal without relying on malformed bytes.
fn nested_codec_value_fixture(width: usize) -> Value {
    let entries = (0..width)
        .map(|index| {
            let blob_byte = u8::try_from(index % 256)
                .expect("codec benchmark blob byte should fit after modulo");
            (
                Value::Text(format!("key-{index:04}")),
                Value::List(vec![
                    Value::Uint(index as u64),
                    Value::Text(generated_text("payload", index, 24)),
                    Value::Blob(vec![blob_byte; 16]),
                ]),
            )
        })
        .collect::<Vec<_>>();
    let map = Value::try_from(entries).expect("codec benchmark map fixture should be canonical");

    Value::List(vec![
        map,
        Value::Text("codec-root".to_string()),
        Value::Uint(width as u64),
    ])
}

// Compute a small deterministic weight for decoded codec values so encode and
// decode results remain observable to the optimizer.
fn value_weight(value: &Value) -> usize {
    match value {
        Value::Blob(bytes) => bytes.len(),
        Value::List(values) => values.iter().map(value_weight).sum(),
        Value::Map(entries) => entries
            .iter()
            .map(|(key, value)| value_weight(key).saturating_add(value_weight(value)))
            .sum(),
        Value::Text(value) => value.len(),
        _ => 1,
    }
}
