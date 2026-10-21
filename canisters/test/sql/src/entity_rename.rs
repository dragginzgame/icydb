//! Fixed controls for the populated entity-rename deployment rehearsal.

use crate::{typed_fixture_invariant_error, typed_operation_fixture_error};

use icydb::{
    db::{StructuralPatch, WriteCell, with_request_execution},
    metrics::metrics_report,
    traits::EntitySource,
    types::Id,
    value::InputValue,
};
#[cfg(feature = "entity-rename-successor")]
use icydb_testing_test_sql_fixtures::entity_rename::CatalogItem as RenameItem;
use icydb_testing_test_sql_fixtures::entity_rename::Holder;
#[cfg(not(feature = "entity-rename-successor"))]
use icydb_testing_test_sql_fixtures::entity_rename::Item as RenameItem;

// These controls exist in every lifecycle artifact, including the source.
// They observe or invoke maintained owners; no alternate startup driver is added.
fn lifecycle_ready() -> Result<bool, icydb::Error> {
    crate::startup_state()
        .map(|state| state == icydb::db::DatabaseStartupState::Ready)
        .map_err(|failure| failure.error().clone())
}

/// Observe readiness and cumulative work/scheduler instructions since upgrade.

fn entity_creation_startup_snapshot() -> Result<(bool, u64, u128, u128, u64, u64), icydb::Error> {
    let identity = ic_timers::TimerIdentity::try_new("icydb", "startup", "recovery")
        .map_err(|_| typed_fixture_invariant_error())?;
    let snapshot = ic_timers::timer_snapshot(&identity)
        .map_err(|_| typed_fixture_invariant_error())?
        .ok_or_else(typed_fixture_invariant_error)?;
    let performance = snapshot.observability().performance();
    let counters = snapshot.observability().counters();
    let work = performance.work_instructions().aggregate();
    let scheduler = performance.scheduler_instructions().aggregate();
    // The fixture reply retains u64 counts and must reject inexact evidence.
    let exact_count = |counter: ic_timers::MeasurementCounter| {
        let total = counter
            .exact_total()
            .map_err(|_| typed_fixture_invariant_error())?;
        u64::try_from(total).map_err(|_| typed_fixture_invariant_error())
    };
    Ok((
        lifecycle_ready()?,
        work.samples(),
        work.total(),
        scheduler.total(),
        exact_count(counters.work_started())?,
        exact_count(counters.work_completed())?,
    ))
}
icydb_testing_wasm_helpers::export_wire!(query, "entity_creation_startup_snapshot", EntityCreationStartupSnapshot, entity_creation_startup_snapshot, (), Result<(bool, u64, u128, u128, u64, u64), icydb::Error>);

/// Measure one generated-schema submission, including lowering and publication.

fn measure_entity_creation_schema_application() -> (
    Result<(), icydb::Error>,
    u64,
    icydb::metrics::SchemaLifecycleMetrics,
) {
    let start = ic_platform::ic0::performance_counter(1);
    let result = with_request_execution(|| {
        let session = icydb::db::DbSession::new(crate::__icydb_generated::core_db()?);
        session
            .apply_generated_schema_fragment(
                crate::__icydb_generated::ICYDB_SCHEMA_FRAGMENT,
                crate::__icydb_generated::ICYDB_SCHEMA_MIGRATION_PLAN,
                crate::__icydb_generated::ICYDB_SCHEMA_SUBMISSION_KEY,
                crate::__icydb_generated::ICYDB_SCHEMA_ENTITY_STORES,
            )
            .map(|_receipt| ())
    });
    let instructions = ic_platform::ic0::performance_counter(1).saturating_sub(start);
    match metrics_report() {
        Ok(report) => (result, instructions, report.schema_lifecycle().clone()),
        Err(error) => (
            Err(error),
            instructions,
            icydb::metrics::SchemaLifecycleMetrics::default(),
        ),
    }
}
icydb_testing_wasm_helpers::export_wire!(
    update,
    "measure_entity_creation_schema_application",
    MeasureEntityCreationSchemaApplication,
    measure_entity_creation_schema_application,
    (),
    (
        Result<(), icydb::Error>,
        u64,
        icydb::metrics::SchemaLifecycleMetrics,
    )
);

/// Measure a canonical driver page with readiness before/after and quiescence.

fn measure_entity_creation_startup_step() -> (
    Result<(bool, bool, bool), icydb::Error>,
    u64,
    icydb::metrics::SchemaLifecycleMetrics,
) {
    let start = ic_platform::ic0::performance_counter(1);
    let result = with_request_execution(|| {
        let ready_before = lifecycle_ready()?;
        let terminal = crate::__icydb_generated::__icydb_startup_driver_attempt_for_tests()?;
        Ok((ready_before, lifecycle_ready()?, terminal))
    });
    let instructions = ic_platform::ic0::performance_counter(1).saturating_sub(start);
    match metrics_report() {
        Ok(report) => (result, instructions, report.schema_lifecycle().clone()),
        Err(error) => (
            Err(error),
            instructions,
            icydb::metrics::SchemaLifecycleMetrics::default(),
        ),
    }
}
icydb_testing_wasm_helpers::export_wire!(
    update,
    "measure_entity_creation_startup_step",
    MeasureEntityCreationStartupStep,
    measure_entity_creation_startup_step,
    (),
    (
        Result<(bool, bool, bool), icydb::Error>,
        u64,
        icydb::metrics::SchemaLifecycleMetrics,
    )
);

/// Observe fixed schema-owner counters even before ordinary read admission.

fn entity_creation_lifecycle_metrics()
-> Result<icydb::metrics::SchemaLifecycleMetrics, icydb::Error> {
    Ok(metrics_report()?.schema_lifecycle().clone())
}
icydb_testing_wasm_helpers::export_wire!(query, "entity_creation_lifecycle_metrics", EntityCreationLifecycleMetrics, entity_creation_lifecycle_metrics, (), Result<icydb::metrics::SchemaLifecycleMetrics, icydb::Error>);

/// Seed the three fixed rows through ordinary accepted structural writes.

fn seed_entity_rename() -> Result<(), icydb::Error> {
    with_request_execution(|| {
        let session = icydb::db!()?;
        for (id, parent) in [(1, InputValue::null()), (2, InputValue::nat64(1))] {
            let patch = StructuralPatch::new()
                .field("id", WriteCell::Value(InputValue::nat64(id)))
                .field("key", WriteCell::Value(InputValue::nat64(100 + id)))
                .field("label", WriteCell::Value(InputValue::nat64(200 + id)))
                .field("parent_id", WriteCell::Value(parent));
            session.execute_trusted_structural_insert_batch(RenameItem::ENTITY, vec![patch])?;
        }
        let holder = StructuralPatch::new()
            .field("id", WriteCell::Value(InputValue::nat64(10)))
            .field("item_id", WriteCell::Value(InputValue::nat64(2)));
        session.execute_trusted_structural_insert_batch(Holder::ENTITY, vec![holder])?;
        Ok(())
    })
}
icydb_testing_wasm_helpers::export_wire!(update, "seed_entity_rename", SeedEntityRename, seed_entity_rename, (), Result<(), icydb::Error>);

/// Seed an independent relation target for the entity-creation rehearsal.

fn seed_entity_creation_target() -> Result<(), icydb::Error> {
    with_request_execution(|| {
        let session = icydb::db!()?;
        let patch = StructuralPatch::new()
            .field("id", WriteCell::Value(InputValue::nat64(3)))
            .field("key", WriteCell::Value(InputValue::nat64(103)))
            .field("label", WriteCell::Value(InputValue::nat64(303)))
            .field("parent_id", WriteCell::Value(InputValue::null()));
        session.execute_trusted_structural_insert_batch(RenameItem::ENTITY, vec![patch])?;
        Ok(())
    })
}
icydb_testing_wasm_helpers::export_wire!(update, "seed_entity_creation_target", SeedEntityCreationTarget, seed_entity_creation_target, (), Result<(), icydb::Error>);

/// Exercise both reverse-relation delete restrictions without caller-owned SQL.

fn check_entity_rename_deletes() -> Result<Vec<icydb::Error>, icydb::Error> {
    with_request_execution(|| {
        let session = icydb::db!()?;
        let mut errors = Vec::new();
        for id in [1, 2] {
            let sql = format!("DELETE FROM {} WHERE id = {id}", RenameItem::ENTITY);
            let Err(error) = session.execute_trusted_sql_mutation(&sql) else {
                return Err(typed_fixture_invariant_error());
            };
            errors.push(error);
        }
        Ok(errors)
    })
}
icydb_testing_wasm_helpers::export_wire!(
    update,
    "check_entity_rename_deletes",
    CheckEntityRenameDeletes,
    check_entity_rename_deletes,
    (),
    Result<Vec<icydb::Error>, icydb::Error>
);

/// Resolve and decode both generated entities through accepted bindings.

fn check_entity_rename_bindings() -> Result<(), icydb::Error> {
    with_request_execution(|| {
        let session = icydb::db!()?;
        // Exact keys obey ordinary typed-read admission without a full scan.
        for expected in [(1, 101, 201, None), (2, 102, 202, Some(1))] {
            let row = session
                .get::<RenameItem>(Id::from_key(expected.0))
                .map_err(typed_operation_fixture_error)?
                .ok_or_else(typed_fixture_invariant_error)?;
            if (row.id, row.key, row.label, row.parent_id) != expected {
                return Err(typed_fixture_invariant_error());
            }
        }
        let holder = session
            .get::<Holder>(Id::from_key(10))
            .map_err(typed_operation_fixture_error)?
            .ok_or_else(typed_fixture_invariant_error)?;
        if (holder.id, holder.item_id) != (10, 2) {
            return Err(typed_fixture_invariant_error());
        }
        Ok(())
    })
}
icydb_testing_wasm_helpers::export_wire!(query, "check_entity_rename_bindings", CheckEntityRenameBindings, check_entity_rename_bindings, (), Result<(), icydb::Error>);
