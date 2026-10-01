//! Fixed controls for the populated entity-rename deployment rehearsal.

use crate::{typed_fixture_invariant_error, typed_operation_fixture_error};
use ic_cdk::{query, update};
use icydb::{
    __reexports::ic_timers,
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
#[query]
fn entity_creation_startup_snapshot() -> Result<(bool, u64, u64, u64, u64, u64), icydb::Error> {
    let identity = ic_timers::TimerIdentity::try_new("icydb", "startup", "recovery")
        .map_err(|_| typed_fixture_invariant_error())?;
    let snapshot = ic_timers::timer_snapshot(&identity)
        .map_err(|_| typed_fixture_invariant_error())?
        .ok_or_else(typed_fixture_invariant_error)?;
    let performance = snapshot.observability().performance();
    let counters = snapshot.observability().counters();
    Ok((
        lifecycle_ready()?,
        performance.work_instructions().samples(),
        performance.work_instructions().total(),
        performance.scheduler_instructions().total(),
        counters.work_started(),
        counters.work_completed(),
    ))
}

/// Measure one generated-schema submission, including lowering and publication.
#[update]
fn measure_entity_creation_schema_application() -> (
    Result<(), icydb::Error>,
    u64,
    icydb::metrics::SchemaLifecycleMetrics,
) {
    let start = ic_cdk::api::performance_counter(1);
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
    (
        result,
        ic_cdk::api::performance_counter(1).saturating_sub(start),
        metrics_report().schema_lifecycle().clone(),
    )
}

/// Measure a canonical driver page with readiness before/after and quiescence.
#[update]
fn measure_entity_creation_startup_step() -> (
    Result<(bool, bool, bool), icydb::Error>,
    u64,
    icydb::metrics::SchemaLifecycleMetrics,
) {
    let start = ic_cdk::api::performance_counter(1);
    let result = with_request_execution(|| {
        let ready_before = lifecycle_ready()?;
        let terminal = crate::__icydb_generated::__icydb_startup_driver_attempt_for_tests()?;
        Ok((ready_before, lifecycle_ready()?, terminal))
    });
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    (
        result,
        instructions,
        metrics_report().schema_lifecycle().clone(),
    )
}

/// Observe fixed schema-owner counters even before ordinary read admission.
#[query]
fn entity_creation_lifecycle_metrics() -> icydb::metrics::SchemaLifecycleMetrics {
    metrics_report().schema_lifecycle().clone()
}

/// Seed the three fixed rows through ordinary accepted structural writes.
#[update]
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

/// Seed an independent relation target for the entity-creation rehearsal.
#[update]
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

/// Exercise both reverse-relation delete restrictions without caller-owned SQL.
#[update]
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

/// Resolve and decode both generated entities through accepted bindings.
#[query]
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
