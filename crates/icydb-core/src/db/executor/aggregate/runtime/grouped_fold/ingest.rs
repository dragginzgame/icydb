//! Module: db::executor::aggregate::runtime::grouped_fold::ingest
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_fold::ingest.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use std::cmp::Ordering;

use crate::{
    db::{
        Context,
        contracts::canonical_value_compare,
        data::{DataKey, DataRow, RawRow},
        executor::{
            OrderedKeyStream,
            aggregate::{AggregateEngine, ExecutionContext, FoldControl},
            group::{CanonicalKey, GroupKey, KeyCanonicalError},
            pipeline::contracts::{GroupedRouteStageProjection, GroupedStreamStage, LoadExecutor},
        },
        predicate::MissingRowPolicy,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Ingest grouped source rows into aggregate reducers while preserving budget contracts.
    pub(super) fn ingest_grouped_rows_into_engines<R>(
        route: &R,
        stream: &mut GroupedStreamStage<'_, E>,
        grouped_execution_context: &mut ExecutionContext,
        grouped_engines: &mut [AggregateEngine<E>],
        short_circuit_keys: &mut [Vec<Value>],
        max_groups_bound: usize,
    ) -> Result<(usize, usize), InternalError>
    where
        R: GroupedRouteStageProjection<E>,
    {
        let (ctx, execution_preparation, resolved) = stream.parts_mut();
        let compiled_predicate = execution_preparation.compiled_predicate();
        let mut read_row_for_key = |key: &DataKey| -> Result<Option<RawRow>, InternalError> {
            let row = match route.consistency() {
                MissingRowPolicy::Error => ctx.read_strict(key),
                MissingRowPolicy::Ignore => ctx.read(key),
            };

            match row {
                Ok(row) => Ok(Some(row)),
                Err(err) if err.is_not_found() => Ok(None),
                Err(err) => Err(err),
            }
        };
        let mut decode_grouped_fold_input =
            |row: DataRow| -> Result<Option<GroupedFoldIngestInput>, InternalError> {
                let (id, entity) = Context::<E>::deserialize_row(row)?;
                if let Some(compiled_predicate) = compiled_predicate
                    && !compiled_predicate.eval(&entity)
                {
                    return Ok(None);
                }

                let group_values = route
                    .group_fields()
                    .iter()
                    .map(|field| {
                        entity.get_value_by_index(field.index()).ok_or_else(|| {
                            crate::db::error::query_executor_invariant(format!(
                                "grouped field slot missing on entity: index={}",
                                field.index()
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let group_key = Value::List(group_values)
                    .canonical_key()
                    .map_err(KeyCanonicalError::into_internal_error)?;
                let canonical_group_value = group_key.canonical_value().clone();
                let data_key = DataKey::try_new::<E>(id.key())?;

                Ok(Some(GroupedFoldIngestInput {
                    group_key,
                    canonical_group_value,
                    data_key,
                }))
            };
        let mut ingest_grouped_fold_input =
            |input: &GroupedFoldIngestInput| -> Result<(), InternalError> {
                let mut ingest_engine = |index: usize| -> Result<bool, InternalError> {
                    let Some(engine) = grouped_engines.get_mut(index) else {
                        return Err(crate::db::error::query_executor_invariant(format!(
                            "grouped engine index out of bounds during fold ingest: index={index}, engine_count={}",
                            grouped_engines.len()
                        )));
                    };
                    let fold_control = engine
                        .ingest_grouped(
                            input.group_key.clone(),
                            &input.data_key,
                            grouped_execution_context,
                        )
                        .map_err(Self::map_group_error)?;

                    Ok(matches!(fold_control, FoldControl::Break))
                };
                ingest_grouped_fold_input_dyn(
                    short_circuit_keys,
                    &input.canonical_group_value,
                    max_groups_bound,
                    &mut ingest_engine,
                )
            };

        ingest_grouped_rows_loop_dyn(
            resolved.key_stream_mut(),
            &mut read_row_for_key,
            &mut decode_grouped_fold_input,
            &mut ingest_grouped_fold_input,
        )
    }
}

///
/// GroupedFoldIngestInput
///
/// Canonical grouped ingest row payload passed from typed decode/predicate
/// wrappers into the shared grouped-fold ingest control-flow boundary.
///

struct GroupedFoldIngestInput {
    group_key: GroupKey,
    canonical_group_value: Value,
    data_key: DataKey,
}

// Shared grouped-fold ingest loop.
// Typed wrappers provide row-read/decode and engine-ingest callbacks so the
// key-stream + row-count control flow compiles once.
fn ingest_grouped_rows_loop_dyn(
    key_stream: &mut dyn OrderedKeyStream,
    read_row_for_key: &mut dyn FnMut(&DataKey) -> Result<Option<RawRow>, InternalError>,
    decode_grouped_fold_input: &mut dyn FnMut(
        DataRow,
    ) -> Result<
        Option<GroupedFoldIngestInput>,
        InternalError,
    >,
    ingest_grouped_fold_input: &mut dyn FnMut(&GroupedFoldIngestInput) -> Result<(), InternalError>,
) -> Result<(usize, usize), InternalError> {
    // Phase 1: scan keys and read authoritative rows.
    let mut scanned_rows = 0usize;
    let mut filtered_rows = 0usize;

    while let Some(key) = key_stream.next_key()? {
        let Some(raw_row) = read_row_for_key(&key)? else {
            continue;
        };
        scanned_rows = scanned_rows.saturating_add(1);

        // Phase 2: decode/filter rows into grouped-ingest payloads.
        let Some(input) = decode_grouped_fold_input((key, raw_row))? else {
            continue;
        };
        filtered_rows = filtered_rows.saturating_add(1);

        // Phase 3: apply grouped reducer ingestion and short-circuit tracking.
        ingest_grouped_fold_input(&input)?;
    }

    Ok((scanned_rows, filtered_rows))
}

// Shared per-row grouped-engine ingest control flow.
// Typed wrappers inject aggregate-engine ingestion while this helper owns
// short-circuit key rejection and bounded tracking invariants.
fn ingest_grouped_fold_input_dyn(
    short_circuit_keys: &mut [Vec<Value>],
    canonical_group_value: &Value,
    max_groups_bound: usize,
    ingest_engine: &mut dyn FnMut(usize) -> Result<bool, InternalError>,
) -> Result<(), InternalError> {
    for (index, done_group_keys) in short_circuit_keys.iter_mut().enumerate() {
        if done_group_keys
            .iter()
            .any(|done| canonical_value_compare(done, canonical_group_value) == Ordering::Equal)
        {
            continue;
        }

        if ingest_engine(index)? {
            done_group_keys.push(canonical_group_value.clone());
            debug_assert!(
                done_group_keys.len() <= max_groups_bound,
                "grouped short-circuit key tracking must stay bounded by max_groups",
            );
        }
    }

    Ok(())
}
