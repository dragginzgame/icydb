//! Module: db::executor::aggregate::runtime::grouped_distinct::aggregate
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_distinct::aggregate.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        Context, GroupedRow,
        executor::{
            KeyStreamLoopControl,
            aggregate::{
                AggregateEngine, AggregateExecutionSpec, AggregateIngestAdapter, AggregateKind,
                ExecutionContext, FoldControl, execute_aggregate_engine,
                field::{
                    AggregateFieldValueError, FieldSlot, extract_numeric_field_decimal,
                    extract_orderable_field_value,
                },
                runtime::grouped_output::aggregate_output_to_value,
            },
            group::{CanonicalKey, GroupKeySet, KeyCanonicalError},
            pipeline::contracts::{LoadExecutor, ResolvedExecutionKeyStream},
        },
        predicate::MissingRowPolicy,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Decimal,
    value::Value,
};

///
/// GlobalDistinctFieldAggregateKind
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum GlobalDistinctFieldAggregateKind {
    Count,
    Sum,
    Avg,
}

enum GlobalDistinctFieldReducerMode {
    Count,
    Numeric,
}

///
/// GlobalDistinctFieldAggregateDispatcher
///
/// GlobalDistinctFieldAggregateDispatcher owns aggregate-kind dispatch for
/// grouped global DISTINCT field reducers.
/// This centralizes kind-to-slot and kind-to-finalization routing so the fold
/// loop remains kind-agnostic.
///

struct GlobalDistinctFieldAggregateDispatcher<'a> {
    target_field: &'a str,
    field_slot: FieldSlot,
    aggregate_kind: AggregateKind,
    mode: GlobalDistinctFieldReducerMode,
}

impl<'a> GlobalDistinctFieldAggregateDispatcher<'a> {
    // Build one dispatcher from one execution spec and resolve slot contracts.
    fn resolve<E>(
        execution_spec: (&'a str, GlobalDistinctFieldAggregateKind),
    ) -> Result<Self, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let (target_field, aggregate_kind) = execution_spec;
        match aggregate_kind {
            GlobalDistinctFieldAggregateKind::Count => Ok(Self {
                target_field,
                field_slot: LoadExecutor::<E>::resolve_any_field_slot(target_field)?,
                aggregate_kind: AggregateKind::Count,
                mode: GlobalDistinctFieldReducerMode::Count,
            }),
            GlobalDistinctFieldAggregateKind::Sum => Ok(Self {
                target_field,
                field_slot: LoadExecutor::<E>::resolve_numeric_field_slot(target_field)?,
                aggregate_kind: AggregateKind::Sum,
                mode: GlobalDistinctFieldReducerMode::Numeric,
            }),
            GlobalDistinctFieldAggregateKind::Avg => Ok(Self {
                target_field,
                field_slot: LoadExecutor::<E>::resolve_numeric_field_slot(target_field)?,
                aggregate_kind: AggregateKind::Avg,
                mode: GlobalDistinctFieldReducerMode::Numeric,
            }),
        }
    }

    #[must_use]
    const fn aggregate_kind(&self) -> AggregateKind {
        self.aggregate_kind
    }

    // Resolve one canonical distinct value for key admission and dedup.
    fn distinct_value<E>(&self, entity: &E) -> Result<Value, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        extract_orderable_field_value(entity, self.target_field, self.field_slot)
            .map_err(AggregateFieldValueError::into_internal_error)
    }

    // Project one admitted distinct entity into one aggregate ingest payload.
    fn distinct_numeric_value<E>(&self, entity: &E) -> Result<Option<Decimal>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        if matches!(self.mode, GlobalDistinctFieldReducerMode::Numeric) {
            return extract_numeric_field_decimal(entity, self.target_field, self.field_slot)
                .map(Some)
                .map_err(AggregateFieldValueError::into_internal_error);
        }

        Ok(None)
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one global DISTINCT field-target grouped aggregate with grouped
    // distinct budget accounting and deterministic reducer behavior.
    pub(in crate::db::executor) fn execute_global_distinct_field_aggregate(
        consistency: MissingRowPolicy,
        ctx: &Context<'_, E>,
        resolved: &mut ResolvedExecutionKeyStream,
        compiled_predicate: Option<&crate::db::predicate::PredicateProgram>,
        grouped_execution_context: &mut ExecutionContext,
        execution_spec: (&str, GlobalDistinctFieldAggregateKind),
        row_counters: (&mut usize, &mut usize),
    ) -> Result<GroupedRow, InternalError> {
        let (scanned_rows, filtered_rows) = row_counters;
        let dispatcher = GlobalDistinctFieldAggregateDispatcher::resolve::<E>(execution_spec)?;
        let mut distinct_values = GroupKeySet::new();

        grouped_execution_context
            .record_implicit_single_group::<E>()
            .map_err(Self::map_group_error)?;

        let mut ingest_all = |ingest_adapter: &mut AggregateIngestAdapter<'_, E>,
                              engine: &mut AggregateEngine<E>|
         -> Result<(), InternalError> {
            let mut pre_key = || KeyStreamLoopControl::Emit;
            let mut on_key =
                |_key, entity: Option<E>| -> Result<KeyStreamLoopControl, InternalError> {
                    let Some(entity) = entity else {
                        return Ok(KeyStreamLoopControl::Emit);
                    };
                    *scanned_rows = scanned_rows.saturating_add(1);
                    if let Some(compiled_predicate) = compiled_predicate
                        && !compiled_predicate.eval(&entity)
                    {
                        return Ok(KeyStreamLoopControl::Emit);
                    }
                    *filtered_rows = filtered_rows.saturating_add(1);

                    let distinct_value = dispatcher.distinct_value(&entity)?;
                    let distinct_key = distinct_value
                        .canonical_key()
                        .map_err(KeyCanonicalError::into_internal_error)?;
                    let distinct_admitted = grouped_execution_context
                        .admit_distinct_key(
                            &mut distinct_values,
                            grouped_execution_context
                                .config()
                                .max_distinct_values_per_group(),
                            distinct_key,
                        )
                        .map_err(Self::map_group_error)?;
                    if !distinct_admitted {
                        return Ok(KeyStreamLoopControl::Emit);
                    }

                    let numeric_value = dispatcher.distinct_numeric_value(&entity)?;
                    let fold_control = ingest_adapter
                        .ingest_global_distinct_value(engine, numeric_value)
                        .map_err(Self::map_group_error)?;
                    if matches!(fold_control, FoldControl::Break) {
                        return Ok(KeyStreamLoopControl::Stop);
                    }

                    Ok(KeyStreamLoopControl::Emit)
                };
            Self::drive_field_entity_stream(
                ctx,
                consistency,
                resolved.key_stream_mut(),
                &mut pre_key,
                &mut on_key,
            )
        };
        let finalize_outputs = execute_aggregate_engine(
            AggregateEngine::new_global_distinct_field(dispatcher.aggregate_kind())
                .map_err(Self::map_group_error)?,
            AggregateExecutionSpec::global_distinct_field(),
            &mut ingest_all,
        )?
        .into_grouped()?;
        if finalize_outputs.len() != 1 {
            return Err(crate::db::error::query_executor_invariant(format!(
                "grouped global DISTINCT finalize must return exactly one grouped output row, found {}",
                finalize_outputs.len()
            )));
        }
        let finalized = finalize_outputs.first().ok_or_else(|| {
            crate::db::error::query_executor_invariant(
                "grouped global DISTINCT finalize output must contain one grouped row",
            )
        })?;
        match finalized.group_key().canonical_value() {
            Value::List(group_key_values) if group_key_values.is_empty() => {}
            value => {
                return Err(crate::db::error::query_executor_invariant(format!(
                    "grouped global DISTINCT finalize row must use empty grouped key, found {value:?}",
                )));
            }
        }
        let aggregate_value = aggregate_output_to_value(finalized.output());

        Ok(GroupedRow::new(Vec::new(), vec![aggregate_value]))
    }
}
