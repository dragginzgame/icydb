mod authority;
mod candidate;
mod delete;
mod insert;
mod update;

use crate::{
    db::{
        DbSession, QueryError,
        data::AcceptedMutationIntentPatch,
        executor::{EntityAuthority, StructuralProjectionScanBudget},
        query::intent::StructuralQuery,
        response::ResponseError,
        schema::{AcceptedRowLayoutRuntimeContract, AcceptedSchemaSnapshot},
        session::{
            AcceptedSchemaCatalogContext, AcceptedStructuralMutation,
            AcceptedStructuralMutationTarget,
            sql::{
                CompiledSqlCommand, SqlCacheAttribution, SqlCompiledCommandSurface,
                SqlStatementResult,
                execute::write_returning::{
                    projection_labels_from_accepted_write_descriptor,
                    sql_returning_statement_projection, validate_sql_materialized_returning_bounds,
                },
                write_policy::SqlWriteReturningBounds,
            },
            write::AcceptedStructuralMutationRow,
        },
        sql::parser::{SqlInsertSource, SqlInsertStatement, SqlReturningProjection},
        write_context::{AcceptedWriteContext, MutationMode},
    },
    error::ErrorClass,
    metrics::sink::{MetricsEvent, SqlWriteKind, record},
    traits::CanisterKind,
    value::Value,
};
use authority::{
    reject_explicit_sql_write_to_generated_field, reject_explicit_sql_write_to_managed_field,
    require_sql_write_policy_plan, sql_write_input_for_accepted_field,
    sql_write_patch_set_accepted_field, sql_write_patch_set_insert_default,
    sql_write_patch_set_update_default,
};
use candidate::{
    SqlWriteCandidateAccounting, SqlWriteCandidateBoundCheck, SqlWriteCandidateBounds,
    SqlWriteCandidateCollection, SqlWriteCandidateRows, SqlWriteMutationBatch,
    SqlWriteProjectedSourceRows, sql_exact_update_candidate_bounds, sql_insert_candidate_bounds,
    sql_update_candidate_bounds, sql_write_candidate_collection_capacity,
};

// Collapse SQL execution failures into the stable error taxonomy used by the
// public metrics report instead of exposing internal query-error variants.
const fn sql_write_error_class(error: &QueryError) -> ErrorClass {
    match error {
        QueryError::Execute(err) => err.as_internal().class(),
        QueryError::Response(ResponseError::NotFound { .. }) => ErrorClass::NotFound,
        QueryError::Response(ResponseError::NotUnique { .. }) => ErrorClass::Conflict,
        QueryError::Validate(_)
        | QueryError::Plan(_)
        | QueryError::Intent(_)
        | QueryError::AccessRequirement(_) => ErrorClass::Unsupported,
    }
}

// Preserve the important INSERT shape distinction because `INSERT ... SELECT`
// has very different execution and debugging characteristics from VALUES.
const fn sql_insert_write_kind(statement: &SqlInsertStatement) -> SqlWriteKind {
    match &statement.source {
        SqlInsertSource::Values(_) | SqlInsertSource::DefaultValues => SqlWriteKind::Insert,
        SqlInsertSource::Select(_) => SqlWriteKind::InsertSelect,
    }
}

// Record only rejected SQL writes at the statement boundary. Successful writes
// are counted by the write executors after they know row cardinalities.
fn record_sql_write_error(
    entity_path: &'static str,
    kind: SqlWriteKind,
    result: &Result<SqlStatementResult, QueryError>,
) {
    if let Err(error) = result {
        record(MetricsEvent::SqlWriteError {
            entity_path,
            kind,
            class: sql_write_error_class(error),
        });
    }
}

fn sql_write_statement_result_with_default_cache(
    entity_path: &'static str,
    kind: SqlWriteKind,
    result: Result<SqlStatementResult, QueryError>,
) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError> {
    record_sql_write_error(entity_path, kind, &result);
    SqlCacheAttribution::with_default(result)
}

pub(super) fn execute_compiled_sql_write_with_default_cache<C>(
    session: &DbSession<C>,
    compiled: &CompiledSqlCommand,
    catalog: Option<&AcceptedSchemaCatalogContext>,
    surface: Option<SqlCompiledCommandSurface>,
) -> Option<Result<(SqlStatementResult, SqlCacheAttribution), QueryError>>
where
    C: CanisterKind,
{
    let entity_path = catalog.map_or("<unresolved-sql-entity>", |catalog| {
        catalog.identity().entity_path()
    });
    match compiled {
        CompiledSqlCommand::Delete { query, returning } => {
            let result = session.execute_sql_delete_statement(
                query.as_ref(),
                returning.as_ref(),
                catalog,
            );
            Some(sql_write_statement_result_with_default_cache(
                entity_path,
                SqlWriteKind::Delete,
                result,
            ))
        }
        CompiledSqlCommand::Insert(command) => {
            let result = if surface == Some(SqlCompiledCommandSurface::Mutation) {
                session.execute_sql_insert_statement_with_update_surface_bounds(
                    command.statement(),
                    command.source_query(),
                    catalog,
                )
            } else {
                session.execute_sql_insert_statement(
                    command.statement(),
                    command.source_query(),
                    catalog,
                )
            };
            Some(sql_write_statement_result_with_default_cache(
                entity_path,
                sql_insert_write_kind(command.statement()),
                result,
            ))
        }
        CompiledSqlCommand::Update(_statement) => {
            Some(sql_write_statement_result_with_default_cache(
                entity_path,
                SqlWriteKind::Update,
                Err(QueryError::sql_surface_mismatch(
                    icydb_diagnostic_code::SqlSurfaceMismatchCode::MutationRequiresExplicitUpdateIntent,
                )),
            ))
        }
        CompiledSqlCommand::Select { .. }
        | CompiledSqlCommand::GlobalAggregate { .. }
        | CompiledSqlCommand::DescribeEntity
        | CompiledSqlCommand::ShowConstraintsEntity
        | CompiledSqlCommand::ShowIndexesEntity
        | CompiledSqlCommand::ShowColumnsEntity
        | CompiledSqlCommand::ShowEntities { .. }
        | CompiledSqlCommand::ShowStores { .. }
        | CompiledSqlCommand::ShowMemory => None,
        #[cfg(feature = "sql-explain")]
        CompiledSqlCommand::Explain(..) => None,
    }
}

fn record_sql_write_metrics(
    entity_path: &'static str,
    kind: SqlWriteKind,
    accounting: SqlWriteCandidateAccounting,
) {
    record(MetricsEvent::SqlWrite {
        entity_path,
        kind,
        staged_rows: accounting.staged_metric(),
        matched_rows: accounting.matched_metric(),
        mutated_rows: accounting.mutated_metric(),
        returning_rows: accounting.returning_metric(),
    });
}

fn record_sql_write_mutation_metrics(
    entity_path: &'static str,
    kind: SqlWriteKind,
    staged_rows: SqlWriteCandidateRows,
    mutated_rows: usize,
    returning: Option<&SqlReturningProjection>,
) {
    record_sql_write_metrics(
        entity_path,
        kind,
        SqlWriteCandidateAccounting::mutation_batch(staged_rows, mutated_rows, returning),
    );
}

fn sql_write_mutation_statement_result(
    entity_path: &'static str,
    kind: SqlWriteKind,
    staged_rows: SqlWriteCandidateRows,
    rows: Vec<Vec<Value>>,
    returning: Option<&SqlReturningProjection>,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    catalog: &AcceptedSchemaCatalogContext,
) -> Result<SqlStatementResult, QueryError> {
    let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);
    record_sql_write_mutation_metrics(entity_path, kind, staged_rows, rows.len(), returning);
    match returning {
        None => Ok(SqlStatementResult::Count { row_count }),
        Some(returning) => sql_returning_statement_projection(
            catalog.enum_catalog(),
            projection_labels_from_accepted_write_descriptor(descriptor),
            rows,
            row_count,
            returning,
        ),
    }
}

struct SqlWriteMutationExecution {
    rows: SqlWriteMutationBatch<AcceptedStructuralMutationTarget>,
    staged_rows: SqlWriteCandidateRows,
    kind: SqlWriteKind,
    mode: MutationMode,
    context: AcceptedWriteContext,
    returning_bounds: Option<SqlWriteReturningBounds>,
}

impl SqlWriteMutationExecution {
    fn from_bounded_collection(
        mut collection: SqlWriteCandidateCollection<AcceptedStructuralMutationTarget>,
        bounds: SqlWriteCandidateBounds,
        kind: SqlWriteKind,
        mode: MutationMode,
        context: AcceptedWriteContext,
        returning_bounds: Option<SqlWriteReturningBounds>,
    ) -> Result<Self, QueryError> {
        let staged_rows = collection
            .validate_staged_rows_at(bounds, SqlWriteCandidateBoundCheck::MutationBatchHandoff)?;
        let rows = collection.into_batch();

        Ok(Self {
            rows,
            staged_rows,
            kind,
            mode,
            context,
            returning_bounds,
        })
    }
}

impl<C: CanisterKind> DbSession<C> {
    fn collect_bounded_sql_write_candidate_collection_from_structural_query<K>(
        &self,
        schema: &AcceptedSchemaSnapshot,
        authority: EntityAuthority,
        query: &StructuralQuery,
        bounds: SqlWriteCandidateBounds,
        scan_budget: Option<StructuralProjectionScanBudget>,
        mut row_to_patch: impl FnMut(&[Value]) -> Result<(K, AcceptedMutationIntentPatch), QueryError>,
    ) -> Result<SqlWriteCandidateCollection<K>, QueryError> {
        self.collect_sql_write_candidate_collection_from_structural_query_with_bounds(
            schema,
            authority,
            query,
            bounds,
            scan_budget,
            &mut row_to_patch,
        )
    }

    fn collect_sql_write_candidate_collection_from_structural_query_with_bounds<K>(
        &self,
        schema: &AcceptedSchemaSnapshot,
        authority: EntityAuthority,
        query: &StructuralQuery,
        bounds: SqlWriteCandidateBounds,
        scan_budget: Option<StructuralProjectionScanBudget>,
        row_to_patch: &mut impl FnMut(&[Value]) -> Result<(K, AcceptedMutationIntentPatch), QueryError>,
    ) -> Result<SqlWriteCandidateCollection<K>, QueryError> {
        let (payload, _) = match scan_budget {
            Some(scan_budget) => self
                .execute_primary_only_sql_projection_from_structural_query_with_scan_budget(
                    query.clone(),
                    authority,
                    schema,
                    scan_budget,
                ),
            None => self.execute_sql_projection_from_structural_query_without_sql_compiled_cache(
                query.clone(),
                authority,
                schema,
            ),
        }?;
        let (_, _, projected_rows, _) = payload.into_components();
        let mut rows = SqlWriteCandidateCollection::with_capacity(
            sql_write_candidate_collection_capacity(projected_rows.as_slice()),
        );
        rows.record_projected_source_rows(SqlWriteProjectedSourceRows::from_len(
            projected_rows.len(),
        ));
        for row in projected_rows {
            let (key, patch) = row_to_patch(row.as_slice())?;
            rows.push(key, patch);
            rows.validate_staged_rows_at(bounds, SqlWriteCandidateBoundCheck::SelectorSourceBatch)?;
        }

        Ok(rows)
    }

    fn execute_sql_write_mutation_batch(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        execution: SqlWriteMutationExecution,
        returning: Option<&SqlReturningProjection>,
    ) -> Result<SqlStatementResult, QueryError> {
        let entity_path = catalog.identity().entity_path();
        let rows = execution
            .rows
            .into_rows()
            .into_iter()
            .map(|(target, patch)| AcceptedStructuralMutation::new(target, patch))
            .collect();
        let rows = self
            .execute_accepted_structural_save_batch(
                catalog,
                descriptor,
                execution.mode,
                rows,
                execution.context.operation_timestamp(),
            )
            .map_err(QueryError::execute)?;
        let rows = rows
            .into_iter()
            .map(AcceptedStructuralMutationRow::into_values)
            .collect::<Vec<_>>();
        if let Some(returning) = returning {
            validate_sql_materialized_returning_bounds(
                catalog.snapshot().persisted_snapshot().entity_name(),
                &projection_labels_from_accepted_write_descriptor(descriptor),
                rows.as_slice(),
                u32::try_from(rows.len()).unwrap_or(u32::MAX),
                returning,
                catalog.enum_catalog(),
                execution.returning_bounds,
            )
            .map_err(QueryError::execute)?;
        }

        sql_write_mutation_statement_result(
            entity_path,
            execution.kind,
            execution.staged_rows,
            rows,
            returning,
            descriptor,
            catalog,
        )
    }
}
