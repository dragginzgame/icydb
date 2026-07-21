mod authority;
mod candidate;
mod delete;
mod insert;
mod update;

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        data::AuthoredStructuralPatch,
        executor::{EntityAuthority, MutationMode},
        query::intent::StructuralQuery,
        response::ResponseError,
        schema::{AcceptedRowLayoutRuntimeContract, AcceptedSchemaSnapshot},
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                CompiledSqlCommand, SqlCacheAttribution, SqlCompiledCommandSurface,
                SqlStatementResult,
                execute::write_returning::{
                    sql_write_statement_result, validate_sql_returning_bounds,
                },
                write_policy::SqlWriteReturningBounds,
            },
        },
        sql::parser::{SqlInsertSource, SqlInsertStatement, SqlReturningProjection},
    },
    error::ErrorClass,
    metrics::sink::{MetricsEvent, SqlWriteKind, record},
    sanitize::SanitizeWriteContext,
    traits::CanisterKind,
    value::Value,
};
use authority::{
    accepted_sql_write_save_contract, reject_explicit_sql_write_to_generated_field,
    reject_explicit_sql_write_to_managed_field, require_sql_write_policy_plan,
    sql_write_input_for_accepted_field, sql_write_key_from_component_literals,
    sql_write_key_from_literal, sql_write_patch_set_accepted_field,
};
use candidate::{
    SqlWriteCandidateAccounting, SqlWriteCandidateBoundCheck, SqlWriteCandidateBounds,
    SqlWriteCandidateCollection, SqlWriteCandidateRows, SqlWriteMutationBatch,
    SqlWriteProjectedSourceRows, sql_insert_candidate_bounds, sql_update_candidate_bounds,
    sql_write_candidate_collection_capacity,
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
        SqlInsertSource::Values(_) => SqlWriteKind::Insert,
        SqlInsertSource::Select(_) => SqlWriteKind::InsertSelect,
    }
}

// Record only rejected SQL writes at the statement boundary. Successful writes
// are counted by the write executors after they know row cardinalities.
fn record_sql_write_error<E, C>(kind: SqlWriteKind, result: &Result<SqlStatementResult, QueryError>)
where
    E: PersistedRow<Canister = C>,
    C: CanisterKind,
{
    if let Err(error) = result {
        record(MetricsEvent::SqlWriteError {
            entity_path: E::PATH,
            kind,
            class: sql_write_error_class(error),
        });
    }
}

fn sql_write_statement_result_with_default_cache<E, C>(
    kind: SqlWriteKind,
    result: Result<SqlStatementResult, QueryError>,
) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
where
    E: PersistedRow<Canister = C>,
    C: CanisterKind,
{
    record_sql_write_error::<E, C>(kind, &result);
    SqlCacheAttribution::with_default(result)
}

pub(super) fn execute_compiled_sql_write_with_default_cache<E, C>(
    session: &DbSession<C>,
    compiled: &CompiledSqlCommand,
    catalog: Option<&AcceptedSchemaCatalogContext>,
    surface: Option<SqlCompiledCommandSurface>,
) -> Option<Result<(SqlStatementResult, SqlCacheAttribution), QueryError>>
where
    E: PersistedRow<Canister = C>,
    C: CanisterKind,
{
    match compiled {
        CompiledSqlCommand::Delete { query, returning } => {
            let result = session.execute_sql_delete_statement::<E>(
                query.as_ref(),
                returning.as_ref(),
                catalog,
            );
            Some(sql_write_statement_result_with_default_cache::<E, C>(
                SqlWriteKind::Delete,
                result,
            ))
        }
        CompiledSqlCommand::Insert(command) => {
            let result = if surface == Some(SqlCompiledCommandSurface::Update) {
                session.execute_sql_insert_statement_with_update_surface_bounds::<E>(
                    command.statement(),
                    command.source_query(),
                    catalog,
                )
            } else {
                session.execute_sql_insert_statement::<E>(
                    command.statement(),
                    command.source_query(),
                    catalog,
                )
            };
            Some(sql_write_statement_result_with_default_cache::<E, C>(
                sql_insert_write_kind(command.statement()),
                result,
            ))
        }
        CompiledSqlCommand::Update(statement) => {
            let result = session.execute_trusted_sql_mutation_statement::<E>(statement, catalog);
            Some(sql_write_statement_result_with_default_cache::<E, C>(
                SqlWriteKind::Update,
                result,
            ))
        }
        CompiledSqlCommand::Select { .. }
        | CompiledSqlCommand::GlobalAggregate { .. }
        | CompiledSqlCommand::DescribeEntity
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

fn sql_write_mutation_statement_result<E>(
    entity_path: &'static str,
    kind: SqlWriteKind,
    staged_rows: SqlWriteCandidateRows,
    entities: Vec<E>,
    returning: Option<&SqlReturningProjection>,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    catalog: &AcceptedSchemaCatalogContext,
) -> Result<SqlStatementResult, QueryError>
where
    E: PersistedRow,
{
    record_sql_write_mutation_metrics(entity_path, kind, staged_rows, entities.len(), returning);

    sql_write_statement_result::<E>(
        entities,
        returning,
        descriptor,
        catalog.value_catalog_handle(),
    )
}

struct SqlWriteMutationExecution<E>
where
    E: PersistedRow,
{
    rows: SqlWriteMutationBatch<E::Key>,
    staged_rows: SqlWriteCandidateRows,
    kind: SqlWriteKind,
    mode: MutationMode,
    context: SanitizeWriteContext,
    returning_bounds: Option<SqlWriteReturningBounds>,
}

impl<E> SqlWriteMutationExecution<E>
where
    E: PersistedRow,
{
    fn from_bounded_collection(
        mut collection: SqlWriteCandidateCollection<E::Key>,
        bounds: SqlWriteCandidateBounds,
        kind: SqlWriteKind,
        mode: MutationMode,
        context: SanitizeWriteContext,
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
        mut row_to_patch: impl FnMut(&[Value]) -> Result<(K, AuthoredStructuralPatch), QueryError>,
    ) -> Result<SqlWriteCandidateCollection<K>, QueryError> {
        self.collect_sql_write_candidate_collection_from_structural_query_with_bounds(
            schema,
            authority,
            query,
            bounds,
            &mut row_to_patch,
        )
    }

    fn collect_sql_write_candidate_collection_from_structural_query_with_bounds<K>(
        &self,
        schema: &AcceptedSchemaSnapshot,
        authority: EntityAuthority,
        query: &StructuralQuery,
        bounds: SqlWriteCandidateBounds,
        row_to_patch: &mut impl FnMut(&[Value]) -> Result<(K, AuthoredStructuralPatch), QueryError>,
    ) -> Result<SqlWriteCandidateCollection<K>, QueryError> {
        let (payload, _) = self
            .execute_sql_projection_from_structural_query_without_sql_compiled_cache(
                query.clone(),
                authority,
                schema,
            )?;
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

    fn execute_sql_write_mutation_batch<E>(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        execution: SqlWriteMutationExecution<E>,
        returning: Option<&SqlReturningProjection>,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let (
            row_decode_contract,
            mutation_row_decode_contract,
            accepted_schema_info,
            accepted_schema_fingerprint,
        ) = accepted_sql_write_save_contract::<E>(catalog, descriptor);
        let entities = self
            .execute_save_with_checked_accepted_row_contract::<E, _, _>(
                row_decode_contract,
                accepted_schema_info,
                accepted_schema_fingerprint,
                |save| {
                    save.apply_internal_lowered_structural_mutation_batch_with_precommit(
                        execution.mode,
                        execution.rows.into_rows(),
                        execution.context,
                        mutation_row_decode_contract,
                        |entities| {
                            validate_sql_returning_bounds(
                                E::MODEL.name(),
                                entities,
                                returning,
                                descriptor,
                                catalog.value_catalog_handle(),
                                execution.returning_bounds,
                            )
                        },
                    )
                },
                std::convert::identity,
            )
            .map_err(QueryError::execute)?;

        sql_write_mutation_statement_result::<E>(
            E::PATH,
            execution.kind,
            execution.staged_rows,
            entities,
            returning,
            descriptor,
            catalog,
        )
    }
}
