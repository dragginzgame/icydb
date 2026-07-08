mod delete;
mod insert;
mod update;

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        data::{FieldSlot, StructuralPatch},
        executor::{EntityAuthority, MutationMode},
        query::intent::StructuralQuery,
        response::ResponseError,
        schema::{
            AcceptedRowLayoutRuntimeContract, AcceptedSchemaSnapshot, SchemaFieldWritePolicy,
            SchemaInfo, accepted_commit_schema_fingerprint,
            canonicalize_strict_sql_literal_for_persisted_kind, field_type_from_persisted_kind,
            literal_matches_type,
        },
        session::{
            accepted_schema::{AcceptedSaveContract, accepted_save_contract_for_descriptor},
            sql::{
                CompiledSqlCommand, SqlCacheAttribution, SqlStatementResult,
                combined_optional_row_bound,
                execute::write_returning::{
                    sql_write_statement_result, validate_sql_returning_bounds,
                    validate_sql_returning_projection_fields,
                },
                write_policy::{SqlWriteExecutionBounds, SqlWriteReturningBounds},
            },
        },
        sql::parser::{SqlInsertSource, SqlInsertStatement, SqlReturningProjection},
    },
    error::ErrorClass,
    metrics::sink::{MetricsEvent, SqlWriteKind, record},
    sanitize::SanitizeWriteContext,
    traits::{CanisterKind, EntityKind, EntityValue, KeyValueCodec},
    value::Value,
};
use icydb_diagnostic_code::SqlWriteBoundaryCode;

const SQL_WRITE_MUTATION_BATCH_INITIAL_RESERVE_ROWS: usize = 64;

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
    E: PersistedRow<Canister = C> + EntityValue,
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
    E: PersistedRow<Canister = C> + EntityValue,
    C: CanisterKind,
{
    record_sql_write_error::<E, C>(kind, &result);
    SqlCacheAttribution::with_default(result)
}

pub(super) fn execute_compiled_sql_write_with_default_cache<E, C>(
    session: &DbSession<C>,
    compiled: &CompiledSqlCommand,
) -> Option<Result<(SqlStatementResult, SqlCacheAttribution), QueryError>>
where
    E: PersistedRow<Canister = C> + EntityValue,
    C: CanisterKind,
{
    match compiled {
        CompiledSqlCommand::Delete { query, returning } => {
            let result =
                session.execute_sql_delete_statement::<E>(query.as_ref(), returning.as_ref());
            Some(sql_write_statement_result_with_default_cache::<E, C>(
                SqlWriteKind::Delete,
                result,
            ))
        }
        CompiledSqlCommand::Insert(command) => {
            let result = session
                .execute_sql_insert_statement::<E>(command.statement(), command.source_query());
            Some(sql_write_statement_result_with_default_cache::<E, C>(
                sql_insert_write_kind(command.statement()),
                result,
            ))
        }
        CompiledSqlCommand::Update(statement) => {
            let result = session.execute_sql_update_statement::<E>(statement);
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

fn sql_write_key_from_literal<E>(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    value: &Value,
) -> Result<E::Key, QueryError>
where
    E: EntityKind,
{
    if descriptor.primary_key_names().len() > 1 {
        let Value::List(values) = value else {
            return Err(QueryError::sql_write_boundary(
                SqlWriteBoundaryCode::PrimaryKeyLiteralShape,
            ));
        };

        return sql_write_key_from_component_literals::<E>(descriptor, values.as_slice());
    }

    if let Some(key) = <E::Key as KeyValueCodec>::from_key_value(value) {
        return Ok(key);
    }

    let primary_key_kind = descriptor.first_primary_key_kind();
    let normalized = canonicalize_strict_sql_literal_for_persisted_kind(primary_key_kind, value)
        .unwrap_or_else(|| value.clone());

    <E::Key as KeyValueCodec>::from_key_value(&normalized).ok_or_else(|| {
        QueryError::sql_write_boundary(SqlWriteBoundaryCode::PrimaryKeyLiteralIncompatible)
    })
}

fn sql_write_key_from_component_literals<E>(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    values: &[Value],
) -> Result<E::Key, QueryError>
where
    E: EntityKind,
{
    let primary_key_names = descriptor.primary_key_names();
    let primary_key_kinds = descriptor.primary_key_kinds();
    if values.len() != primary_key_names.len() {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::PrimaryKeyLiteralShape,
        ));
    }

    let mut normalized = Vec::with_capacity(values.len());
    for ((_field_name, kind), value) in primary_key_names
        .iter()
        .zip(primary_key_kinds.iter())
        .zip(values.iter())
    {
        let value = canonicalize_strict_sql_literal_for_persisted_kind(kind, value)
            .unwrap_or_else(|| value.clone());

        normalized.push(value);
    }

    let key_value = if normalized.len() == 1 {
        normalized
            .into_iter()
            .next()
            .ok_or_else(QueryError::invariant)?
    } else {
        Value::List(normalized)
    };

    <E::Key as KeyValueCodec>::from_key_value(&key_value).ok_or_else(|| {
        QueryError::sql_write_boundary(SqlWriteBoundaryCode::PrimaryKeyLiteralIncompatible)
    })
}

fn checked_accepted_write_descriptor<E>(
    schema: &AcceptedSchemaSnapshot,
) -> Result<AcceptedRowLayoutRuntimeContract<'_>, QueryError>
where
    E: EntityKind,
{
    let (descriptor, _) =
        AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(schema, E::MODEL)
            .map_err(QueryError::execute)?;

    Ok(descriptor)
}

fn checked_accepted_write_descriptor_for_returning<'a, E>(
    schema: &'a AcceptedSchemaSnapshot,
    returning: Option<&SqlReturningProjection>,
) -> Result<AcceptedRowLayoutRuntimeContract<'a>, QueryError>
where
    E: EntityKind,
{
    let descriptor = checked_accepted_write_descriptor::<E>(schema)?;
    validate_sql_returning_projection_fields(&descriptor, returning)?;

    Ok(descriptor)
}

fn require_sql_write_policy_plan<T>(plan: Option<T>) -> Result<T, QueryError> {
    plan.ok_or_else(QueryError::unsupported_query)
}

fn accepted_sql_write_save_contract<E>(
    schema: &AcceptedSchemaSnapshot,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    schema_info: Option<SchemaInfo>,
) -> Result<AcceptedSaveContract, QueryError>
where
    E: EntityKind,
{
    if let Some(schema_info) = schema_info {
        let row_decode_contract = descriptor.row_decode_contract();
        let mutation_row_decode_contract = row_decode_contract.clone();
        let schema_fingerprint =
            accepted_commit_schema_fingerprint(schema).map_err(QueryError::execute)?;

        return Ok((
            row_decode_contract,
            mutation_row_decode_contract,
            schema_info,
            schema_fingerprint,
        ));
    }

    accepted_save_contract_for_descriptor::<E>(schema, descriptor).map_err(QueryError::execute)
}

fn accepted_write_field_slot(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
) -> Result<FieldSlot, QueryError> {
    let accepted_slot = descriptor
        .field_slot_index_by_name(field_name)
        .ok_or_else(QueryError::invariant)?;

    Ok(FieldSlot::from_validated_index(accepted_slot))
}

fn sql_write_patch_set_accepted_field(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    patch: StructuralPatch,
    field_name: &str,
    value: Value,
) -> Result<StructuralPatch, QueryError> {
    let slot = accepted_write_field_slot(descriptor, field_name)?;

    Ok(patch.set(slot, value))
}

fn write_policy_for_accepted_name(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
) -> Result<SchemaFieldWritePolicy, QueryError> {
    let Some(field) = descriptor.field_by_name(field_name) else {
        return Err(QueryError::invariant());
    };

    Ok(field.write_policy())
}

fn sql_write_value_for_accepted_field(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
    value: &Value,
) -> Result<Value, QueryError> {
    let accepted_kind = descriptor
        .field_kind_by_name(field_name)
        .ok_or_else(QueryError::invariant)?;
    let normalized = canonicalize_strict_sql_literal_for_persisted_kind(accepted_kind, value)
        .unwrap_or_else(|| value.clone());

    let field_type = field_type_from_persisted_kind(accepted_kind);
    if !literal_matches_type(&normalized, &field_type) {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::InvalidFieldLiteral,
        ));
    }

    Ok(normalized)
}

fn reject_explicit_sql_write_to_managed_field(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
) -> Result<(), QueryError> {
    let Ok(policy) = write_policy_for_accepted_name(descriptor, field_name) else {
        return Ok(());
    };

    if policy.write_management().is_some() {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::ExplicitManagedField,
        ));
    }

    Ok(())
}

fn reject_explicit_sql_write_to_generated_field(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
) -> Result<(), QueryError> {
    let Ok(policy) = write_policy_for_accepted_name(descriptor, field_name) else {
        return Ok(());
    };

    if policy.insert_generation().is_some() {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::ExplicitGeneratedField,
        ));
    }

    Ok(())
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[derive(Clone, Copy)]
struct SqlWriteCandidateAccounting {
    semantic_candidates: SqlWriteCandidateRows,
    matched_candidates: SqlWriteCandidateRows,
    mutated_rows: usize,
    returning_rows: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SqlWriteCandidateRows(usize);

impl SqlWriteCandidateRows {
    const fn from_len(len: usize) -> Self {
        Self(len)
    }

    const fn len(self) -> usize {
        self.0
    }

    fn from_delete_count(row_count: u32) -> Self {
        Self(usize::try_from(row_count).unwrap_or(usize::MAX))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SqlWriteProjectedSourceRows(usize);

impl SqlWriteProjectedSourceRows {
    const fn from_len(len: usize) -> Self {
        Self(len)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlWriteCandidateBoundCheck {
    InsertValuesSource,
    MutationBatchHandoff,
    SelectorSourceBatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SqlWriteCandidateDiagnostics {
    projected_source_rows: Option<SqlWriteProjectedSourceRows>,
    semantic_candidates: SqlWriteCandidateRows,
    over_limit_at: Option<SqlWriteCandidateBoundCheck>,
}

impl SqlWriteCandidateDiagnostics {
    const fn within_limit(semantic_candidates: SqlWriteCandidateRows) -> Self {
        Self {
            projected_source_rows: None,
            semantic_candidates,
            over_limit_at: None,
        }
    }

    const fn over_limit(
        semantic_candidates: SqlWriteCandidateRows,
        at: SqlWriteCandidateBoundCheck,
    ) -> Self {
        Self {
            projected_source_rows: None,
            semantic_candidates,
            over_limit_at: Some(at),
        }
    }

    const fn over_limit_at(self) -> Option<SqlWriteCandidateBoundCheck> {
        self.over_limit_at
    }

    const fn projected_source_rows(self) -> Option<SqlWriteProjectedSourceRows> {
        self.projected_source_rows
    }

    const fn with_projected_source_rows(
        self,
        projected_source_rows: Option<SqlWriteProjectedSourceRows>,
    ) -> Self {
        Self {
            projected_source_rows,
            ..self
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SqlWriteCandidateBounds {
    max_rows: Option<u32>,
}

impl SqlWriteCandidateBounds {
    const fn from_max_rows(max_rows: Option<u32>) -> Self {
        Self { max_rows }
    }

    const fn max_rows(self) -> Option<u32> {
        self.max_rows
    }

    fn diagnostics_at(
        self,
        candidate_rows: SqlWriteCandidateRows,
        at: SqlWriteCandidateBoundCheck,
    ) -> SqlWriteCandidateDiagnostics {
        let Some(max_rows) = self.max_rows else {
            return SqlWriteCandidateDiagnostics::within_limit(candidate_rows);
        };
        let max_rows = usize::try_from(max_rows).unwrap_or(usize::MAX);
        if candidate_rows.len() <= max_rows {
            return SqlWriteCandidateDiagnostics::within_limit(candidate_rows);
        }

        SqlWriteCandidateDiagnostics::over_limit(candidate_rows, at)
    }

    fn validate_at(
        self,
        candidate_rows: SqlWriteCandidateRows,
        at: SqlWriteCandidateBoundCheck,
    ) -> Result<SqlWriteCandidateDiagnostics, QueryError> {
        let diagnostics = self.diagnostics_at(candidate_rows, at);
        if diagnostics.over_limit_at().is_none() {
            return Ok(diagnostics);
        }

        Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::StagedRowsTooMany,
        ))
    }
}

fn sql_update_candidate_bounds(
    execution_bounds: Option<SqlWriteExecutionBounds>,
) -> SqlWriteCandidateBounds {
    SqlWriteCandidateBounds::from_max_rows(
        execution_bounds.and_then(|bounds| bounds.max_staged_rows),
    )
}

const fn sql_insert_candidate_bounds(
    execution_bounds: Option<SqlWriteExecutionBounds>,
    returning: bool,
) -> SqlWriteCandidateBounds {
    let Some(execution_bounds) = execution_bounds else {
        return SqlWriteCandidateBounds::from_max_rows(None);
    };

    if !returning {
        return SqlWriteCandidateBounds::from_max_rows(execution_bounds.max_staged_rows);
    }

    SqlWriteCandidateBounds::from_max_rows(combined_optional_row_bound(
        execution_bounds.max_staged_rows,
        execution_bounds.returning.max_rows,
    ))
}

impl SqlWriteCandidateAccounting {
    const fn mutation_batch(
        staged_rows: SqlWriteCandidateRows,
        mutated_rows: usize,
        returning: Option<&SqlReturningProjection>,
    ) -> Self {
        Self {
            semantic_candidates: staged_rows,
            matched_candidates: staged_rows,
            mutated_rows,
            returning_rows: if returning.is_some() { mutated_rows } else { 0 },
        }
    }

    const fn delete_count(candidate_rows: SqlWriteCandidateRows, returning: bool) -> Self {
        Self {
            semantic_candidates: candidate_rows,
            matched_candidates: candidate_rows,
            mutated_rows: candidate_rows.len(),
            returning_rows: if returning { candidate_rows.len() } else { 0 },
        }
    }

    fn staged_metric(self) -> u64 {
        usize_to_u64_saturating(self.semantic_candidates.len())
    }

    fn matched_metric(self) -> u64 {
        usize_to_u64_saturating(self.matched_candidates.len())
    }

    fn mutated_metric(self) -> u64 {
        usize_to_u64_saturating(self.mutated_rows)
    }

    fn returning_metric(self) -> u64 {
        usize_to_u64_saturating(self.returning_rows)
    }
}

struct SqlWriteMutationBatch<K> {
    rows: Vec<(K, StructuralPatch)>,
}

impl<K> SqlWriteMutationBatch<K> {
    const fn new() -> Self {
        Self { rows: Vec::new() }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
        }
    }

    fn reserve(&mut self, additional: usize) {
        self.rows.reserve(additional);
    }

    fn push(&mut self, key: K, patch: StructuralPatch) {
        self.rows.push((key, patch));
    }

    const fn staged_rows(&self) -> SqlWriteCandidateRows {
        SqlWriteCandidateRows(self.rows.len())
    }

    fn into_rows(self) -> Vec<(K, StructuralPatch)> {
        self.rows
    }
}

struct SqlWriteCandidateCollection<K> {
    diagnostics: SqlWriteCandidateDiagnostics,
    rows: SqlWriteMutationBatch<K>,
}

impl<K> SqlWriteCandidateCollection<K> {
    const fn new() -> Self {
        Self {
            diagnostics: SqlWriteCandidateDiagnostics::within_limit(SqlWriteCandidateRows(0)),
            rows: SqlWriteMutationBatch::new(),
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            diagnostics: SqlWriteCandidateDiagnostics::within_limit(SqlWriteCandidateRows(0)),
            rows: SqlWriteMutationBatch::with_capacity(capacity),
        }
    }

    fn reserve(&mut self, additional: usize) {
        self.rows.reserve(additional);
    }

    fn push(&mut self, key: K, patch: StructuralPatch) {
        self.rows.push(key, patch);
        self.diagnostics.semantic_candidates = self.staged_rows();
    }

    const fn staged_rows(&self) -> SqlWriteCandidateRows {
        self.rows.staged_rows()
    }

    const fn record_projected_source_rows(&mut self, source_rows: SqlWriteProjectedSourceRows) {
        self.diagnostics.projected_source_rows = Some(source_rows);
    }

    #[cfg(test)]
    const fn diagnostics(&self) -> SqlWriteCandidateDiagnostics {
        self.diagnostics
    }

    fn validate_staged_rows_at(
        &mut self,
        bounds: SqlWriteCandidateBounds,
        at: SqlWriteCandidateBoundCheck,
    ) -> Result<SqlWriteCandidateRows, QueryError> {
        let staged_rows = self.staged_rows();
        self.diagnostics = bounds
            .validate_at(staged_rows, at)?
            .with_projected_source_rows(self.diagnostics.projected_source_rows());

        Ok(staged_rows)
    }

    fn into_batch(self) -> SqlWriteMutationBatch<K> {
        self.rows
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
) -> Result<SqlStatementResult, QueryError>
where
    E: PersistedRow + EntityValue,
{
    record_sql_write_mutation_metrics(entity_path, kind, staged_rows, entities.len(), returning);

    sql_write_statement_result::<E>(entities, returning, descriptor)
}

struct SqlWriteMutationExecution<E>
where
    E: PersistedRow + EntityValue,
{
    rows: SqlWriteMutationBatch<E::Key>,
    staged_rows: SqlWriteCandidateRows,
    kind: SqlWriteKind,
    mode: MutationMode,
    context: SanitizeWriteContext,
    returning_bounds: Option<SqlWriteReturningBounds>,
    save_schema_info: Option<SchemaInfo>,
}

impl<E> SqlWriteMutationExecution<E>
where
    E: PersistedRow + EntityValue,
{
    fn from_bounded_collection(
        mut collection: SqlWriteCandidateCollection<E::Key>,
        bounds: SqlWriteCandidateBounds,
        kind: SqlWriteKind,
        mode: MutationMode,
        context: SanitizeWriteContext,
        returning_bounds: Option<SqlWriteReturningBounds>,
        save_schema_info: Option<SchemaInfo>,
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
            save_schema_info,
        })
    }
}

impl<C: CanisterKind> DbSession<C> {
    fn accepted_sql_write_authority_schema_info<E>(
        schema: &AcceptedSchemaSnapshot,
    ) -> Result<(EntityAuthority, SchemaInfo), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let authority =
            Self::accepted_entity_authority_for_schema::<E>(schema).map_err(QueryError::execute)?;
        let schema_info = authority
            .accepted_schema_info()
            .ok_or_else(QueryError::invariant)?
            .clone();

        Ok((authority, schema_info))
    }

    fn with_checked_accepted_write_descriptor_for_returning<E, T>(
        &self,
        returning: Option<&SqlReturningProjection>,
        run: impl for<'a> FnOnce(
            &'a AcceptedSchemaSnapshot,
            AcceptedRowLayoutRuntimeContract<'a>,
        ) -> Result<T, QueryError>,
    ) -> Result<T, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let schema = self
            .ensure_accepted_schema_snapshot::<E>()
            .map_err(QueryError::execute)?;
        let descriptor = checked_accepted_write_descriptor_for_returning::<E>(&schema, returning)?;

        run(&schema, descriptor)
    }

    fn collect_bounded_sql_write_candidate_collection_from_structural_query<K>(
        &self,
        schema: &AcceptedSchemaSnapshot,
        authority: EntityAuthority,
        query: &StructuralQuery,
        bounds: SqlWriteCandidateBounds,
        mut row_to_patch: impl FnMut(&[Value]) -> Result<(K, StructuralPatch), QueryError>,
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
        row_to_patch: &mut impl FnMut(&[Value]) -> Result<(K, StructuralPatch), QueryError>,
    ) -> Result<SqlWriteCandidateCollection<K>, QueryError> {
        let (payload, _) = self
            .execute_sql_projection_from_structural_query_without_sql_compiled_cache(
                query.clone(),
                authority,
                schema,
            )?;
        let (_, _, projected_rows, _) = payload.into_components();
        let mut rows = SqlWriteCandidateCollection::with_capacity(
            projected_rows
                .len()
                .min(SQL_WRITE_MUTATION_BATCH_INITIAL_RESERVE_ROWS),
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
        schema: &AcceptedSchemaSnapshot,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        execution: SqlWriteMutationExecution<E>,
        returning: Option<&SqlReturningProjection>,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (
            row_decode_contract,
            mutation_row_decode_contract,
            accepted_schema_info,
            accepted_schema_fingerprint,
        ) = accepted_sql_write_save_contract::<E>(schema, descriptor, execution.save_schema_info)?;
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
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SqlWriteCandidateAccounting, SqlWriteCandidateBoundCheck, SqlWriteCandidateBounds,
        SqlWriteCandidateCollection, SqlWriteCandidateRows, SqlWriteProjectedSourceRows,
    };
    use crate::db::data::StructuralPatch;
    use icydb_diagnostic_code::{DiagnosticDetail, SqlWriteBoundaryCode};

    #[test]
    fn sql_write_candidate_row_bound_accepts_unbounded_and_within_limit() {
        SqlWriteCandidateBounds::from_max_rows(None)
            .validate_at(
                SqlWriteCandidateRows(2),
                SqlWriteCandidateBoundCheck::MutationBatchHandoff,
            )
            .expect("unbounded candidate rows should be accepted");
        SqlWriteCandidateBounds::from_max_rows(Some(2))
            .validate_at(
                SqlWriteCandidateRows(2),
                SqlWriteCandidateBoundCheck::MutationBatchHandoff,
            )
            .expect("candidate rows equal to the bound should be accepted");
    }

    #[test]
    fn sql_write_candidate_row_bound_rejects_over_limit() {
        let err = SqlWriteCandidateBounds::from_max_rows(Some(1))
            .validate_at(
                SqlWriteCandidateRows(2),
                SqlWriteCandidateBoundCheck::MutationBatchHandoff,
            )
            .expect_err("candidate rows over the bound should reject");

        assert_eq!(
            err.diagnostic().detail(),
            Some(&DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::StagedRowsTooMany,
            }),
        );
    }

    #[test]
    fn sql_write_candidate_bounds_report_over_limit_stage() {
        let diagnostics = SqlWriteCandidateBounds::from_max_rows(Some(1)).diagnostics_at(
            SqlWriteCandidateRows(2),
            SqlWriteCandidateBoundCheck::SelectorSourceBatch,
        );

        assert_eq!(diagnostics.semantic_candidates, SqlWriteCandidateRows(2));
        assert_eq!(
            diagnostics.over_limit_at(),
            Some(SqlWriteCandidateBoundCheck::SelectorSourceBatch),
        );

        let within_limit = SqlWriteCandidateBounds::from_max_rows(Some(2)).diagnostics_at(
            SqlWriteCandidateRows(2),
            SqlWriteCandidateBoundCheck::InsertValuesSource,
        );

        assert_eq!(within_limit.semantic_candidates, SqlWriteCandidateRows(2));
        assert_eq!(within_limit.over_limit_at(), None);
    }

    #[test]
    fn sql_write_candidate_collection_validates_staged_rows_from_buffer() {
        let mut rows = SqlWriteCandidateCollection::<u64>::new();
        rows.push(1, StructuralPatch::new());
        rows.push(2, StructuralPatch::new());

        let staged_rows = rows
            .validate_staged_rows_at(
                SqlWriteCandidateBounds::from_max_rows(Some(2)),
                SqlWriteCandidateBoundCheck::MutationBatchHandoff,
            )
            .expect("batch staged rows at the bound should be accepted");

        assert_eq!(staged_rows.len(), 2);
        assert!(
            rows.validate_staged_rows_at(
                SqlWriteCandidateBounds::from_max_rows(Some(1)),
                SqlWriteCandidateBoundCheck::MutationBatchHandoff,
            )
            .is_err(),
            "batch staged rows over the bound should reject",
        );
    }

    #[test]
    fn sql_write_candidate_collection_tracks_projected_source_rows() {
        let mut rows = SqlWriteCandidateCollection::<u64>::with_capacity(3);
        rows.record_projected_source_rows(SqlWriteProjectedSourceRows::from_len(3));
        rows.push(1, StructuralPatch::new());
        rows.push(2, StructuralPatch::new());

        rows.validate_staged_rows_at(
            SqlWriteCandidateBounds::from_max_rows(Some(2)),
            SqlWriteCandidateBoundCheck::SelectorSourceBatch,
        )
        .expect("selector source rows at the semantic candidate bound should be accepted");

        let diagnostics = rows.diagnostics();
        assert_eq!(diagnostics.semantic_candidates, SqlWriteCandidateRows(2));
        assert_eq!(
            diagnostics.projected_source_rows(),
            Some(SqlWriteProjectedSourceRows::from_len(3)),
        );
        assert_eq!(diagnostics.over_limit_at(), None);
    }

    #[test]
    fn sql_write_candidate_accounting_counts_delete_rows_and_returning() {
        let count = SqlWriteCandidateAccounting::delete_count(SqlWriteCandidateRows(3), false);
        assert_eq!(count.staged_metric(), 3);
        assert_eq!(count.matched_metric(), 3);
        assert_eq!(count.mutated_metric(), 3);
        assert_eq!(count.returning_metric(), 0);

        let returning = SqlWriteCandidateAccounting::delete_count(SqlWriteCandidateRows(3), true);
        assert_eq!(returning.staged_metric(), 3);
        assert_eq!(returning.matched_metric(), 3);
        assert_eq!(returning.mutated_metric(), 3);
        assert_eq!(returning.returning_metric(), 3);
    }
}
