//! Module: db::executor::projection::tests
//! Covers scalar and grouped projection evaluation behavior.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod grouped;
mod materialize;
mod scalar;

#[cfg(feature = "sql")]
use crate::db::response::ProjectedRow;
use crate::{
    db::query::{
        builder::aggregate::{count, sum},
        plan::{
            EffectiveRuntimeFilterProgram, FieldSlot, GroupedAggregateExecutionSpec,
            expr::{
                Alias, BinaryOp, CompiledExpr, CompiledExprValueReader, Expr, FieldId, FieldPath,
                ProjectionField, ProjectionSpec, ScalarProjectionExpr,
            },
        },
    },
    db::{
        data::{
            CanonicalRow, CanonicalSlotReader, DataKey, DataRow, SlotReader, StructuralSlotReader,
        },
        executor::{
            ProjectionMaterializationMetricsRecorder, StructuralCursorPage,
            terminal::{RetainedSlotRow, RowLayout},
        },
        schema::SchemaInfo,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::{
        field::{FieldKind, FieldStorageDecode},
        index::IndexModel,
    },
    traits::{
        EntitySchema, EntityValue, FieldTypeMeta, PersistedFieldSlotCodec, RuntimeValueDecode,
        RuntimeValueEncode,
    },
    types::Ulid,
    value::{OutputValue, Value},
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;
use std::{borrow::Cow, cell::RefCell, cmp::Ordering};

use super::{
    GroupedRowView, PreparedProjectionPlan, PreparedProjectionShape, ProjectionEvalError,
    compile_grouped_projection_expr, compile_grouped_projection_plan,
    evaluate_grouped_projection_values,
};
#[cfg(feature = "sql")]
use super::{
    count_borrowed_data_row_views_for_test, count_borrowed_identity_data_row_views_for_test,
    count_borrowed_slot_row_views_for_test, project, project_rows_from_projection,
};
use crate::db::{
    executor::projection::eval::{
        eval_compiled_expr_with_required_slot_reader_cow, eval_compiled_expr_with_value_reader,
        eval_effective_runtime_filter_program_with_slot_reader,
    },
    query::plan::expr::compile_scalar_projection_expr,
};

const EMPTY_INDEX_FIELDS: [&str; 0] = [];
const EMPTY_INDEX: IndexModel = IndexModel::generated(
    "query::executor::projection::idx_empty",
    "query::executor::projection::Store",
    &EMPTY_INDEX_FIELDS,
    false,
);

fn output(value: Value) -> OutputValue {
    OutputValue::from(value)
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct ProjectionEvalEntity {
    id: Ulid,
    rank: i64,
    flag: bool,
    label: String,
    profile: ProjectionEvalProfile,
}

impl Default for ProjectionEvalEntity {
    fn default() -> Self {
        Self {
            id: Ulid::from_u128(0),
            rank: 0,
            flag: false,
            label: String::new(),
            profile: ProjectionEvalProfile::default(),
        }
    }
}

///
/// ProjectionEvalProfile
///
/// ProjectionEvalProfile is the typed structured field used by projection
/// evaluator tests.
/// It lowers to the runtime `Value::Map` shape needed by expression tests
/// without making the dynamic `Value` union a persisted field type.
///

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
struct ProjectionEvalProfile {
    name: String,
    rank: i64,
    score: u64,
    details_flag: bool,
}

impl FieldTypeMeta for ProjectionEvalProfile {
    const KIND: FieldKind = FieldKind::Structured { queryable: false };
    const STORAGE_DECODE: FieldStorageDecode = FieldStorageDecode::Value;
}

impl RuntimeValueEncode for ProjectionEvalProfile {
    fn to_value(&self) -> Value {
        Value::Map(vec![
            (
                Value::Text("name".to_string()),
                Value::Text(self.name.clone()),
            ),
            (Value::Text("rank".to_string()), Value::Int(self.rank)),
            (Value::Text("score".to_string()), Value::Uint(self.score)),
            (
                Value::Text("details".to_string()),
                Value::Map(vec![(
                    Value::Text("flag".to_string()),
                    Value::Bool(self.details_flag),
                )]),
            ),
        ])
    }
}

impl RuntimeValueDecode for ProjectionEvalProfile {
    fn from_value(value: &Value) -> Option<Self> {
        let Value::Map(entries) = value else {
            return None;
        };

        let name = projection_profile_entry(entries, "name")?;
        let rank = projection_profile_entry(entries, "rank")?;
        let score = projection_profile_entry(entries, "score")?;
        let details = projection_profile_entry(entries, "details")?;
        let Value::Text(name) = name else {
            return None;
        };
        let Value::Int(rank) = rank else {
            return None;
        };
        let Value::Uint(score) = score else {
            return None;
        };
        let Value::Map(details) = details else {
            return None;
        };
        let Value::Bool(details_flag) = projection_profile_entry(details, "flag")? else {
            return None;
        };

        Some(Self {
            name: name.clone(),
            rank: *rank,
            score: *score,
            details_flag: *details_flag,
        })
    }
}

impl PersistedFieldSlotCodec for ProjectionEvalProfile {
    fn encode_persisted_slot(&self, field_name: &'static str) -> Result<Vec<u8>, InternalError> {
        crate::db::encode_persisted_slot_payload_by_meta(self, field_name)
    }

    fn decode_persisted_slot(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        crate::db::decode_persisted_slot_payload_by_meta(bytes, field_name)
    }

    fn encode_persisted_option_slot(
        value: &Option<Self>,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        crate::db::encode_persisted_option_slot_payload_by_meta(value, field_name)
    }

    fn decode_persisted_option_slot(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        crate::db::decode_persisted_option_slot_payload_by_meta(bytes, field_name)
    }
}

fn projection_profile_entry<'a>(entries: &'a [(Value, Value)], field: &str) -> Option<&'a Value> {
    entries.iter().find_map(|(key, value)| match key {
        Value::Text(key) if key == field => Some(value),
        _ => None,
    })
}

crate::test_canister! {
    ident = ProjectionEvalCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = ProjectionEvalStore,
    canister = ProjectionEvalCanister,
}

crate::test_entity_schema! {
    ident = ProjectionEvalEntity,
    id = Ulid,
    id_field = id,
    entity_name = "ProjectionEvalEntity",
    entity_tag = crate::testing::PROJECTION_EVAL_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("rank", FieldKind::Int),
        ("flag", FieldKind::Bool),
        ("label", FieldKind::Text { max_len: None }),
        (
            "profile",
            FieldKind::Structured { queryable: false },
            FieldStorageDecode::Value
        ),
    ],
    indexes = [&EMPTY_INDEX],
    store = ProjectionEvalStore,
    canister = ProjectionEvalCanister,
}

fn row(
    id: u128,
    rank: i64,
    flag: bool,
) -> (crate::types::Id<ProjectionEvalEntity>, ProjectionEvalEntity) {
    let entity = ProjectionEvalEntity {
        id: Ulid::from_u128(id),
        rank,
        flag,
        label: format!("label-{id}"),
        profile: ProjectionEvalProfile {
            name: format!("profile-{id}"),
            rank,
            score: u64::try_from(rank).expect("test rank should be non-negative"),
            details_flag: flag,
        },
    };

    (entity.id(), entity)
}

#[cfg(feature = "sql")]
pub(in crate::db) fn projection_eval_row_layout_for_materialize_tests() -> RowLayout {
    RowLayout::from_model(ProjectionEvalEntity::MODEL)
}

#[cfg(feature = "sql")]
pub(in crate::db) fn projection_eval_data_row_for_materialize_tests(
    id: u128,
    rank: i64,
    flag: bool,
) -> DataRow {
    let (entity_id, entity) = row(id, rank, flag);
    let data_key = DataKey::try_new::<ProjectionEvalEntity>(entity_id.key())
        .expect("projection eval test key should encode");
    let raw_row = CanonicalRow::from_entity(&entity)
        .expect("projection eval test row should encode")
        .into_raw_row();

    (data_key, raw_row)
}

/// Evaluate one projection expression against one grouped output row view.
fn eval_expr_grouped(
    expr: &Expr,
    grouped_row: &GroupedRowView<'_>,
) -> Result<Value, ProjectionEvalError> {
    let compiled = compile_grouped_projection_expr(
        expr,
        grouped_row.group_fields(),
        grouped_row.aggregate_execution_specs,
    )?;

    compiled.evaluate(grouped_row).map(Cow::into_owned)
}

fn projection_test_reader_error(err: InternalError) -> ProjectionEvalError {
    ProjectionEvalError::ReaderFailed {
        class: err.class(),
        origin: err.origin(),
        message: err.into_message(),
    }
}

fn eval_scalar_projection_expr(
    expr: &ScalarProjectionExpr,
    slots: &mut dyn SlotReader,
) -> Result<Value, InternalError> {
    let compiled = CompiledExpr::compile(expr);
    let mut read_slot = |slot| slots.get_value(slot).ok().flatten();

    eval_compiled_expr_with_value_reader(&compiled, &mut read_slot)
        .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)
}

fn eval_canonical_scalar_projection_expr(
    expr: &ScalarProjectionExpr,
    slots: &dyn CanonicalSlotReader,
) -> Result<Value, InternalError> {
    let compiled = CompiledExpr::compile(expr);
    let mut noop = |_| {};

    eval_compiled_expr_with_required_slot_reader_cow(&compiled, slots, &mut noop)
        .map(Cow::into_owned)
}

fn eval_canonical_scalar_projection_expr_with_required_slot_reader_cow<'a>(
    expr: &'a ScalarProjectionExpr,
    slots: &'a dyn CanonicalSlotReader,
    record_slot: &'a mut dyn FnMut(usize),
) -> Result<Cow<'a, Value>, InternalError> {
    let compiled = CompiledExpr::compile(expr);
    let value = eval_compiled_expr_with_required_slot_reader_cow(&compiled, slots, record_slot)?
        .into_owned();

    Ok(Cow::Owned(value))
}

fn eval_canonical_scalar_projection_expr_with_required_value_reader_cow<'a>(
    expr: &'a ScalarProjectionExpr,
    read_slot: &mut dyn FnMut(usize) -> Result<Cow<'a, Value>, InternalError>,
) -> Result<Cow<'a, Value>, InternalError> {
    struct RequiredCowReader<'reader, 'value> {
        read_slot:
            RefCell<&'reader mut dyn FnMut(usize) -> Result<Cow<'value, Value>, InternalError>>,
    }

    impl CompiledExprValueReader for RequiredCowReader<'_, '_> {
        fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>> {
            (self.read_slot.borrow_mut())(slot)
                .ok()
                .map(|value| match value {
                    Cow::Borrowed(value) => Cow::Borrowed(value),
                    Cow::Owned(value) => Cow::Owned(value),
                })
        }

        fn read_slot_checked(
            &self,
            slot: usize,
        ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
            (self.read_slot.borrow_mut())(slot)
                .map(|value| {
                    Some(match value {
                        Cow::Borrowed(value) => Cow::Borrowed(value),
                        Cow::Owned(value) => Cow::Owned(value),
                    })
                })
                .map_err(projection_test_reader_error)
        }

        fn read_group_key(&self, _offset: usize) -> Option<Cow<'_, Value>> {
            None
        }

        fn read_aggregate(&self, _index: usize) -> Option<Cow<'_, Value>> {
            None
        }
    }

    let compiled = CompiledExpr::compile(expr);
    let reader = RequiredCowReader {
        read_slot: RefCell::new(read_slot),
    };

    compiled
        .evaluate(&reader)
        .map(Cow::into_owned)
        .map(Cow::Owned)
        .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)
}

fn eval_scalar_expr_for_row(
    expr: &Expr,
    row: &ProjectionEvalEntity,
) -> Result<Value, InternalError> {
    let compiled = compile_scalar_projection_expr(ProjectionEvalEntity::MODEL, expr)
        .expect("expression should compile onto scalar projection seam");
    let raw_row = CanonicalRow::from_entity(row)
        .expect("persisted row should encode")
        .into_raw_row();
    let mut row_fields = StructuralSlotReader::from_raw_row(&raw_row, ProjectionEvalEntity::MODEL)
        .expect("persisted row should decode structurally");

    eval_scalar_projection_expr(&compiled, &mut row_fields)
}

fn eval_canonical_scalar_expr_for_row(
    expr: &Expr,
    row: &ProjectionEvalEntity,
) -> Result<Value, InternalError> {
    let compiled = compile_scalar_projection_expr(ProjectionEvalEntity::MODEL, expr)
        .expect("expression should compile onto scalar projection seam");
    let raw_row = CanonicalRow::from_entity(row)
        .expect("persisted row should encode")
        .into_raw_row();
    let row_fields = StructuralSlotReader::from_raw_row(&raw_row, ProjectionEvalEntity::MODEL)
        .expect("persisted row should decode structurally");
    let mut record_slot = |_| {};
    let value = eval_canonical_scalar_projection_expr_with_required_slot_reader_cow(
        &compiled,
        &row_fields,
        &mut record_slot,
    )?;

    Ok(value.into_owned())
}

fn eval_scalar_filter_expr_for_row(
    expr: &Expr,
    row: &ProjectionEvalEntity,
) -> Result<bool, InternalError> {
    let compiled = compile_scalar_projection_expr(ProjectionEvalEntity::MODEL, expr)
        .expect("filter expression should compile onto scalar projection seam");
    let raw_row = CanonicalRow::from_entity(row)
        .expect("persisted row should encode")
        .into_raw_row();
    let row_fields = StructuralSlotReader::from_raw_row(&raw_row, ProjectionEvalEntity::MODEL)
        .expect("persisted row should decode structurally");

    eval_effective_runtime_filter_program_with_slot_reader(
        &EffectiveRuntimeFilterProgram::expression(CompiledExpr::compile(&compiled)),
        &row_fields,
    )
}

fn eval_canonical_scalar_expr_with_required_reader(
    expr: &Expr,
    read_slot: &mut dyn FnMut(usize) -> Result<Value, InternalError>,
) -> Result<Value, InternalError> {
    let compiled = compile_scalar_projection_expr(ProjectionEvalEntity::MODEL, expr)
        .expect("expression should compile onto scalar projection seam");
    let value = eval_canonical_scalar_projection_expr_with_required_value_reader_cow(
        &compiled,
        &mut |slot| read_slot(slot).map(std::borrow::Cow::Owned),
    )?;

    Ok(value.into_owned())
}

fn grouped_execution_specs<const N: usize>(
    aggregate_exprs: [crate::db::query::builder::aggregate::AggregateExpr; N],
) -> [GroupedAggregateExecutionSpec; N] {
    aggregate_exprs.map(|aggregate_expr| {
        GroupedAggregateExecutionSpec::from_aggregate_expr(&aggregate_expr)
            .resolve_for_model(
                ProjectionEvalEntity::MODEL,
                SchemaInfo::cached_for_entity_model(ProjectionEvalEntity::MODEL),
            )
            .expect("grouped execution spec should lower from aggregate expression")
    })
}

///
/// ProjectionMissingDeclaredSlotReader
///
///
/// ProjectionMissingDeclaredSlotReader simulates one canonical structural row
/// whose declared slots are absent so projection evaluators can prove they
/// preserve corruption diagnostics instead of flattening them into
/// invalid-logical-plan failures.
///

struct ProjectionMissingDeclaredSlotReader;

impl SlotReader for ProjectionMissingDeclaredSlotReader {
    fn model(&self) -> &'static crate::model::entity::EntityModel {
        ProjectionEvalEntity::MODEL
    }

    fn has(&self, _slot: usize) -> bool {
        false
    }

    fn get_bytes(&self, _slot: usize) -> Option<&[u8]> {
        None
    }

    fn get_scalar(
        &self,
        _slot: usize,
    ) -> Result<Option<crate::db::data::ScalarSlotValueRef<'_>>, InternalError> {
        Ok(None)
    }

    fn get_value(&mut self, _slot: usize) -> Result<Option<Value>, InternalError> {
        panic!("projection missing-slot test reader should not route through get_value")
    }
}

impl CanonicalSlotReader for ProjectionMissingDeclaredSlotReader {}
