//! Module: db::scalar_expr
//! Responsibility: shared scalar-only expression compilation and evaluation.
//! Does not own: predicate boolean trees or index key framing.
//! Boundary: predicate and index runtimes use this to avoid `Value` fallback for scalar work.

#[cfg(test)]
use crate::{
    db::data::{CanonicalSlotReader, ScalarSlotValueRef, ScalarValueRef, SlotReader},
    error::InternalError,
    model::{entity::EntityModel, field::LeafCodec, index::IndexKeyItem},
};
use crate::{model::index::IndexExpression, types::Date, value::Value};
use std::borrow::Cow;

const MILLIS_PER_DAY: i64 = 86_400_000;
const EXPECTED_TEXT: &str = "Text";
const EXPECTED_DATE_OR_TIMESTAMP: &str = "Date/Timestamp";

///
/// ScalarValueProgram
///
/// ScalarValueProgram is one compiled scalar-only value expression.
/// It intentionally excludes boolean composition so predicate and index layers
/// can share scalar computation without coupling their higher-level control flow.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(in crate::db) enum ScalarValueProgram {
    Field { slot: usize },
    Lower { slot: usize },
    Upper { slot: usize },
    Trim { slot: usize },
    LowerTrim { slot: usize },
    Date { slot: usize },
    Year { slot: usize },
    Month { slot: usize },
    Day { slot: usize },
}

///
/// ScalarIndexExpressionOp
///
/// ScalarIndexExpressionOp is the shared transform opcode for scalar index
/// expressions.
/// Runtime slot evaluation and value-based planner lowering both route through
/// this operator so expression semantics stay aligned.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ScalarIndexExpressionOp {
    Lower,
    Upper,
    Trim,
    LowerTrim,
    Date,
    Year,
    Month,
    Day,
}

impl ScalarIndexExpressionOp {
    // Return the stable expression label used in scalar mismatch diagnostics.
    #[cfg(test)]
    const fn label(self) -> &'static str {
        match self {
            Self::Lower => "LOWER",
            Self::Upper => "UPPER",
            Self::Trim => "TRIM",
            Self::LowerTrim => "LOWER(TRIM)",
            Self::Date => "DATE",
            Self::Year => "YEAR",
            Self::Month => "MONTH",
            Self::Day => "DAY",
        }
    }

    // Build the canonical scalar-expression input mismatch error.
    #[cfg(test)]
    fn input_type_mismatch(self, expected: &'static str) -> InternalError {
        let label = self.label();

        match expected {
            EXPECTED_TEXT => InternalError::query_executor_invariant(format!(
                "scalar expression {label} expected text input",
            )),
            EXPECTED_DATE_OR_TIMESTAMP => InternalError::executor_internal(format!(
                "scalar expression {label} expected date/timestamp input",
            )),
            _ => InternalError::executor_internal(format!(
                "scalar expression {label} expected {expected} input",
            )),
        }
    }
}

///
/// ScalarExprValue
///
/// ScalarExprValue is the shared scalar result container for compiled scalar
/// expressions.
/// It preserves borrowed field payloads where possible and only allocates for
/// derived text transforms.
///

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "scalar expression results intentionally cover every scalar value kind even when a build uses only a subset"
)]
pub(in crate::db) enum ScalarExprValue<'a> {
    Null,
    Blob(Cow<'a, [u8]>),
    Bool(bool),
    Date(crate::types::Date),
    Duration(crate::types::Duration),
    Float32(crate::types::Float32),
    Float64(crate::types::Float64),
    Int(i64),
    Principal(crate::types::Principal),
    Subaccount(crate::types::Subaccount),
    Text(Cow<'a, str>),
    Timestamp(crate::types::Timestamp),
    Nat(u64),
    Ulid(crate::types::Ulid),
    Unit,
}

/// Convert one shared scalar expression value into the runtime `Value` enum.
#[must_use]
pub(in crate::db) fn scalar_expr_value_into_value(value: ScalarExprValue<'_>) -> Value {
    match value {
        ScalarExprValue::Null => Value::Null,
        ScalarExprValue::Blob(value) => Value::Blob(value.into_owned()),
        ScalarExprValue::Bool(value) => Value::Bool(value),
        ScalarExprValue::Date(value) => Value::Date(value),
        ScalarExprValue::Duration(value) => Value::Duration(value),
        ScalarExprValue::Float32(value) => Value::Float32(value),
        ScalarExprValue::Float64(value) => Value::Float64(value),
        ScalarExprValue::Int(value) => Value::Int(value),
        ScalarExprValue::Principal(value) => Value::Principal(value),
        ScalarExprValue::Subaccount(value) => Value::Subaccount(value),
        ScalarExprValue::Text(value) => Value::Text(value.into_owned()),
        ScalarExprValue::Timestamp(value) => Value::Timestamp(value),
        ScalarExprValue::Nat(value) => Value::Nat(value),
        ScalarExprValue::Ulid(value) => Value::Ulid(value),
        ScalarExprValue::Unit => Value::Unit,
    }
}

/// Compile one runtime literal into the shared scalar expression value
/// container when it remains entirely on the scalar seam.
#[must_use]
#[cfg(test)]
pub(in crate::db) fn compile_scalar_literal_expr_value(
    value: &Value,
) -> Option<ScalarExprValue<'static>> {
    match value {
        Value::Null => Some(ScalarExprValue::Null),
        Value::Blob(value) => Some(ScalarExprValue::Blob(Cow::Owned(value.clone()))),
        Value::Bool(value) => Some(ScalarExprValue::Bool(*value)),
        Value::Date(value) => Some(ScalarExprValue::Date(*value)),
        Value::Duration(value) => Some(ScalarExprValue::Duration(*value)),
        Value::Float32(value) => Some(ScalarExprValue::Float32(*value)),
        Value::Float64(value) => Some(ScalarExprValue::Float64(*value)),
        Value::Int(value) => Some(ScalarExprValue::Int(*value)),
        Value::Principal(value) => Some(ScalarExprValue::Principal(*value)),
        Value::Subaccount(value) => Some(ScalarExprValue::Subaccount(*value)),
        Value::Text(value) => Some(ScalarExprValue::Text(Cow::Owned(value.clone()))),
        Value::Timestamp(value) => Some(ScalarExprValue::Timestamp(*value)),
        Value::Nat(value) => Some(ScalarExprValue::Nat(*value)),
        Value::Ulid(value) => Some(ScalarExprValue::Ulid(*value)),
        Value::Unit => Some(ScalarExprValue::Unit),
        Value::Account(_)
        | Value::Decimal(_)
        | Value::Enum(_)
        | Value::Int128(_)
        | Value::IntBig(_)
        | Value::List(_)
        | Value::Map(_)
        | Value::Nat128(_)
        | Value::NatBig(_) => None,
    }
}

/// Compile one scalar field access into the shared scalar-expression program.
#[must_use]
#[cfg(test)]
pub(in crate::db) fn compile_scalar_field_program(
    model: &EntityModel,
    field_name: &str,
) -> Option<ScalarValueProgram> {
    let slot = model.resolve_field_slot(field_name)?;
    let field = model.fields().get(slot)?;
    if !matches!(field.leaf_codec(), LeafCodec::Scalar(_)) {
        return None;
    }

    Some(ScalarValueProgram::Field { slot })
}

/// Compile one index expression into the shared scalar-expression program when
/// the source field remains on the scalar slot seam.
#[must_use]
#[cfg(test)]
pub(in crate::db) fn compile_scalar_index_expression_program(
    model: &'static EntityModel,
    expression: IndexExpression,
) -> Option<ScalarValueProgram> {
    let slot = model.resolve_field_slot(expression.field())?;
    let field = model.fields().get(slot)?;
    if !matches!(field.leaf_codec(), LeafCodec::Scalar(_)) {
        return None;
    }

    Some(match expression {
        IndexExpression::Lower(_) => ScalarValueProgram::Lower { slot },
        IndexExpression::Upper(_) => ScalarValueProgram::Upper { slot },
        IndexExpression::Trim(_) => ScalarValueProgram::Trim { slot },
        IndexExpression::LowerTrim(_) => ScalarValueProgram::LowerTrim { slot },
        IndexExpression::Date(_) => ScalarValueProgram::Date { slot },
        IndexExpression::Year(_) => ScalarValueProgram::Year { slot },
        IndexExpression::Month(_) => ScalarValueProgram::Month { slot },
        IndexExpression::Day(_) => ScalarValueProgram::Day { slot },
    })
}

/// Map one index expression shape to its shared scalar transform opcode.
#[must_use]
pub(in crate::db) const fn scalar_index_expression_op(
    expression: IndexExpression,
) -> ScalarIndexExpressionOp {
    match expression {
        IndexExpression::Lower(_) => ScalarIndexExpressionOp::Lower,
        IndexExpression::Upper(_) => ScalarIndexExpressionOp::Upper,
        IndexExpression::Trim(_) => ScalarIndexExpressionOp::Trim,
        IndexExpression::LowerTrim(_) => ScalarIndexExpressionOp::LowerTrim,
        IndexExpression::Date(_) => ScalarIndexExpressionOp::Date,
        IndexExpression::Year(_) => ScalarIndexExpressionOp::Year,
        IndexExpression::Month(_) => ScalarIndexExpressionOp::Month,
        IndexExpression::Day(_) => ScalarIndexExpressionOp::Day,
    }
}

/// Compile one index key item into the shared scalar-expression program when
/// the item stays entirely on the scalar slot seam.
#[must_use]
#[cfg(test)]
pub(in crate::db) fn compile_scalar_index_key_item_program(
    model: &'static EntityModel,
    key_item: IndexKeyItem,
) -> Option<ScalarValueProgram> {
    match key_item {
        IndexKeyItem::Field(field) => compile_scalar_field_program(model, field),
        IndexKeyItem::Expression(expression) => {
            compile_scalar_index_expression_program(model, expression)
        }
    }
}

/// Evaluate one compiled scalar expression directly from one slot reader.
#[cfg(test)]
pub(in crate::db) fn eval_scalar_value_program<'a>(
    program: &ScalarValueProgram,
    slots: &'a dyn SlotReader,
) -> Result<Option<ScalarExprValue<'a>>, InternalError> {
    // Keep the nullable slot-reader transform logic local to the shared test
    // evaluator so the runtime module does not carry an extra one-caller helper.
    let eval_scalar_expression = |slot: usize, op: ScalarIndexExpressionOp| {
        let Some(value) = slots.get_scalar(slot)? else {
            return Ok(None);
        };
        let value = match value {
            ScalarSlotValueRef::Null => ScalarExprValue::Null,
            ScalarSlotValueRef::Value(value) => scalar_expr_value_from_slot_value(value),
        };

        match value {
            ScalarExprValue::Null => Ok(Some(ScalarExprValue::Null)),
            value => derive_non_null_scalar_expression_value(op, value)
                .map(Some)
                .map_err(|expected| op.input_type_mismatch(expected)),
        }
    };

    match program {
        ScalarValueProgram::Field { slot } => {
            let Some(value) = slots.get_scalar(*slot)? else {
                return Ok(None);
            };

            Ok(Some(match value {
                ScalarSlotValueRef::Null => ScalarExprValue::Null,
                ScalarSlotValueRef::Value(value) => scalar_expr_value_from_slot_value(value),
            }))
        }
        ScalarValueProgram::Lower { slot } => {
            eval_scalar_expression(*slot, ScalarIndexExpressionOp::Lower)
        }
        ScalarValueProgram::Upper { slot } => {
            eval_scalar_expression(*slot, ScalarIndexExpressionOp::Upper)
        }
        ScalarValueProgram::Trim { slot } => {
            eval_scalar_expression(*slot, ScalarIndexExpressionOp::Trim)
        }
        ScalarValueProgram::LowerTrim { slot } => {
            eval_scalar_expression(*slot, ScalarIndexExpressionOp::LowerTrim)
        }
        ScalarValueProgram::Date { slot } => {
            eval_scalar_expression(*slot, ScalarIndexExpressionOp::Date)
        }
        ScalarValueProgram::Year { slot } => {
            eval_scalar_expression(*slot, ScalarIndexExpressionOp::Year)
        }
        ScalarValueProgram::Month { slot } => {
            eval_scalar_expression(*slot, ScalarIndexExpressionOp::Month)
        }
        ScalarValueProgram::Day { slot } => {
            eval_scalar_expression(*slot, ScalarIndexExpressionOp::Day)
        }
    }
}

/// Evaluate one compiled scalar expression through the canonical structural
/// slot seam where declared slots must already exist.
#[cfg(test)]
pub(in crate::db) fn eval_canonical_scalar_value_program<'a>(
    program: &ScalarValueProgram,
    slots: &'a dyn CanonicalSlotReader,
) -> Result<ScalarExprValue<'a>, InternalError> {
    match program {
        ScalarValueProgram::Field { slot } => eval_canonical_scalar_field(*slot, slots),
        ScalarValueProgram::Lower { slot } => {
            eval_canonical_scalar_expression_op(*slot, slots, ScalarIndexExpressionOp::Lower)
        }
        ScalarValueProgram::Upper { slot } => {
            eval_canonical_scalar_expression_op(*slot, slots, ScalarIndexExpressionOp::Upper)
        }
        ScalarValueProgram::Trim { slot } => {
            eval_canonical_scalar_expression_op(*slot, slots, ScalarIndexExpressionOp::Trim)
        }
        ScalarValueProgram::LowerTrim { slot } => {
            eval_canonical_scalar_expression_op(*slot, slots, ScalarIndexExpressionOp::LowerTrim)
        }
        ScalarValueProgram::Date { slot } => {
            eval_canonical_scalar_expression_op(*slot, slots, ScalarIndexExpressionOp::Date)
        }
        ScalarValueProgram::Year { slot } => {
            eval_canonical_scalar_expression_op(*slot, slots, ScalarIndexExpressionOp::Year)
        }
        ScalarValueProgram::Month { slot } => {
            eval_canonical_scalar_expression_op(*slot, slots, ScalarIndexExpressionOp::Month)
        }
        ScalarValueProgram::Day { slot } => {
            eval_canonical_scalar_expression_op(*slot, slots, ScalarIndexExpressionOp::Day)
        }
    }
}

// Evaluate one scalar field access through the canonical slot-reader fast path.
#[cfg(test)]
fn eval_canonical_scalar_field(
    slot: usize,
    slots: &dyn CanonicalSlotReader,
) -> Result<ScalarExprValue<'_>, InternalError> {
    Ok(match slots.required_scalar(slot)? {
        ScalarSlotValueRef::Null => ScalarExprValue::Null,
        ScalarSlotValueRef::Value(value) => scalar_expr_value_from_slot_value(value),
    })
}

// Evaluate one scalar expression operator against the canonical slot seam.
#[cfg(test)]
fn eval_canonical_scalar_expression_op(
    slot: usize,
    slots: &dyn CanonicalSlotReader,
    op: ScalarIndexExpressionOp,
) -> Result<ScalarExprValue<'_>, InternalError> {
    match eval_canonical_scalar_field(slot, slots)? {
        ScalarExprValue::Null => Ok(ScalarExprValue::Null),
        value => derive_non_null_scalar_expression_value(op, value)
            .map_err(|expected| op.input_type_mismatch(expected)),
    }
}

/// Apply one shared scalar expression opcode to one non-null scalar input.
pub(in crate::db) fn derive_non_null_scalar_expression_value(
    op: ScalarIndexExpressionOp,
    source: ScalarExprValue<'_>,
) -> Result<ScalarExprValue<'_>, &'static str> {
    match op {
        ScalarIndexExpressionOp::Lower => match source {
            ScalarExprValue::Text(text) => Ok(ScalarExprValue::Text(Cow::Owned(
                normalize_text_casefold(text.as_ref()),
            ))),
            _ => Err(EXPECTED_TEXT),
        },
        ScalarIndexExpressionOp::Upper => match source {
            ScalarExprValue::Text(text) => Ok(ScalarExprValue::Text(Cow::Owned(
                normalize_text_upper(text.as_ref()),
            ))),
            _ => Err(EXPECTED_TEXT),
        },
        ScalarIndexExpressionOp::Trim => match source {
            ScalarExprValue::Text(text) => {
                Ok(ScalarExprValue::Text(Cow::Owned(text.trim().to_string())))
            }
            _ => Err(EXPECTED_TEXT),
        },
        ScalarIndexExpressionOp::LowerTrim => match source {
            ScalarExprValue::Text(text) => Ok(ScalarExprValue::Text(Cow::Owned(
                normalize_text_casefold(text.trim()),
            ))),
            _ => Err(EXPECTED_TEXT),
        },
        ScalarIndexExpressionOp::Date => match source {
            ScalarExprValue::Date(value) => Ok(ScalarExprValue::Date(value)),
            ScalarExprValue::Timestamp(value) => Ok(ScalarExprValue::Date(
                timestamp_to_bucket_date(value.as_millis()),
            )),
            _ => Err(EXPECTED_DATE_OR_TIMESTAMP),
        },
        ScalarIndexExpressionOp::Year => match source {
            ScalarExprValue::Date(value) => Ok(ScalarExprValue::Int(i64::from(value.year()))),
            ScalarExprValue::Timestamp(value) => {
                let bucket = timestamp_to_bucket_date(value.as_millis());
                Ok(ScalarExprValue::Int(i64::from(bucket.year())))
            }
            _ => Err(EXPECTED_DATE_OR_TIMESTAMP),
        },
        ScalarIndexExpressionOp::Month => match source {
            ScalarExprValue::Date(value) => Ok(ScalarExprValue::Int(i64::from(value.month()))),
            ScalarExprValue::Timestamp(value) => {
                let bucket = timestamp_to_bucket_date(value.as_millis());
                Ok(ScalarExprValue::Int(i64::from(bucket.month())))
            }
            _ => Err(EXPECTED_DATE_OR_TIMESTAMP),
        },
        ScalarIndexExpressionOp::Day => match source {
            ScalarExprValue::Date(value) => Ok(ScalarExprValue::Int(i64::from(value.day()))),
            ScalarExprValue::Timestamp(value) => {
                let bucket = timestamp_to_bucket_date(value.as_millis());
                Ok(ScalarExprValue::Int(i64::from(bucket.day())))
            }
            _ => Err(EXPECTED_DATE_OR_TIMESTAMP),
        },
    }
}

#[cfg(test)]
const fn scalar_expr_value_from_slot_value(value: ScalarValueRef<'_>) -> ScalarExprValue<'_> {
    match value {
        ScalarValueRef::Blob(value) => ScalarExprValue::Blob(Cow::Borrowed(value)),
        ScalarValueRef::Bool(value) => ScalarExprValue::Bool(value),
        ScalarValueRef::Date(value) => ScalarExprValue::Date(value),
        ScalarValueRef::Duration(value) => ScalarExprValue::Duration(value),
        ScalarValueRef::Float32(value) => ScalarExprValue::Float32(value),
        ScalarValueRef::Float64(value) => ScalarExprValue::Float64(value),
        ScalarValueRef::Int(value) => ScalarExprValue::Int(value),
        ScalarValueRef::Principal(value) => ScalarExprValue::Principal(value),
        ScalarValueRef::Subaccount(value) => ScalarExprValue::Subaccount(value),
        ScalarValueRef::Text(value) => ScalarExprValue::Text(Cow::Borrowed(value)),
        ScalarValueRef::Timestamp(value) => ScalarExprValue::Timestamp(value),
        ScalarValueRef::Nat(value) => ScalarExprValue::Nat(value),
        ScalarValueRef::Ulid(value) => ScalarExprValue::Ulid(value),
        ScalarValueRef::Unit => ScalarExprValue::Unit,
    }
}

fn normalize_text_casefold(input: &str) -> String {
    if input.is_ascii() {
        input.to_ascii_lowercase()
    } else {
        input.to_lowercase()
    }
}

fn normalize_text_upper(input: &str) -> String {
    if input.is_ascii() {
        input.to_ascii_uppercase()
    } else {
        input.to_uppercase()
    }
}

fn timestamp_to_bucket_date(timestamp_millis: i64) -> Date {
    let days = timestamp_millis.div_euclid(MILLIS_PER_DAY);
    let days = if let Ok(days) = i32::try_from(days) {
        days
    } else if days < 0 {
        i32::MIN
    } else {
        i32::MAX
    };

    Date::from_days_since_epoch(days)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        ScalarExprValue, compile_scalar_field_program, compile_scalar_index_expression_program,
        compile_scalar_index_key_item_program, eval_canonical_scalar_value_program,
        eval_scalar_value_program,
    };
    use crate::{
        db::{
            data::{CanonicalSlotReader, ScalarSlotValueRef, SlotReader},
            index::derive_index_expression_value,
            scalar_expr::ScalarValueProgram::{Date, Field, Lower},
        },
        error::InternalError,
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel},
            index::{IndexExpression, IndexKeyItem},
        },
        types::Timestamp,
        value::Value,
    };

    static SCALAR_EXPR_FIELDS: [FieldModel; 4] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("name", FieldKind::Text { max_len: None }),
        FieldModel::generated("created_at", FieldKind::Timestamp),
        FieldModel::generated("tags", FieldKind::List(&FieldKind::Text { max_len: None })),
    ];
    static SCALAR_EXPR_MODEL: EntityModel = EntityModel::generated(
        "ScalarExprTestEntity",
        "ScalarExprTestEntity",
        &SCALAR_EXPR_FIELDS[0],
        0,
        &SCALAR_EXPR_FIELDS,
        &[],
    );

    struct TestSlotReader {
        name: Option<ScalarSlotValueRef<'static>>,
        created_at: Option<ScalarSlotValueRef<'static>>,
    }

    impl SlotReader for TestSlotReader {
        fn generated_compatible_field_model(
            &self,
            slot: usize,
        ) -> Result<&FieldModel, InternalError> {
            SCALAR_EXPR_MODEL.fields().get(slot).ok_or_else(|| {
                InternalError::persisted_row_slot_lookup_out_of_bounds(
                    SCALAR_EXPR_MODEL.path(),
                    slot,
                )
            })
        }

        fn has(&self, slot: usize) -> bool {
            match slot {
                1 => self.name.is_some(),
                2 => self.created_at.is_some(),
                _ => false,
            }
        }

        fn get_bytes(&self, _slot: usize) -> Option<&[u8]> {
            None
        }

        fn get_scalar(&self, slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
            Ok(match slot {
                1 => self.name,
                2 => self.created_at,
                _ => None,
            })
        }

        fn get_value(&mut self, _slot: usize) -> Result<Option<Value>, InternalError> {
            panic!("test scalar expr reader should not route through get_value")
        }
    }

    impl CanonicalSlotReader for TestSlotReader {}

    #[test]
    fn scalar_expr_compiles_field_and_index_programs_on_scalar_slots_only() {
        assert_eq!(
            compile_scalar_field_program(&SCALAR_EXPR_MODEL, "name"),
            Some(Field { slot: 1 }),
        );
        assert_eq!(
            compile_scalar_index_key_item_program(&SCALAR_EXPR_MODEL, IndexKeyItem::Field("name")),
            Some(Field { slot: 1 }),
        );
        assert_eq!(
            compile_scalar_index_expression_program(
                &SCALAR_EXPR_MODEL,
                IndexExpression::Lower("name")
            ),
            Some(Lower { slot: 1 }),
        );
        assert_eq!(
            compile_scalar_index_expression_program(
                &SCALAR_EXPR_MODEL,
                IndexExpression::Date("created_at")
            ),
            Some(Date { slot: 2 }),
        );
        assert_eq!(
            compile_scalar_index_key_item_program(
                &SCALAR_EXPR_MODEL,
                IndexKeyItem::Expression(IndexExpression::Date("created_at")),
            ),
            Some(Date { slot: 2 }),
        );
        assert_eq!(
            compile_scalar_field_program(&SCALAR_EXPR_MODEL, "tags"),
            None
        );
        assert_eq!(
            compile_scalar_index_key_item_program(&SCALAR_EXPR_MODEL, IndexKeyItem::Field("tags")),
            None
        );
    }

    #[test]
    fn scalar_expr_evaluates_shared_text_and_temporal_programs() {
        let slots = TestSlotReader {
            name: Some(ScalarSlotValueRef::Value(crate::db::ScalarValueRef::Text(
                "ALIce ",
            ))),
            created_at: Some(ScalarSlotValueRef::Value(
                crate::db::ScalarValueRef::Timestamp(Timestamp::from_millis(86_400_000 * 3 + 123)),
            )),
        };
        let lower = compile_scalar_index_expression_program(
            &SCALAR_EXPR_MODEL,
            IndexExpression::LowerTrim("name"),
        )
        .expect("lower-trim should compile");
        let day = compile_scalar_index_expression_program(
            &SCALAR_EXPR_MODEL,
            IndexExpression::Day("created_at"),
        )
        .expect("day should compile");

        let lower_value = eval_scalar_value_program(&lower, &slots).expect("lower should evaluate");
        let day_value = eval_scalar_value_program(&day, &slots).expect("day should evaluate");

        assert_eq!(
            lower_value,
            Some(ScalarExprValue::Text(std::borrow::Cow::Owned(
                "alice".to_string()
            ))),
        );
        assert_eq!(day_value, Some(ScalarExprValue::Int(4)));
    }

    #[test]
    fn scalar_expr_preserves_null_and_missing_slots() {
        let slots = TestSlotReader {
            name: Some(ScalarSlotValueRef::Null),
            created_at: None,
        };
        let field =
            compile_scalar_field_program(&SCALAR_EXPR_MODEL, "name").expect("field should compile");
        let date = compile_scalar_index_expression_program(
            &SCALAR_EXPR_MODEL,
            IndexExpression::Date("created_at"),
        )
        .expect("date should compile");

        let field_value = eval_scalar_value_program(&field, &slots).expect("field should evaluate");
        let date_value = eval_scalar_value_program(&date, &slots).expect("date should evaluate");

        assert_eq!(field_value, Some(ScalarExprValue::Null));
        assert_eq!(date_value, None);
    }

    #[test]
    fn canonical_scalar_expr_rejects_missing_declared_slots() {
        let slots = TestSlotReader {
            name: Some(ScalarSlotValueRef::Null),
            created_at: None,
        };
        let date = compile_scalar_index_expression_program(
            &SCALAR_EXPR_MODEL,
            IndexExpression::Date("created_at"),
        )
        .expect("date should compile");

        let err = eval_canonical_scalar_value_program(&date, &slots)
            .expect_err("canonical scalar lane must fail closed on missing slots");

        assert!(
            err.message.contains("missing declared field `created_at`"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn scalar_expr_matches_value_lowering_for_shared_index_expression_semantics() {
        let slots = TestSlotReader {
            name: Some(ScalarSlotValueRef::Value(crate::db::ScalarValueRef::Text(
                "ALIce ",
            ))),
            created_at: Some(ScalarSlotValueRef::Value(
                crate::db::ScalarValueRef::Timestamp(Timestamp::from_millis(86_400_000 * 3 + 123)),
            )),
        };
        let lower = compile_scalar_index_expression_program(
            &SCALAR_EXPR_MODEL,
            IndexExpression::LowerTrim("name"),
        )
        .expect("lower-trim should compile");
        let date = compile_scalar_index_expression_program(
            &SCALAR_EXPR_MODEL,
            IndexExpression::Date("created_at"),
        )
        .expect("date should compile");

        let lower_scalar = eval_scalar_value_program(&lower, &slots)
            .expect("lower should evaluate")
            .map(scalar_expr_value_to_value);
        let date_scalar = eval_scalar_value_program(&date, &slots)
            .expect("date should evaluate")
            .map(scalar_expr_value_to_value);

        let lower_value = derive_index_expression_value(
            IndexExpression::LowerTrim("name"),
            Value::Text("ALIce ".to_string()),
        )
        .expect("lower-trim value lowering should succeed");
        let date_value = derive_index_expression_value(
            IndexExpression::Date("created_at"),
            Value::Timestamp(Timestamp::from_millis(86_400_000 * 3 + 123)),
        )
        .expect("date value lowering should succeed");

        assert_eq!(lower_scalar, lower_value);
        assert_eq!(date_scalar, date_value);
    }

    fn scalar_expr_value_to_value(value: ScalarExprValue<'_>) -> Value {
        super::scalar_expr_value_into_value(value)
    }
}
