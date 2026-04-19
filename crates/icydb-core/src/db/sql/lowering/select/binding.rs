use crate::{
    db::predicate::{CoercionId, CoercionSpec, CompareOp, Predicate},
    model::{entity::EntityModel, field::FieldKind},
    types::Ulid,
    value::Value,
};

// Canonicalize strict numeric SQL predicate literals onto the resolved model
// field kind so unsigned-width fields keep strict/indexable semantics even
// though reduced SQL integer tokens parse through one generic numeric value
// variant first.
pub(in crate::db) fn canonicalize_sql_predicate_for_model(
    model: &'static EntityModel,
    predicate: Predicate,
) -> Predicate {
    match predicate {
        Predicate::And(children) => Predicate::And(
            children
                .into_iter()
                .map(|child| canonicalize_sql_predicate_for_model(model, child))
                .collect(),
        ),
        Predicate::Or(children) => Predicate::Or(
            children
                .into_iter()
                .map(|child| canonicalize_sql_predicate_for_model(model, child))
                .collect(),
        ),
        Predicate::Not(inner) => Predicate::Not(Box::new(canonicalize_sql_predicate_for_model(
            model, *inner,
        ))),
        Predicate::Compare(mut cmp) => {
            canonicalize_sql_compare_for_model(model, &mut cmp);
            Predicate::Compare(cmp)
        }
        Predicate::CompareFields(cmp) => Predicate::CompareFields(cmp),
        Predicate::True
        | Predicate::False
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => predicate,
    }
}

// Resolve one lowered predicate field onto the runtime model kind that owns
// its strict literal compatibility rules.
pub(super) fn model_field_kind(model: &'static EntityModel, field: &str) -> Option<FieldKind> {
    model
        .fields()
        .iter()
        .find(|candidate| candidate.name() == field)
        .map(crate::model::field::FieldModel::kind)
}

// Keep SQL-only strict literal canonicalization narrow:
// - only direct field predicates are eligible
// - text operators stay on raw text literals
// - field-kind-owned rewrites stay local to SQL lowering
fn canonicalize_sql_compare_for_model(
    model: &'static EntityModel,
    cmp: &mut crate::db::predicate::ComparePredicate,
) {
    let Some(field_kind) = model_field_kind(model, &cmp.field) else {
        return;
    };

    match cmp.op {
        CompareOp::Eq
        | CompareOp::Ne
        | CompareOp::Lt
        | CompareOp::Lte
        | CompareOp::Gt
        | CompareOp::Gte => {
            if let Some((value, coercion)) = canonicalize_sql_compare_literal_for_kind(
                &field_kind,
                cmp.op,
                &cmp.value,
                cmp.coercion.id,
            ) {
                cmp.value = value;
                cmp.coercion = coercion;
            }
        }
        CompareOp::In | CompareOp::NotIn => {
            let Value::List(items) = &cmp.value else {
                return;
            };

            if let Some((items, coercion)) = canonicalize_sql_compare_list_for_kind(
                &field_kind,
                cmp.op,
                items.as_slice(),
                cmp.coercion.id,
            ) {
                cmp.value = Value::List(items);
                cmp.coercion = coercion;
            }
        }
        CompareOp::Contains | CompareOp::StartsWith | CompareOp::EndsWith => {}
    }
}

fn canonicalize_sql_compare_literal_for_kind(
    kind: &FieldKind,
    op: CompareOp,
    value: &Value,
    coercion: CoercionId,
) -> Option<(Value, CoercionSpec)> {
    let coercion = match (coercion, op) {
        (CoercionId::Strict, _) => CoercionSpec::new(CoercionId::Strict),
        (CoercionId::NumericWiden, CompareOp::Eq | CompareOp::Ne) => {
            CoercionSpec::new(CoercionId::Strict)
        }
        _ => return None,
    };
    let value = canonicalize_strict_sql_literal_for_kind(kind, value)?;

    Some((value, coercion))
}

fn canonicalize_sql_compare_list_for_kind(
    kind: &FieldKind,
    op: CompareOp,
    items: &[Value],
    coercion: CoercionId,
) -> Option<(Vec<Value>, CoercionSpec)> {
    let coercion = match (coercion, op) {
        (CoercionId::Strict, _) => CoercionSpec::new(CoercionId::Strict),
        (CoercionId::NumericWiden, CompareOp::In | CompareOp::NotIn) => {
            CoercionSpec::new(CoercionId::Strict)
        }
        _ => return None,
    };
    let items = items
        .iter()
        .map(|item| canonicalize_strict_sql_literal_for_kind(kind, item))
        .collect::<Option<Vec<_>>>()?;

    Some((items, coercion))
}

// Convert one parsed strict SQL literal into the exact runtime `Value` variant
// required by the field kind when that conversion is lossless and unambiguous.
// This keeps SQL string tokens usable for scalar key types like `Ulid` without
// widening text coercion across the general predicate surface.
pub(in crate::db) fn canonicalize_strict_sql_literal_for_kind(
    kind: &FieldKind,
    value: &Value,
) -> Option<Value> {
    match kind {
        FieldKind::Relation { key_kind, .. } => {
            canonicalize_strict_sql_literal_for_kind(key_kind, value)
        }
        FieldKind::Int => match value {
            Value::Int(inner) => Some(Value::Int(*inner)),
            Value::Uint(inner) => i64::try_from(*inner).ok().map(Value::Int),
            _ => None,
        },
        FieldKind::Uint => match value {
            Value::Int(inner) => u64::try_from(*inner).ok().map(Value::Uint),
            Value::Uint(inner) => Some(Value::Uint(*inner)),
            _ => None,
        },
        FieldKind::Ulid => match value {
            Value::Text(inner) => Ulid::from_str(inner).ok().map(Value::Ulid),
            Value::Ulid(inner) => Some(Value::Ulid(*inner)),
            _ => None,
        },
        FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Enum { .. }
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::List(_)
        | FieldKind::Map { .. }
        | FieldKind::Principal
        | FieldKind::Set(_)
        | FieldKind::Structured { .. }
        | FieldKind::Subaccount
        | FieldKind::Text
        | FieldKind::Timestamp
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Unit => None,
    }
}
