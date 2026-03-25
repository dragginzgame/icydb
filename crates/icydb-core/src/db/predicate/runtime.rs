//! Module: predicate::runtime
//! Responsibility: compile/evaluate executable predicates against entities.
//! Does not own: schema validation or normalization policy.
//! Boundary: executor row filtering uses this runtime program.

use crate::{
    db::{
        data::{ScalarSlotValueRef, ScalarValueRef, SlotReader, decode_slot_value_by_contract},
        predicate::{
            CoercionId, CoercionSpec, CompareOp, ComparePredicate, ExecutableComparePredicate,
            ExecutablePredicate, Predicate, PredicateCapabilityContext, PredicateExecutionModel,
            ScalarPredicateCapability, TextOp, classify_predicate_capabilities, compare_eq,
            compare_order, compare_text,
        },
    },
    model::{
        entity::{EntityModel, resolve_field_slot},
        field::LeafCodec,
    },
    value::{TextMode, Value},
};
use std::cmp::Ordering;

///
/// PredicateProgram
///
/// Canonical executable predicate program for runtime row filtering.
/// Field names are resolved once during setup; runtime and index paths consume
/// the same structural execution tree.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct PredicateProgram {
    executable: ExecutablePredicate,
    compiled: CompiledPredicate,
}

///
/// CompiledPredicate
///
/// Execution-mode-specific predicate program selected once at lowering time.
/// Scalar programs never route through generic `Value` fallback during
/// structural slot evaluation.
///

#[derive(Clone, Debug)]
enum CompiledPredicate {
    Scalar(ScalarPredicateProgram),
    Generic(GenericPredicateProgram),
}

///
/// GenericPredicateProgram
///
/// Marker for the executable-predicate generic executor path.
/// The canonical executable tree remains the source of truth for generic
/// evaluation and for downstream consumers that still inspect the predicate
/// execution boundary.
///

#[derive(Clone, Copy, Debug, Default)]
struct GenericPredicateProgram;

///
/// ScalarPredicateProgram
///
/// Marker that scalar-only execution is valid for the canonical executable
/// predicate tree.
///

#[derive(Clone, Copy, Debug, Default)]
struct ScalarPredicateProgram;

impl PredicateProgram {
    /// Compile a predicate into a slot-based executable form using structural model data only.
    #[must_use]
    pub(in crate::db) fn compile_with_model(
        model: &'static EntityModel,
        predicate: &PredicateExecutionModel,
    ) -> Self {
        let executable = compile_predicate_program(model, predicate);
        let compiled = compile_scalar_predicate_program(model, &executable).map_or(
            CompiledPredicate::Generic(GenericPredicateProgram),
            CompiledPredicate::Scalar,
        );

        Self {
            executable,
            compiled,
        }
    }

    /// Evaluate one precompiled predicate program against one slot-reader callback.
    #[must_use]
    pub(in crate::db) fn eval_with_slot_reader(
        &self,
        read_slot: &mut dyn FnMut(usize) -> Option<Value>,
    ) -> bool {
        eval_with_executable_slots(&self.executable, read_slot)
    }

    /// Evaluate one precompiled predicate program against one structural slot reader.
    pub(in crate::db) fn eval_with_structural_slot_reader(
        &self,
        slots: &dyn SlotReader,
    ) -> Result<bool, crate::error::InternalError> {
        match &self.compiled {
            CompiledPredicate::Scalar(program) => {
                eval_scalar_predicate_program(*program, &self.executable, slots)
            }
            CompiledPredicate::Generic(_) => eval_with_structural_slots(&self.executable, slots),
        }
    }

    /// Borrow the canonical executable predicate tree used by runtime evaluators.
    #[must_use]
    pub(in crate::db) const fn executable(&self) -> &ExecutablePredicate {
        &self.executable
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) const fn uses_scalar_program(&self) -> bool {
        matches!(self.compiled, CompiledPredicate::Scalar(_))
    }
}

///
/// FieldPresence
///
/// Result of attempting to read a field from an entity during slot-based
/// predicate evaluation.
///

enum FieldPresence {
    Present(Value),
    Missing,
}

/// Compile field-name predicates to stable field-slot predicates once per query.
fn compile_predicate_program(
    model: &'static EntityModel,
    predicate: &PredicateExecutionModel,
) -> ExecutablePredicate {
    fn resolve_field(model: &'static EntityModel, field_name: &str) -> Option<usize> {
        resolve_field_slot(model, field_name)
    }

    // Compile field-name predicates into slot-index predicates once per query.
    match predicate {
        Predicate::True => ExecutablePredicate::True,
        Predicate::False => ExecutablePredicate::False,
        Predicate::And(children) => ExecutablePredicate::And(
            children
                .iter()
                .map(|child| compile_predicate_program(model, child))
                .collect::<Vec<_>>(),
        ),
        Predicate::Or(children) => ExecutablePredicate::Or(
            children
                .iter()
                .map(|child| compile_predicate_program(model, child))
                .collect::<Vec<_>>(),
        ),
        Predicate::Not(inner) => {
            ExecutablePredicate::Not(Box::new(compile_predicate_program(model, inner)))
        }
        Predicate::Compare(ComparePredicate {
            field,
            op,
            value,
            coercion,
        }) => ExecutablePredicate::Compare(ExecutableComparePredicate {
            field_slot: resolve_field(model, field),
            op: *op,
            value: value.clone(),
            coercion: coercion.clone(),
        }),
        Predicate::IsNull { field } => ExecutablePredicate::IsNull {
            field_slot: resolve_field(model, field),
        },
        Predicate::IsNotNull { field } => ExecutablePredicate::IsNotNull {
            field_slot: resolve_field(model, field),
        },
        Predicate::IsMissing { field } => ExecutablePredicate::IsMissing {
            field_slot: resolve_field(model, field),
        },
        Predicate::IsEmpty { field } => ExecutablePredicate::IsEmpty {
            field_slot: resolve_field(model, field),
        },
        Predicate::IsNotEmpty { field } => ExecutablePredicate::IsNotEmpty {
            field_slot: resolve_field(model, field),
        },
        Predicate::TextContains { field, value } => ExecutablePredicate::TextContains {
            field_slot: resolve_field(model, field),
            value: value.clone(),
        },
        Predicate::TextContainsCi { field, value } => ExecutablePredicate::TextContainsCi {
            field_slot: resolve_field(model, field),
            value: value.clone(),
        },
    }
}

// Admit scalar fast-path execution only when the canonical executable tree
// stays entirely on the scalar slot seam.
fn compile_scalar_predicate_program(
    model: &'static EntityModel,
    predicate: &ExecutablePredicate,
) -> Option<ScalarPredicateProgram> {
    (classify_predicate_capabilities(predicate, PredicateCapabilityContext::runtime(model))
        .scalar()
        == ScalarPredicateCapability::ScalarSafe)
        .then_some(ScalarPredicateProgram)
}

/// Read one field by pre-resolved slot through one runtime slot reader.
fn field_from_slot(
    field_slot: Option<usize>,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> FieldPresence {
    let value = field_slot.and_then(read_slot);

    match value {
        Some(value) => FieldPresence::Present(value),
        None => FieldPresence::Missing,
    }
}

/// Evaluate one slot-based field predicate only when the field is present.
fn on_present_slot(
    field_slot: Option<usize>,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
    f: impl FnOnce(&Value) -> bool,
) -> bool {
    match field_from_slot(field_slot, read_slot) {
        FieldPresence::Present(value) => f(&value),
        FieldPresence::Missing => false,
    }
}

/// Evaluate one executable predicate against one runtime slot reader.
fn eval_with_executable_slots(
    predicate: &ExecutablePredicate,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> bool {
    // Evaluate recursively against the canonical executable predicate tree.
    match predicate {
        ExecutablePredicate::True => true,
        ExecutablePredicate::False => false,
        ExecutablePredicate::And(children) => {
            for child in children {
                if !eval_with_executable_slots(child, read_slot) {
                    return false;
                }
            }

            true
        }
        ExecutablePredicate::Or(children) => {
            for child in children {
                if eval_with_executable_slots(child, read_slot) {
                    return true;
                }
            }

            false
        }
        ExecutablePredicate::Not(inner) => !eval_with_executable_slots(inner, read_slot),
        ExecutablePredicate::Compare(cmp) => eval_compare_with_executable_slots(cmp, read_slot),
        ExecutablePredicate::IsNull { field_slot } => {
            matches!(
                field_from_slot(*field_slot, read_slot),
                FieldPresence::Present(Value::Null)
            )
        }
        ExecutablePredicate::IsNotNull { field_slot } => {
            matches!(field_from_slot(*field_slot, read_slot), FieldPresence::Present(value) if !matches!(value, Value::Null))
        }
        ExecutablePredicate::IsMissing { field_slot } => {
            matches!(
                field_from_slot(*field_slot, read_slot),
                FieldPresence::Missing
            )
        }
        ExecutablePredicate::IsEmpty { field_slot } => {
            on_present_slot(*field_slot, read_slot, is_empty_value)
        }
        ExecutablePredicate::IsNotEmpty { field_slot } => {
            on_present_slot(*field_slot, read_slot, |value| !is_empty_value(value))
        }
        ExecutablePredicate::TextContains { field_slot, value } => {
            on_present_slot(*field_slot, read_slot, |actual| {
                actual.text_contains(value, TextMode::Cs).unwrap_or(false)
            })
        }
        ExecutablePredicate::TextContainsCi { field_slot, value } => {
            on_present_slot(*field_slot, read_slot, |actual| {
                actual.text_contains(value, TextMode::Ci).unwrap_or(false)
            })
        }
    }
}

/// Evaluate an executable comparison predicate against one runtime slot reader.
fn eval_compare_with_executable_slots(
    cmp: &ExecutableComparePredicate,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> bool {
    let FieldPresence::Present(actual) = field_from_slot(cmp.field_slot, read_slot) else {
        return false;
    };

    eval_compare_values(&actual, cmp.op, &cmp.value, &cmp.coercion)
}

// Evaluate one scalar-only compiled predicate program without generic fallback.
fn eval_scalar_predicate_program(
    _program: ScalarPredicateProgram,
    predicate: &ExecutablePredicate,
    slots: &dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    eval_scalar_executable_predicate(predicate, slots)
}

// Evaluate one executable predicate tree through scalar-only slot reads.
fn eval_scalar_executable_predicate(
    predicate: &ExecutablePredicate,
    slots: &dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    match predicate {
        ExecutablePredicate::True => Ok(true),
        ExecutablePredicate::False => Ok(false),
        ExecutablePredicate::And(children) => {
            eval_all_children_result(children, slots, eval_scalar_executable_predicate)
        }
        ExecutablePredicate::Or(children) => {
            eval_any_children_result(children, slots, eval_scalar_executable_predicate)
        }
        ExecutablePredicate::Not(inner) => Ok(!eval_scalar_executable_predicate(inner, slots)?),
        ExecutablePredicate::Compare(cmp) => eval_scalar_executable_compare_predicate(cmp, slots),
        ExecutablePredicate::IsNull { field_slot } => Ok(matches!(
            slots.get_scalar(field_slot.expect("scalar fast path validated field slot"))?,
            Some(ScalarSlotValueRef::Null)
        )),
        ExecutablePredicate::IsNotNull { field_slot } => Ok(matches!(
            slots.get_scalar(field_slot.expect("scalar fast path validated field slot"))?,
            Some(ScalarSlotValueRef::Value(_))
        )),
        ExecutablePredicate::IsMissing { field_slot } => {
            Ok(field_slot.is_none_or(|field_slot| !slots.has(field_slot)))
        }
        ExecutablePredicate::IsEmpty { field_slot } => eval_scalar_is_empty(
            field_slot.expect("scalar fast path validated field slot"),
            slots,
        ),
        ExecutablePredicate::IsNotEmpty { field_slot } => eval_scalar_is_not_empty(
            field_slot.expect("scalar fast path validated field slot"),
            slots,
        ),
        ExecutablePredicate::TextContains { field_slot, value } => {
            let Value::Text(needle) = value else {
                return Ok(false);
            };
            eval_scalar_text_contains(
                field_slot.expect("scalar fast path validated field slot"),
                needle,
                TextMode::Cs,
                slots,
            )
        }
        ExecutablePredicate::TextContainsCi { field_slot, value } => {
            let Value::Text(needle) = value else {
                return Ok(false);
            };
            eval_scalar_text_contains(
                field_slot.expect("scalar fast path validated field slot"),
                needle,
                TextMode::Ci,
                slots,
            )
        }
    }
}

// Evaluate one scalar `IS EMPTY` node directly through the scalar slot seam.
fn eval_scalar_is_empty(
    field_slot: usize,
    slots: &dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(actual) = slots.get_scalar(field_slot)? else {
        return Ok(false);
    };

    Ok(match actual {
        ScalarSlotValueRef::Value(ScalarValueRef::Text(text)) => text.is_empty(),
        ScalarSlotValueRef::Value(ScalarValueRef::Blob(bytes)) => bytes.is_empty(),
        ScalarSlotValueRef::Null | ScalarSlotValueRef::Value(_) => false,
    })
}

// Evaluate one scalar `IS NOT EMPTY` node directly through the scalar slot seam.
fn eval_scalar_is_not_empty(
    field_slot: usize,
    slots: &dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    if !slots.has(field_slot) {
        return Ok(false);
    }

    eval_scalar_is_empty(field_slot, slots).map(|empty| !empty)
}

// Evaluate one scalar text-contains node directly through the scalar slot seam.
fn eval_scalar_text_contains(
    field_slot: usize,
    needle: &str,
    mode: TextMode,
    slots: &dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(actual) = slots.get_scalar(field_slot)? else {
        return Ok(false);
    };

    Ok(match actual {
        ScalarSlotValueRef::Value(ScalarValueRef::Text(actual)) => {
            text_contains_scalar(actual, needle, mode)
        }
        ScalarSlotValueRef::Null | ScalarSlotValueRef::Value(_) => false,
    })
}

// Evaluate one executable comparison against one slot reader under the scalar fast path.
fn eval_scalar_executable_compare_predicate(
    cmp: &ExecutableComparePredicate,
    slots: &dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(field_slot) = cmp.field_slot else {
        return Ok(false);
    };
    let Some(actual) = slots.get_scalar(field_slot)? else {
        return Ok(false);
    };

    Ok(eval_compare_scalar_slot(
        actual,
        cmp.op,
        &cmp.value,
        &cmp.coercion,
    )
    .unwrap_or_else(|| {
        debug_assert!(
            false,
            "scalar executable predicate path admitted unsupported compare node: op={:?} coercion={:?} value={:?}",
            cmp.op,
            cmp.coercion,
            cmp.value,
        );
        false
    }))
}

// Evaluate one executable predicate against one structural slot reader.
fn eval_with_structural_slots(
    predicate: &ExecutablePredicate,
    slots: &dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    match predicate {
        ExecutablePredicate::True => Ok(true),
        ExecutablePredicate::False => Ok(false),
        ExecutablePredicate::And(children) => {
            eval_all_children_result(children, slots, eval_with_structural_slots)
        }
        ExecutablePredicate::Or(children) => {
            eval_any_children_result(children, slots, eval_with_structural_slots)
        }
        ExecutablePredicate::Not(inner) => Ok(!eval_with_structural_slots(inner, slots)?),
        ExecutablePredicate::Compare(cmp) => eval_compare_with_structural_slots(cmp, slots),
        ExecutablePredicate::IsNull { field_slot } => {
            eval_is_null_with_structural_slots(*field_slot, slots)
        }
        ExecutablePredicate::IsNotNull { field_slot } => {
            eval_is_not_null_with_structural_slots(*field_slot, slots)
        }
        ExecutablePredicate::IsMissing { field_slot } => {
            Ok(field_slot.is_none_or(|slot| !slots.has(slot)))
        }
        ExecutablePredicate::IsEmpty { field_slot } => {
            eval_is_empty_with_structural_slots(*field_slot, slots)
        }
        ExecutablePredicate::IsNotEmpty { field_slot } => {
            eval_is_not_empty_with_structural_slots(*field_slot, slots)
        }
        ExecutablePredicate::TextContains { field_slot, value } => {
            eval_text_contains_with_structural_slots(*field_slot, value, TextMode::Cs, slots)
        }
        ExecutablePredicate::TextContainsCi { field_slot, value } => {
            eval_text_contains_with_structural_slots(*field_slot, value, TextMode::Ci, slots)
        }
    }
}

// Evaluate one comparison predicate through the structural slot seam.
fn eval_compare_with_structural_slots(
    cmp: &ExecutableComparePredicate,
    slots: &dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(field_slot) = cmp.field_slot else {
        return Ok(false);
    };
    let Some(field) = slots.model().fields().get(field_slot) else {
        return Ok(false);
    };

    if matches!(field.leaf_codec(), LeafCodec::Scalar(_))
        && let Some(actual) = slots.get_scalar(field_slot)?
        && let Some(result) = eval_compare_scalar_slot(actual, cmp.op, &cmp.value, &cmp.coercion)
    {
        return Ok(result);
    }

    let Some(actual) = decode_slot_value_by_contract(slots, field_slot)? else {
        return Ok(false);
    };

    Ok(eval_compare_values(
        &actual,
        cmp.op,
        &cmp.value,
        &cmp.coercion,
    ))
}

// Evaluate one logical `AND` child list through a shared fallible predicate walker.
fn eval_all_children_result<T>(
    children: &[ExecutablePredicate],
    input: T,
    eval_child: fn(&ExecutablePredicate, T) -> Result<bool, crate::error::InternalError>,
) -> Result<bool, crate::error::InternalError>
where
    T: Copy,
{
    for child in children {
        if !eval_child(child, input)? {
            return Ok(false);
        }
    }

    Ok(true)
}

// Evaluate one logical `OR` child list through a shared fallible predicate walker.
fn eval_any_children_result<T>(
    children: &[ExecutablePredicate],
    input: T,
    eval_child: fn(&ExecutablePredicate, T) -> Result<bool, crate::error::InternalError>,
) -> Result<bool, crate::error::InternalError>
where
    T: Copy,
{
    for child in children {
        if eval_child(child, input)? {
            return Ok(true);
        }
    }

    Ok(false)
}

// Evaluate `IS NULL` through the structural slot seam.
fn eval_is_null_with_structural_slots(
    field_slot: Option<usize>,
    slots: &dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(field_slot) = field_slot else {
        return Ok(false);
    };
    let Some(field) = slots.model().fields().get(field_slot) else {
        return Ok(false);
    };

    if matches!(field.leaf_codec(), LeafCodec::Scalar(_)) {
        return Ok(matches!(
            slots.get_scalar(field_slot)?,
            Some(ScalarSlotValueRef::Null)
        ));
    }

    Ok(matches!(
        decode_slot_value_by_contract(slots, field_slot)?,
        Some(Value::Null)
    ))
}

// Evaluate `IS NOT NULL` through the structural slot seam.
fn eval_is_not_null_with_structural_slots(
    field_slot: Option<usize>,
    slots: &dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(field_slot) = field_slot else {
        return Ok(false);
    };
    let Some(field) = slots.model().fields().get(field_slot) else {
        return Ok(false);
    };

    if matches!(field.leaf_codec(), LeafCodec::Scalar(_)) {
        return Ok(matches!(
            slots.get_scalar(field_slot)?,
            Some(ScalarSlotValueRef::Value(_))
        ));
    }

    Ok(matches!(
        decode_slot_value_by_contract(slots, field_slot)?,
        Some(value) if !matches!(value, Value::Null)
    ))
}

// Evaluate `IS EMPTY` through the structural slot seam.
fn eval_is_empty_with_structural_slots(
    field_slot: Option<usize>,
    slots: &dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(field_slot) = field_slot else {
        return Ok(false);
    };
    let Some(field) = slots.model().fields().get(field_slot) else {
        return Ok(false);
    };

    if matches!(field.leaf_codec(), LeafCodec::Scalar(_))
        && let Some(actual) = slots.get_scalar(field_slot)?
    {
        return Ok(match actual {
            ScalarSlotValueRef::Value(ScalarValueRef::Text(text)) => text.is_empty(),
            ScalarSlotValueRef::Value(ScalarValueRef::Blob(bytes)) => bytes.is_empty(),
            ScalarSlotValueRef::Null | ScalarSlotValueRef::Value(_) => false,
        });
    }

    Ok(decode_slot_value_by_contract(slots, field_slot)?
        .is_some_and(|value| is_empty_value(&value)))
}

// Evaluate `IS NOT EMPTY` through the structural slot seam.
fn eval_is_not_empty_with_structural_slots(
    field_slot: Option<usize>,
    slots: &dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(field_slot) = field_slot else {
        return Ok(false);
    };
    if !slots.has(field_slot) {
        return Ok(false);
    }

    eval_is_empty_with_structural_slots(Some(field_slot), slots).map(|empty| !empty)
}

// Evaluate `TEXT CONTAINS` through the structural slot seam.
fn eval_text_contains_with_structural_slots(
    field_slot: Option<usize>,
    value: &Value,
    mode: TextMode,
    slots: &dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(field_slot) = field_slot else {
        return Ok(false);
    };
    let Some(field) = slots.model().fields().get(field_slot) else {
        return Ok(false);
    };

    if matches!(field.leaf_codec(), LeafCodec::Scalar(_))
        && let Some(actual) = slots.get_scalar(field_slot)?
    {
        return Ok(match (actual, value) {
            (ScalarSlotValueRef::Value(ScalarValueRef::Text(actual)), Value::Text(needle)) => {
                text_contains_scalar(actual, needle, mode)
            }
            _ => false,
        });
    }

    Ok(decode_slot_value_by_contract(slots, field_slot)?
        .is_some_and(|actual| actual.text_contains(value, mode).unwrap_or(false)))
}

// Evaluate one compare op directly against one scalar slot value when possible.
fn eval_compare_scalar_slot(
    actual: ScalarSlotValueRef<'_>,
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
) -> Option<bool> {
    match actual {
        ScalarSlotValueRef::Null => eval_null_scalar_compare(op, value, coercion),
        ScalarSlotValueRef::Value(ScalarValueRef::Text(actual)) => {
            eval_text_scalar_compare(actual, op, value, coercion)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Blob(actual)) => {
            eval_blob_scalar_compare(actual, op, value, coercion)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Bool(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_bool_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Date(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_date_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Duration(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_duration_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Float32(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_float32_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Float64(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_float64_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Int(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_int_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Principal(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_principal_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Subaccount(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_subaccount_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Timestamp(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_timestamp_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Uint(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_uint_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Ulid(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_ulid_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Unit) => {
            eval_direct_scalar_compare((), op, value, coercion, scalar_unit_from_value)
        }
    }
}

// Evaluate one strict scalar compare directly against the predicate literal and
// literal lists, leaving only unsupported coercions on the generic fallback.
fn eval_direct_scalar_compare<T>(
    actual: T,
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
    decode: impl Fn(&Value) -> Option<T>,
) -> Option<bool>
where
    T: Copy + Eq + Ord,
{
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => match op {
            CompareOp::Eq
            | CompareOp::Ne
            | CompareOp::Lt
            | CompareOp::Lte
            | CompareOp::Gt
            | CompareOp::Gte => Some(eval_ordered_scalar_compare(actual, op, value, decode)),
            CompareOp::In => Some(scalar_in_list(actual, value, decode).unwrap_or(false)),
            CompareOp::NotIn => {
                Some(scalar_in_list(actual, value, decode).is_some_and(|matched| !matched))
            }
            CompareOp::Contains | CompareOp::StartsWith | CompareOp::EndsWith => Some(false),
        },
        CoercionId::TextCasefold => Some(false),
        CoercionId::NumericWiden => None,
    }
}

// Evaluate one ordered scalar literal compare after decoding the predicate
// literal exactly once for the whole compare branch.
fn eval_ordered_scalar_compare<T>(
    actual: T,
    op: CompareOp,
    value: &Value,
    decode: impl Fn(&Value) -> Option<T>,
) -> bool
where
    T: Copy + Ord,
{
    let Some(expected) = decode(value) else {
        return false;
    };

    match op {
        CompareOp::Eq => actual == expected,
        CompareOp::Ne => actual != expected,
        CompareOp::Lt => actual < expected,
        CompareOp::Lte => actual <= expected,
        CompareOp::Gt => actual > expected,
        CompareOp::Gte => actual >= expected,
        CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => false,
    }
}

// Evaluate direct blob equality/list membership without rebuilding `Value::Blob`.
fn eval_blob_scalar_compare(
    actual: &[u8],
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
) -> Option<bool> {
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => match op {
            CompareOp::Eq => {
                Some(matches!(value, Value::Blob(expected) if actual == expected.as_slice()))
            }
            CompareOp::Ne => {
                Some(matches!(value, Value::Blob(expected) if actual != expected.as_slice()))
            }
            CompareOp::In => Some(blob_in_list(actual, value).unwrap_or(false)),
            CompareOp::NotIn => Some(blob_in_list(actual, value).is_some_and(|matched| !matched)),
            CompareOp::Lt
            | CompareOp::Lte
            | CompareOp::Gt
            | CompareOp::Gte
            | CompareOp::Contains
            | CompareOp::StartsWith
            | CompareOp::EndsWith => Some(false),
        },
        CoercionId::TextCasefold => Some(false),
        CoercionId::NumericWiden => None,
    }
}

// Evaluate direct null comparisons without rebuilding `Value::Null`.
fn eval_null_scalar_compare(op: CompareOp, value: &Value, coercion: &CoercionSpec) -> Option<bool> {
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => match op {
            CompareOp::Eq => Some(matches!(value, Value::Null)),
            CompareOp::Ne
            | CompareOp::Lt
            | CompareOp::Lte
            | CompareOp::Gt
            | CompareOp::Gte
            | CompareOp::Contains
            | CompareOp::StartsWith
            | CompareOp::EndsWith => Some(false),
            CompareOp::In => Some(null_in_list(value).unwrap_or(false)),
            CompareOp::NotIn => Some(null_in_list(value).is_some_and(|matched| !matched)),
        },
        CoercionId::TextCasefold => Some(false),
        CoercionId::NumericWiden => None,
    }
}

// Evaluate one scalar text compare without allocating an owned `Value::Text`.
fn eval_text_scalar_compare(
    actual: &str,
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
) -> Option<bool> {
    let mode = match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => TextMode::Cs,
        CoercionId::TextCasefold => TextMode::Ci,
        CoercionId::NumericWiden => return None,
    };

    match op {
        CompareOp::Eq
        | CompareOp::Ne
        | CompareOp::Lt
        | CompareOp::Lte
        | CompareOp::Gt
        | CompareOp::Gte => Some(eval_text_scalar_order_compare(actual, op, value, mode)),
        CompareOp::StartsWith => Some(
            matches!(value, Value::Text(expected) if text_starts_with_scalar(actual, expected, mode)),
        ),
        CompareOp::EndsWith => Some(
            matches!(value, Value::Text(expected) if text_ends_with_scalar(actual, expected, mode)),
        ),
        CompareOp::In => Some(text_in_list(actual, value, mode).unwrap_or(false)),
        CompareOp::NotIn => Some(text_in_list(actual, value, mode).is_some_and(|matched| !matched)),
        CompareOp::Contains => Some(false),
    }
}

// Evaluate one ordered text compare against one scalar text value without
// repeating the literal-match and canonical text compare path for each op.
fn eval_text_scalar_order_compare(
    actual: &str,
    op: CompareOp,
    value: &Value,
    mode: TextMode,
) -> bool {
    let Value::Text(expected) = value else {
        return false;
    };

    let ordering = compare_scalar_text(actual, expected, mode);
    match op {
        CompareOp::Eq => ordering == Ordering::Equal,
        CompareOp::Ne => ordering != Ordering::Equal,
        CompareOp::Lt => ordering.is_lt(),
        CompareOp::Lte => ordering.is_le(),
        CompareOp::Gt => ordering.is_gt(),
        CompareOp::Gte => ordering.is_ge(),
        CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => false,
    }
}

fn scalar_in_list<T>(actual: T, list: &Value, decode: impl Fn(&Value) -> Option<T>) -> Option<bool>
where
    T: Copy + Eq,
{
    let Value::List(items) = list else {
        return None;
    };

    let mut saw_valid = false;
    for item in items {
        if let Some(expected) = decode(item) {
            if actual == expected {
                return Some(true);
            }
            saw_valid = true;
        }
    }

    saw_valid.then_some(false)
}

fn blob_in_list(actual: &[u8], list: &Value) -> Option<bool> {
    let Value::List(items) = list else {
        return None;
    };

    let mut saw_valid = false;
    for item in items {
        if let Value::Blob(expected) = item {
            if actual == expected.as_slice() {
                return Some(true);
            }
            saw_valid = true;
        }
    }

    saw_valid.then_some(false)
}

fn null_in_list(list: &Value) -> Option<bool> {
    let Value::List(items) = list else {
        return None;
    };

    for item in items {
        if matches!(item, Value::Null) {
            return Some(true);
        }
    }

    None
}

fn text_in_list(actual: &str, list: &Value, mode: TextMode) -> Option<bool> {
    let Value::List(items) = list else {
        return None;
    };

    let mut saw_valid = false;
    for item in items {
        if let Value::Text(expected) = item {
            if compare_scalar_text(actual, expected, mode) == Ordering::Equal {
                return Some(true);
            }
            saw_valid = true;
        }
    }

    saw_valid.then_some(false)
}

const fn scalar_bool_from_value(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_date_from_value(value: &Value) -> Option<crate::types::Date> {
    match value {
        Value::Date(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_duration_from_value(value: &Value) -> Option<crate::types::Duration> {
    match value {
        Value::Duration(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_float32_from_value(value: &Value) -> Option<crate::types::Float32> {
    match value {
        Value::Float32(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_float64_from_value(value: &Value) -> Option<crate::types::Float64> {
    match value {
        Value::Float64(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_int_from_value(value: &Value) -> Option<i64> {
    match value {
        Value::Int(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_principal_from_value(value: &Value) -> Option<crate::types::Principal> {
    match value {
        Value::Principal(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_subaccount_from_value(value: &Value) -> Option<crate::types::Subaccount> {
    match value {
        Value::Subaccount(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_timestamp_from_value(value: &Value) -> Option<crate::types::Timestamp> {
    match value {
        Value::Timestamp(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_uint_from_value(value: &Value) -> Option<u64> {
    match value {
        Value::Uint(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_ulid_from_value(value: &Value) -> Option<crate::types::Ulid> {
    match value {
        Value::Ulid(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_unit_from_value(value: &Value) -> Option<()> {
    match value {
        Value::Unit => Some(()),
        _ => None,
    }
}

fn compare_scalar_text(actual: &str, expected: &str, mode: TextMode) -> Ordering {
    match mode {
        TextMode::Cs => actual.cmp(expected),
        TextMode::Ci => casefold_scalar_text(actual).cmp(&casefold_scalar_text(expected)),
    }
}

fn text_contains_scalar(actual: &str, needle: &str, mode: TextMode) -> bool {
    match mode {
        TextMode::Cs => actual.contains(needle),
        TextMode::Ci => casefold_scalar_text(actual).contains(&casefold_scalar_text(needle)),
    }
}

fn text_starts_with_scalar(actual: &str, prefix: &str, mode: TextMode) -> bool {
    match mode {
        TextMode::Cs => actual.starts_with(prefix),
        TextMode::Ci => casefold_scalar_text(actual).starts_with(&casefold_scalar_text(prefix)),
    }
}

fn text_ends_with_scalar(actual: &str, suffix: &str, mode: TextMode) -> bool {
    match mode {
        TextMode::Cs => actual.ends_with(suffix),
        TextMode::Ci => casefold_scalar_text(actual).ends_with(&casefold_scalar_text(suffix)),
    }
}

fn casefold_scalar_text(input: &str) -> String {
    if input.is_ascii() {
        return input.to_ascii_lowercase();
    }

    input.to_lowercase()
}

/// Shared compare-op semantics for slot-path evaluation.
pub(in crate::db) fn eval_compare_values(
    actual: &Value,
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
) -> bool {
    // NOTE: Comparison helpers return None when a comparison is invalid; eval treats that as false.
    match op {
        CompareOp::Eq => compare_eq(actual, value, coercion).unwrap_or(false),
        CompareOp::Ne => compare_eq(actual, value, coercion).is_some_and(|v| !v),

        CompareOp::Lt => compare_order(actual, value, coercion).is_some_and(Ordering::is_lt),
        CompareOp::Lte => compare_order(actual, value, coercion).is_some_and(Ordering::is_le),
        CompareOp::Gt => compare_order(actual, value, coercion).is_some_and(Ordering::is_gt),
        CompareOp::Gte => compare_order(actual, value, coercion).is_some_and(Ordering::is_ge),

        CompareOp::In => in_list(actual, value, coercion).unwrap_or(false),
        CompareOp::NotIn => in_list(actual, value, coercion).is_some_and(|matched| !matched),

        CompareOp::Contains => contains(actual, value, coercion),

        CompareOp::StartsWith => {
            compare_text(actual, value, coercion, TextOp::StartsWith).unwrap_or(false)
        }
        CompareOp::EndsWith => {
            compare_text(actual, value, coercion, TextOp::EndsWith).unwrap_or(false)
        }
    }
}

/// Determine whether a value is considered empty for `IsEmpty` checks.
const fn is_empty_value(value: &Value) -> bool {
    match value {
        Value::Text(text) => text.is_empty(),
        Value::List(items) => items.is_empty(),
        _ => false,
    }
}

/// Check whether a value equals any element in a list.
fn in_list(actual: &Value, list: &Value, coercion: &CoercionSpec) -> Option<bool> {
    let Value::List(items) = list else {
        return None;
    };

    let mut saw_valid = false;
    for item in items {
        match compare_eq(actual, item, coercion) {
            Some(true) => return Some(true),
            Some(false) => saw_valid = true,
            None => {}
        }
    }

    saw_valid.then_some(false)
}

/// Check whether a collection contains another value.
///
/// CONTRACT: text substring matching uses TextContains/TextContainsCi only.
fn contains(actual: &Value, needle: &Value, coercion: &CoercionSpec) -> bool {
    if matches!(actual, Value::Text(_)) {
        return false;
    }
    let Value::List(items) = actual else {
        return false;
    };

    items
        .iter()
        .any(|item| compare_eq(item, needle, coercion).unwrap_or(false))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{PredicateProgram, eval_compare_scalar_slot, eval_compare_values};
    use crate::{
        db::{
            data::{ScalarSlotValueRef, ScalarValueRef, SlotReader},
            predicate::{
                CoercionId, CoercionSpec, CompareOp, ComparePredicate, ExecutablePredicate,
                Predicate,
            },
        },
        error::InternalError,
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel},
        },
        types::{Float32, Principal},
        value::Value,
    };

    static PREDICATE_FIELDS: [FieldModel; 4] = [
        FieldModel::new("id", FieldKind::Ulid),
        FieldModel::new("score", FieldKind::Int),
        FieldModel::new("tags", FieldKind::List(&FieldKind::Text)),
        FieldModel::new("name", FieldKind::Text),
    ];
    static PREDICATE_MODEL: EntityModel = EntityModel::new(
        "PredicateTestEntity",
        "PredicateTestEntity",
        &PREDICATE_FIELDS[0],
        &PREDICATE_FIELDS,
        &[],
    );

    struct PredicateTestSlotReader {
        score: Option<ScalarSlotValueRef<'static>>,
        name: Option<ScalarSlotValueRef<'static>>,
    }

    impl SlotReader for PredicateTestSlotReader {
        fn model(&self) -> &'static EntityModel {
            &PREDICATE_MODEL
        }

        fn has(&self, slot: usize) -> bool {
            match slot {
                1 => self.score.is_some(),
                3 => self.name.is_some(),
                _ => false,
            }
        }

        fn get_bytes(&self, _slot: usize) -> Option<&[u8]> {
            None
        }

        fn get_scalar(&self, slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
            Ok(match slot {
                1 => self.score,
                3 => self.name,
                _ => None,
            })
        }

        crate::db::data::impl_scalar_only_test_slot_reader_get_value!();
    }

    #[test]
    fn scalar_compare_fast_path_matches_value_semantics_for_strict_int_and_text() {
        let strict = CoercionSpec::new(CoercionId::Strict);
        let int_actual = ScalarSlotValueRef::Value(ScalarValueRef::Int(7));
        let text_actual = ScalarSlotValueRef::Value(ScalarValueRef::Text("Alpha"));

        let int_cases = [
            (CompareOp::Eq, Value::Int(7)),
            (CompareOp::Ne, Value::Int(8)),
            (CompareOp::Gt, Value::Int(3)),
            (
                CompareOp::In,
                Value::List(vec![Value::Int(1), Value::Int(7)]),
            ),
            (
                CompareOp::NotIn,
                Value::List(vec![Value::Int(1), Value::Int(2)]),
            ),
        ];
        for (op, expected) in int_cases {
            let direct = eval_compare_scalar_slot(int_actual, op, &expected, &strict);
            let generic = eval_compare_values(&Value::Int(7), op, &expected, &strict);

            assert_eq!(direct, Some(generic), "int fast path diverged for {op:?}");
        }

        let text_cases = [
            (CompareOp::Eq, Value::Text("Alpha".to_string())),
            (CompareOp::StartsWith, Value::Text("Al".to_string())),
            (
                CompareOp::In,
                Value::List(vec![
                    Value::Text("Beta".to_string()),
                    Value::Text("Alpha".to_string()),
                ]),
            ),
            (CompareOp::Contains, Value::Text("ph".to_string())),
        ];
        for (op, expected) in text_cases {
            let direct = eval_compare_scalar_slot(text_actual, op, &expected, &strict);
            let generic =
                eval_compare_values(&Value::Text("Alpha".to_string()), op, &expected, &strict);

            assert_eq!(direct, Some(generic), "text fast path diverged for {op:?}");
        }
    }

    #[test]
    fn scalar_compare_fast_path_falls_back_for_numeric_widen() {
        let numeric = CoercionSpec::new(CoercionId::NumericWiden);
        let actual = ScalarSlotValueRef::Value(ScalarValueRef::Float32(
            Float32::try_new(7.0).expect("finite float should build"),
        ));

        let direct = eval_compare_scalar_slot(actual, CompareOp::Eq, &Value::Int(7), &numeric);

        assert_eq!(
            direct, None,
            "numeric widen should stay on fallback for now"
        );
    }

    #[test]
    fn scalar_compare_fast_path_preserves_strict_variant_mismatch_false() {
        let strict = CoercionSpec::new(CoercionId::Strict);
        let actual = ScalarSlotValueRef::Value(ScalarValueRef::Principal(Principal::anonymous()));

        let eq = eval_compare_scalar_slot(
            actual,
            CompareOp::Eq,
            &Value::Text("x".to_string()),
            &strict,
        );
        let ne = eval_compare_scalar_slot(
            actual,
            CompareOp::Ne,
            &Value::Text("x".to_string()),
            &strict,
        );

        assert_eq!(eq, Some(false));
        assert_eq!(ne, Some(false));
    }

    #[test]
    fn predicate_program_dispatches_scalar_only_predicates_once() {
        let scalar_predicate = Predicate::And(vec![
            Predicate::Compare(ComparePredicate {
                field: "score".to_string(),
                op: CompareOp::Gt,
                value: Value::Int(10),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
            Predicate::Not(Box::new(Predicate::Compare(ComparePredicate {
                field: "score".to_string(),
                op: CompareOp::In,
                value: Value::List(vec![Value::Int(1), Value::Int(2)]),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }))),
        ]);
        let generic_predicate = Predicate::Compare(ComparePredicate {
            field: "tags".to_string(),
            op: CompareOp::Contains,
            value: Value::Text("x".to_string()),
            coercion: CoercionSpec::new(CoercionId::CollectionElement),
        });

        let scalar_program =
            PredicateProgram::compile_with_model(&PREDICATE_MODEL, &scalar_predicate);
        let generic_program =
            PredicateProgram::compile_with_model(&PREDICATE_MODEL, &generic_predicate);

        assert!(scalar_program.uses_scalar_program());
        assert!(!generic_program.uses_scalar_program());
    }

    #[test]
    fn scalar_predicate_program_reuses_canonical_executable_tree() {
        let predicate = Predicate::And(vec![
            Predicate::Compare(ComparePredicate {
                field: "score".to_string(),
                op: CompareOp::Eq,
                value: Value::Int(10),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
            Predicate::Compare(ComparePredicate {
                field: "score".to_string(),
                op: CompareOp::In,
                value: Value::List(vec![Value::Int(1), Value::Int(2)]),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
        ]);

        let program = PredicateProgram::compile_with_model(&PREDICATE_MODEL, &predicate);
        let ExecutablePredicate::And(children) = program.executable() else {
            panic!("expected executable and-predicate");
        };

        let ExecutablePredicate::Compare(eq) = &children[0] else {
            panic!("expected eq compare");
        };
        let ExecutablePredicate::Compare(in_list) = &children[1] else {
            panic!("expected in-list compare");
        };

        assert_eq!(eq.value, Value::Int(10));
        assert_eq!(
            in_list.value,
            Value::List(vec![Value::Int(1), Value::Int(2)]),
        );
    }

    #[test]
    fn scalar_predicate_fast_path_preserves_null_and_variant_mismatch_semantics() {
        let strict = CoercionSpec::new(CoercionId::Strict);

        let null_eq = eval_compare_scalar_slot(
            ScalarSlotValueRef::Null,
            CompareOp::Eq,
            &Value::Null,
            &strict,
        );
        let null_in = eval_compare_scalar_slot(
            ScalarSlotValueRef::Null,
            CompareOp::In,
            &Value::List(vec![Value::Null]),
            &strict,
        );
        let mismatch = eval_compare_scalar_slot(
            ScalarSlotValueRef::Value(ScalarValueRef::Int(7)),
            CompareOp::Eq,
            &Value::Text("x".to_string()),
            &strict,
        );

        assert_eq!(null_eq, Some(true));
        assert_eq!(null_in, Some(true));
        assert_eq!(mismatch, Some(false));
    }

    #[test]
    fn scalar_predicate_fast_path_matches_text_prefix_suffix_semantics() {
        let strict = CoercionSpec::new(CoercionId::Strict);
        let casefold = CoercionSpec::new(CoercionId::TextCasefold);
        let actual = ScalarSlotValueRef::Value(ScalarValueRef::Text("Alpha"));

        let strict_prefix = eval_compare_scalar_slot(
            actual,
            CompareOp::StartsWith,
            &Value::Text("Al".to_string()),
            &strict,
        );
        let ci_suffix = eval_compare_scalar_slot(
            actual,
            CompareOp::EndsWith,
            &Value::Text("HA".to_string()),
            &casefold,
        );

        assert_eq!(strict_prefix, Some(true));
        assert_eq!(ci_suffix, Some(true));
    }

    #[test]
    fn scalar_predicate_program_handles_scalar_non_compare_nodes() {
        let predicate = Predicate::And(vec![
            Predicate::IsNotNull {
                field: "score".to_string(),
            },
            Predicate::IsMissing {
                field: "missing".to_string(),
            },
            Predicate::IsNotEmpty {
                field: "name".to_string(),
            },
            Predicate::TextContainsCi {
                field: "name".to_string(),
                value: Value::Text("alp".to_string()),
            },
        ]);
        let program = PredicateProgram::compile_with_model(&PREDICATE_MODEL, &predicate);
        let slots = PredicateTestSlotReader {
            score: Some(ScalarSlotValueRef::Value(ScalarValueRef::Int(7))),
            name: Some(ScalarSlotValueRef::Value(ScalarValueRef::Text("Alpha"))),
        };

        assert!(program.uses_scalar_program());
        assert!(
            program
                .eval_with_structural_slot_reader(&slots)
                .expect("scalar non-compare predicate should evaluate")
        );
    }

    #[test]
    fn scalar_predicate_program_compiles_text_prefix_suffix_compares() {
        let predicate = Predicate::And(vec![
            Predicate::Compare(ComparePredicate {
                field: "name".to_string(),
                op: CompareOp::StartsWith,
                value: Value::Text("Al".to_string()),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
            Predicate::Compare(ComparePredicate {
                field: "name".to_string(),
                op: CompareOp::EndsWith,
                value: Value::Text("HA".to_string()),
                coercion: CoercionSpec::new(CoercionId::TextCasefold),
            }),
        ]);
        let program = PredicateProgram::compile_with_model(&PREDICATE_MODEL, &predicate);
        let slots = PredicateTestSlotReader {
            score: None,
            name: Some(ScalarSlotValueRef::Value(ScalarValueRef::Text("Alpha"))),
        };

        assert!(program.uses_scalar_program());
        assert!(
            program
                .eval_with_structural_slot_reader(&slots)
                .expect("scalar text prefix/suffix predicate should evaluate")
        );
    }

    #[test]
    fn scalar_predicate_program_audit_covers_expected_scalar_shapes() {
        let scalar_predicates = [
            Predicate::Compare(ComparePredicate {
                field: "score".to_string(),
                op: CompareOp::Eq,
                value: Value::Int(7),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
            Predicate::Compare(ComparePredicate {
                field: "score".to_string(),
                op: CompareOp::In,
                value: Value::List(vec![Value::Int(1), Value::Int(7)]),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
            Predicate::Compare(ComparePredicate {
                field: "name".to_string(),
                op: CompareOp::StartsWith,
                value: Value::Text("Al".to_string()),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
            Predicate::Compare(ComparePredicate {
                field: "name".to_string(),
                op: CompareOp::EndsWith,
                value: Value::Text("HA".to_string()),
                coercion: CoercionSpec::new(CoercionId::TextCasefold),
            }),
            Predicate::IsNull {
                field: "score".to_string(),
            },
            Predicate::IsNotNull {
                field: "score".to_string(),
            },
            Predicate::IsMissing {
                field: "score".to_string(),
            },
            Predicate::IsMissing {
                field: "missing".to_string(),
            },
            Predicate::IsEmpty {
                field: "name".to_string(),
            },
            Predicate::IsNotEmpty {
                field: "name".to_string(),
            },
            Predicate::TextContains {
                field: "name".to_string(),
                value: Value::Text("lp".to_string()),
            },
            Predicate::TextContainsCi {
                field: "name".to_string(),
                value: Value::Text("LP".to_string()),
            },
            Predicate::And(vec![
                Predicate::IsNotNull {
                    field: "score".to_string(),
                },
                Predicate::TextContainsCi {
                    field: "name".to_string(),
                    value: Value::Text("LP".to_string()),
                },
            ]),
            Predicate::Or(vec![
                Predicate::Compare(ComparePredicate {
                    field: "score".to_string(),
                    op: CompareOp::Eq,
                    value: Value::Int(1),
                    coercion: CoercionSpec::new(CoercionId::Strict),
                }),
                Predicate::Compare(ComparePredicate {
                    field: "score".to_string(),
                    op: CompareOp::Eq,
                    value: Value::Int(2),
                    coercion: CoercionSpec::new(CoercionId::Strict),
                }),
            ]),
            Predicate::Not(Box::new(Predicate::IsEmpty {
                field: "name".to_string(),
            })),
        ];

        for predicate in scalar_predicates {
            let program = PredicateProgram::compile_with_model(&PREDICATE_MODEL, &predicate);
            assert!(
                program.uses_scalar_program(),
                "expected scalar program for predicate: {predicate:?}"
            );
        }
    }

    #[test]
    fn scalar_predicate_program_audit_preserves_expected_generic_shapes() {
        let generic_predicates = [
            Predicate::Compare(ComparePredicate {
                field: "score".to_string(),
                op: CompareOp::Eq,
                value: Value::Float32(Float32::try_new(7.0).expect("finite float should build")),
                coercion: CoercionSpec::new(CoercionId::NumericWiden),
            }),
            Predicate::Compare(ComparePredicate {
                field: "tags".to_string(),
                op: CompareOp::Contains,
                value: Value::Text("x".to_string()),
                coercion: CoercionSpec::new(CoercionId::CollectionElement),
            }),
            Predicate::IsEmpty {
                field: "tags".to_string(),
            },
            Predicate::TextContains {
                field: "name".to_string(),
                value: Value::Int(1),
            },
            Predicate::And(vec![
                Predicate::Compare(ComparePredicate {
                    field: "score".to_string(),
                    op: CompareOp::Eq,
                    value: Value::Int(7),
                    coercion: CoercionSpec::new(CoercionId::Strict),
                }),
                Predicate::Compare(ComparePredicate {
                    field: "score".to_string(),
                    op: CompareOp::Eq,
                    value: Value::Float32(
                        Float32::try_new(7.0).expect("finite float should build"),
                    ),
                    coercion: CoercionSpec::new(CoercionId::NumericWiden),
                }),
            ]),
        ];

        for predicate in generic_predicates {
            let program = PredicateProgram::compile_with_model(&PREDICATE_MODEL, &predicate);
            assert!(
                !program.uses_scalar_program(),
                "expected generic program for predicate: {predicate:?}"
            );
        }
    }
}
