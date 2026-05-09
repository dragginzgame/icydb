//! Module: predicate::runtime
//! Responsibility: compile/evaluate executable predicates against entities.
//! Does not own: schema validation or normalization policy.
//! Boundary: executor row filtering uses this runtime program.

mod compare;

#[cfg(test)]
mod tests;

use crate::db::predicate::runtime::compare::{
    eval_compare_scalar_slot, eval_compare_values, is_empty_value, text_contains_scalar,
};
#[cfg(test)]
use crate::model::entity::EntityModel;
use crate::{
    db::{
        data::{CanonicalSlotReader, ScalarSlotValueRef, ScalarValueRef, StructuralRowContract},
        predicate::{
            CoercionSpec, CompareOp, ComparePredicate, ExecutableCompareOperand,
            ExecutableComparePredicate, ExecutablePredicate, Predicate, PredicateCapabilityContext,
            ScalarPredicateCapability, classify_predicate_capabilities,
        },
        query::plan::expr::CompiledPredicate,
        schema::SchemaInfo,
    },
    model::field::LeafCodec,
    value::{TextMode, Value},
};
use std::borrow::Cow;

///
/// PredicateProgram
///
/// Canonical executable predicate program for runtime row filtering.
/// Field names are resolved once during setup; runtime and index paths consume
/// the same structural execution tree.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PredicateProgram {
    executable: ExecutablePredicate,
    compiled: PredicateExecutionMode,
}

///
/// PredicateExecutionMode
///
/// PredicateExecutionMode is selected once at lowering time for the canonical executable
/// predicate tree.
/// Scalar mode never routes through generic `Value` fallback during
/// structural slot evaluation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum PredicateExecutionMode {
    Scalar,
    Generic,
}

impl PredicateProgram {
    /// Compile a predicate into a model-only slot-based executable form.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn compile_for_model_only(
        model: &EntityModel,
        predicate: &Predicate,
    ) -> Self {
        Self::compile_with_schema_info(
            SchemaInfo::cached_for_generated_entity_model(model),
            predicate,
        )
    }

    /// Compile a predicate through explicit schema field-slot and scalar-leaf authority.
    #[must_use]
    pub(in crate::db) fn compile_with_schema_info(
        schema_info: &SchemaInfo,
        predicate: &Predicate,
    ) -> Self {
        let executable = compile_predicate_program_with_resolver(predicate, &|field_name| {
            schema_info.field_slot_index(field_name)
        });
        let compiled = if compile_scalar_predicate_program(schema_info, &executable) {
            PredicateExecutionMode::Scalar
        } else {
            PredicateExecutionMode::Generic
        };

        Self {
            executable,
            compiled,
        }
    }

    /// Compile a predicate through accepted row-contract field-slot authority.
    ///
    /// This deliberately selects the generic structural execution mode. The
    /// structural evaluator still uses accepted-aware scalar helpers where it
    /// can, but predicate slot resolution no longer depends on generated model
    /// field order.
    #[must_use]
    pub(in crate::db) fn compile_with_row_contract(
        row_contract: &StructuralRowContract,
        predicate: &Predicate,
    ) -> Self {
        let executable = compile_predicate_program_with_resolver(predicate, &|field_name| {
            row_contract.field_slot_index_by_name(field_name).ok()
        });

        Self {
            executable,
            compiled: PredicateExecutionMode::Generic,
        }
    }

    /// Evaluate one precompiled predicate program against one borrowed slot
    /// reader so structural row paths do not clone already-decoded values on
    /// every predicate access.
    #[must_use]
    pub(in crate::db) fn eval_with_slot_value_ref_reader<'a, F>(&self, read_slot: &mut F) -> bool
    where
        F: FnMut(usize) -> Option<&'a Value>,
    {
        eval_with_executable_slot_refs(&self.executable, read_slot)
    }

    /// Evaluate one precompiled predicate program against one row reader that
    /// may return either borrowed or owned values per slot access.
    /// This keeps structural row paths on borrowed values while letting typed
    /// fallback rows continue producing owned `Value` payloads through the
    /// same predicate hot loop.
    #[must_use]
    pub(in crate::db) fn eval_with_slot_value_cow_reader<'a, F>(&self, read_slot: &mut F) -> bool
    where
        F: FnMut(usize) -> Option<Cow<'a, Value>>,
    {
        eval_with_executable_slot_cows(&self.executable, read_slot)
    }

    /// Evaluate one precompiled predicate program against one structural slot reader.
    pub(in crate::db) fn eval_with_structural_slot_reader(
        &self,
        slots: &dyn CanonicalSlotReader,
    ) -> Result<bool, crate::error::InternalError> {
        match &self.compiled {
            PredicateExecutionMode::Scalar => {
                eval_scalar_executable_predicate(&self.executable, slots)
            }
            PredicateExecutionMode::Generic => eval_with_structural_slots(&self.executable, slots),
        }
    }

    /// Borrow the canonical executable predicate tree used by runtime evaluators.
    #[must_use]
    pub(in crate::db) const fn executable(&self) -> &ExecutablePredicate {
        &self.executable
    }

    /// Mark every structural slot referenced by this executable predicate.
    pub(in crate::db) fn mark_referenced_slots(&self, required_slots: &mut [bool]) {
        mark_executable_predicate_referenced_slots(&self.executable, required_slots);
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) const fn uses_scalar_program(&self) -> bool {
        matches!(self.compiled, PredicateExecutionMode::Scalar)
    }
}

impl CompiledPredicate for PredicateProgram {
    fn eval(&self, slots: &[Value]) -> bool {
        self.eval_with_slot_value_ref_reader(&mut |slot| slots.get(slot))
    }
}

// Compile field-name predicates to stable field-slot predicates using caller
// supplied slot authority.
fn compile_predicate_program_with_resolver(
    predicate: &Predicate,
    resolve_field: &dyn Fn(&str) -> Option<usize>,
) -> ExecutablePredicate {
    // Compile field-name predicates into slot-index predicates once per query.
    match predicate {
        Predicate::True => ExecutablePredicate::True,
        Predicate::False => ExecutablePredicate::False,
        Predicate::And(children) => ExecutablePredicate::And(
            children
                .iter()
                .map(|child| compile_predicate_program_with_resolver(child, resolve_field))
                .collect::<Vec<_>>(),
        ),
        Predicate::Or(children) => ExecutablePredicate::Or(
            children
                .iter()
                .map(|child| compile_predicate_program_with_resolver(child, resolve_field))
                .collect::<Vec<_>>(),
        ),
        Predicate::Not(inner) => ExecutablePredicate::Not(Box::new(
            compile_predicate_program_with_resolver(inner, resolve_field),
        )),
        Predicate::Compare(ComparePredicate {
            field,
            op,
            value,
            coercion,
        }) => ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
            resolve_field(field),
            *op,
            value.clone(),
            coercion.clone(),
        )),
        Predicate::CompareFields(crate::db::predicate::CompareFieldsPredicate {
            left_field,
            op,
            right_field,
            coercion,
        }) => ExecutablePredicate::Compare(ExecutableComparePredicate::field_field(
            resolve_field(left_field),
            *op,
            resolve_field(right_field),
            coercion.clone(),
        )),
        Predicate::IsNull { field } => ExecutablePredicate::IsNull {
            field_slot: resolve_field(field),
        },
        Predicate::IsNotNull { field } => ExecutablePredicate::IsNotNull {
            field_slot: resolve_field(field),
        },
        Predicate::IsMissing { field } => ExecutablePredicate::IsMissing {
            field_slot: resolve_field(field),
        },
        Predicate::IsEmpty { field } => ExecutablePredicate::IsEmpty {
            field_slot: resolve_field(field),
        },
        Predicate::IsNotEmpty { field } => ExecutablePredicate::IsNotEmpty {
            field_slot: resolve_field(field),
        },
        Predicate::TextContains { field, value } => ExecutablePredicate::TextContains {
            field_slot: resolve_field(field),
            value: value.clone(),
        },
        Predicate::TextContainsCi { field, value } => ExecutablePredicate::TextContainsCi {
            field_slot: resolve_field(field),
            value: value.clone(),
        },
    }
}

// Admit scalar fast-path execution only when the canonical executable tree
// stays entirely on the scalar slot seam.
fn compile_scalar_predicate_program(
    schema_info: &SchemaInfo,
    predicate: &ExecutablePredicate,
) -> bool {
    classify_predicate_capabilities(
        predicate,
        PredicateCapabilityContext::runtime_schema(schema_info),
    )
    .scalar()
        == ScalarPredicateCapability::ScalarSafe
}

// Mark every slot referenced by the canonical executable predicate tree.
fn mark_executable_predicate_referenced_slots(
    predicate: &ExecutablePredicate,
    required_slots: &mut [bool],
) {
    match predicate {
        ExecutablePredicate::True | ExecutablePredicate::False => {}
        ExecutablePredicate::And(children) | ExecutablePredicate::Or(children) => {
            for child in children {
                mark_executable_predicate_referenced_slots(child, required_slots);
            }
        }
        ExecutablePredicate::Not(child) => {
            mark_executable_predicate_referenced_slots(child.as_ref(), required_slots);
        }
        ExecutablePredicate::Compare(compare) => {
            mark_compare_operand_slots(&compare.left, required_slots);
            mark_compare_operand_slots(&compare.right, required_slots);
        }
        ExecutablePredicate::IsNull { field_slot }
        | ExecutablePredicate::IsNotNull { field_slot }
        | ExecutablePredicate::IsMissing { field_slot }
        | ExecutablePredicate::IsEmpty { field_slot }
        | ExecutablePredicate::IsNotEmpty { field_slot }
        | ExecutablePredicate::TextContains { field_slot, .. }
        | ExecutablePredicate::TextContainsCi { field_slot, .. } => {
            mark_predicate_slot(*field_slot, required_slots);
        }
    }
}

// Mark one compare operand when it resolves to a field slot.
fn mark_compare_operand_slots(operand: &ExecutableCompareOperand, required_slots: &mut [bool]) {
    if let ExecutableCompareOperand::FieldSlot(slot) = operand {
        mark_predicate_slot(*slot, required_slots);
    }
}

// Mark one resolved predicate slot when it exists inside the current model
// field span.
fn mark_predicate_slot(slot: Option<usize>, required_slots: &mut [bool]) {
    if let Some(slot) = slot
        && let Some(required) = required_slots.get_mut(slot)
    {
        *required = true;
    }
}

// Read one field by pre-resolved slot through one borrowed runtime slot reader.
fn field_from_slot_ref<'a, F>(field_slot: Option<usize>, read_slot: &mut F) -> Option<&'a Value>
where
    F: FnMut(usize) -> Option<&'a Value>,
{
    field_slot.and_then(read_slot)
}

// Evaluate one slot-based field predicate only when the field is present and
// already available by reference.
fn on_present_slot_ref<'a, F>(
    field_slot: Option<usize>,
    read_slot: &mut F,
    f: impl FnOnce(&Value) -> bool,
) -> bool
where
    F: FnMut(usize) -> Option<&'a Value>,
{
    field_from_slot_ref(field_slot, read_slot).is_some_and(f)
}

// Read one field by pre-resolved slot through one mixed borrowed/owned slot reader.
fn field_from_slot_cow<'a, F>(
    field_slot: Option<usize>,
    read_slot: &mut F,
) -> Option<Cow<'a, Value>>
where
    F: FnMut(usize) -> Option<Cow<'a, Value>>,
{
    field_slot.and_then(read_slot)
}

// Evaluate one slot-based field predicate only when the field is present and
// already available as either borrowed or owned data.
fn on_present_slot_cow<'a, F>(
    field_slot: Option<usize>,
    read_slot: &mut F,
    f: impl FnOnce(&Value) -> bool,
) -> bool
where
    F: FnMut(usize) -> Option<Cow<'a, Value>>,
{
    field_from_slot_cow(field_slot, read_slot).is_some_and(|value| f(value.as_ref()))
}

// Load both compare operands once through the borrowed row-reader seam and
// then route the resolved values into one shared compare callback.
fn with_compare_operands_ref<'a, F, R>(
    cmp: &ExecutableComparePredicate,
    read_slot: &mut F,
    eval: impl FnOnce(&Value, &Value) -> R,
) -> Option<R>
where
    F: FnMut(usize) -> Option<&'a Value>,
{
    let left = match &cmp.left {
        ExecutableCompareOperand::FieldSlot(slot) => field_from_slot_ref(*slot, read_slot)?,
        ExecutableCompareOperand::Literal(value) => value,
    };
    let right = match &cmp.right {
        ExecutableCompareOperand::FieldSlot(slot) => field_from_slot_ref(*slot, read_slot)?,
        ExecutableCompareOperand::Literal(value) => value,
    };

    Some(eval(left, right))
}

// Load both compare operands once through the mixed borrowed/owned row-reader
// seam and then route the resolved values into one shared compare callback.
fn with_compare_operands_cow<'a, F, R>(
    cmp: &ExecutableComparePredicate,
    read_slot: &mut F,
    eval: impl FnOnce(&Value, &Value) -> R,
) -> Option<R>
where
    F: FnMut(usize) -> Option<Cow<'a, Value>>,
{
    let left = match &cmp.left {
        ExecutableCompareOperand::FieldSlot(slot) => field_from_slot_cow(*slot, read_slot)?,
        ExecutableCompareOperand::Literal(value) => Cow::Borrowed(value),
    };
    let right = match &cmp.right {
        ExecutableCompareOperand::FieldSlot(slot) => field_from_slot_cow(*slot, read_slot)?,
        ExecutableCompareOperand::Literal(value) => Cow::Borrowed(value),
    };

    Some(eval(left.as_ref(), right.as_ref()))
}

// Load both compare operands once through the structural slot seam and then
// route the resolved values into one shared compare callback.
fn with_compare_operands_structural<R>(
    cmp: &ExecutableComparePredicate,
    slots: &dyn CanonicalSlotReader,
    eval: impl FnOnce(&Value, &Value) -> R,
) -> Result<Option<R>, crate::error::InternalError> {
    let left = match &cmp.left {
        ExecutableCompareOperand::FieldSlot(slot) => {
            let Some(slot) = slot else {
                return Ok(None);
            };
            slots.required_value_by_contract_cow(*slot)?
        }
        ExecutableCompareOperand::Literal(value) => Cow::Borrowed(value),
    };
    let right = match &cmp.right {
        ExecutableCompareOperand::FieldSlot(slot) => {
            let Some(slot) = slot else {
                return Ok(None);
            };
            slots.required_value_by_contract_cow(*slot)?
        }
        ExecutableCompareOperand::Literal(value) => Cow::Borrowed(value),
    };

    Ok(Some(eval(left.as_ref(), right.as_ref())))
}

// Evaluate one executable predicate against one borrowed runtime slot reader.
fn eval_with_executable_slot_refs<'a, F>(predicate: &ExecutablePredicate, read_slot: &mut F) -> bool
where
    F: FnMut(usize) -> Option<&'a Value>,
{
    match predicate {
        ExecutablePredicate::True => true,
        ExecutablePredicate::False => false,
        ExecutablePredicate::And(children) => {
            for child in children {
                if !eval_with_executable_slot_refs(child, read_slot) {
                    return false;
                }
            }

            true
        }
        ExecutablePredicate::Or(children) => {
            for child in children {
                if eval_with_executable_slot_refs(child, read_slot) {
                    return true;
                }
            }

            false
        }
        ExecutablePredicate::Not(inner) => !eval_with_executable_slot_refs(inner, read_slot),
        ExecutablePredicate::Compare(cmp) => eval_compare_with_executable_slot_refs(cmp, read_slot),
        ExecutablePredicate::IsNull { field_slot } => {
            matches!(
                field_from_slot_ref(*field_slot, read_slot),
                Some(Value::Null)
            )
        }
        ExecutablePredicate::IsNotNull { field_slot } => {
            matches!(
                field_from_slot_ref(*field_slot, read_slot),
                Some(value) if !matches!(value, Value::Null)
            )
        }
        ExecutablePredicate::IsMissing { field_slot } => {
            field_from_slot_ref(*field_slot, read_slot).is_none()
        }
        ExecutablePredicate::IsEmpty { field_slot } => {
            on_present_slot_ref(*field_slot, read_slot, is_empty_value)
        }
        ExecutablePredicate::IsNotEmpty { field_slot } => {
            on_present_slot_ref(*field_slot, read_slot, |value| !is_empty_value(value))
        }
        ExecutablePredicate::TextContains { field_slot, value } => {
            on_present_slot_ref(*field_slot, read_slot, |actual| {
                actual.text_contains(value, TextMode::Cs).unwrap_or(false)
            })
        }
        ExecutablePredicate::TextContainsCi { field_slot, value } => {
            on_present_slot_ref(*field_slot, read_slot, |actual| {
                actual.text_contains(value, TextMode::Ci).unwrap_or(false)
            })
        }
    }
}

// Evaluate one executable predicate against one mixed borrowed/owned runtime
// slot reader.
fn eval_with_executable_slot_cows<'a, F>(predicate: &ExecutablePredicate, read_slot: &mut F) -> bool
where
    F: FnMut(usize) -> Option<Cow<'a, Value>>,
{
    match predicate {
        ExecutablePredicate::True => true,
        ExecutablePredicate::False => false,
        ExecutablePredicate::And(children) => {
            for child in children {
                if !eval_with_executable_slot_cows(child, read_slot) {
                    return false;
                }
            }

            true
        }
        ExecutablePredicate::Or(children) => {
            for child in children {
                if eval_with_executable_slot_cows(child, read_slot) {
                    return true;
                }
            }

            false
        }
        ExecutablePredicate::Not(inner) => !eval_with_executable_slot_cows(inner, read_slot),
        ExecutablePredicate::Compare(cmp) => eval_compare_with_executable_slot_cows(cmp, read_slot),
        ExecutablePredicate::IsNull { field_slot } => {
            matches!(
                field_from_slot_cow(*field_slot, read_slot).as_deref(),
                Some(Value::Null)
            )
        }
        ExecutablePredicate::IsNotNull { field_slot } => {
            matches!(
                field_from_slot_cow(*field_slot, read_slot).as_deref(),
                Some(value) if !matches!(value, Value::Null)
            )
        }
        ExecutablePredicate::IsMissing { field_slot } => {
            field_from_slot_cow(*field_slot, read_slot).is_none()
        }
        ExecutablePredicate::IsEmpty { field_slot } => {
            on_present_slot_cow(*field_slot, read_slot, is_empty_value)
        }
        ExecutablePredicate::IsNotEmpty { field_slot } => {
            on_present_slot_cow(*field_slot, read_slot, |value| !is_empty_value(value))
        }
        ExecutablePredicate::TextContains { field_slot, value } => {
            on_present_slot_cow(*field_slot, read_slot, |actual| {
                actual.text_contains(value, TextMode::Cs).unwrap_or(false)
            })
        }
        ExecutablePredicate::TextContainsCi { field_slot, value } => {
            on_present_slot_cow(*field_slot, read_slot, |actual| {
                actual.text_contains(value, TextMode::Ci).unwrap_or(false)
            })
        }
    }
}

// Evaluate one executable comparison predicate against one borrowed runtime
// slot reader.
fn eval_compare_with_executable_slot_refs<'a, F>(
    cmp: &ExecutableComparePredicate,
    read_slot: &mut F,
) -> bool
where
    F: FnMut(usize) -> Option<&'a Value>,
{
    with_compare_operands_ref(cmp, read_slot, |left, right| {
        eval_compare_values(left, cmp.op, right, &cmp.coercion)
    })
    .unwrap_or(false)
}

// Evaluate one executable comparison predicate against one mixed borrowed/owned
// runtime slot reader.
fn eval_compare_with_executable_slot_cows<'a, F>(
    cmp: &ExecutableComparePredicate,
    read_slot: &mut F,
) -> bool
where
    F: FnMut(usize) -> Option<Cow<'a, Value>>,
{
    with_compare_operands_cow(cmp, read_slot, |left, right| {
        eval_compare_values(left, cmp.op, right, &cmp.coercion)
    })
    .unwrap_or(false)
}

// Evaluate one executable predicate tree through scalar-only slot reads.
fn eval_scalar_executable_predicate(
    predicate: &ExecutablePredicate,
    slots: &dyn CanonicalSlotReader,
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
        ExecutablePredicate::IsNull { field_slot } => eval_required_scalar_slot(
            field_slot.expect("scalar fast path validated field slot"),
            slots,
            |actual| matches!(actual, ScalarSlotValueRef::Null),
        ),
        ExecutablePredicate::IsNotNull { field_slot } => eval_required_scalar_slot(
            field_slot.expect("scalar fast path validated field slot"),
            slots,
            |actual| matches!(actual, ScalarSlotValueRef::Value(_)),
        ),
        ExecutablePredicate::IsMissing { field_slot } => Ok(field_slot.is_none()),
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

// Read one scalar slot once and let the caller classify the already validated
// scalar payload without repeating the required-slot boilerplate.
fn eval_required_scalar_slot(
    field_slot: usize,
    slots: &dyn CanonicalSlotReader,
    eval: impl FnOnce(ScalarSlotValueRef<'_>) -> bool,
) -> Result<bool, crate::error::InternalError> {
    Ok(eval(slots.required_scalar(field_slot)?))
}

// Read one non-scalar value slot once and let the caller classify the decoded
// canonical value without repeating the required-slot decode boilerplate.
fn eval_required_value_slot(
    field_slot: usize,
    slots: &dyn CanonicalSlotReader,
    eval: impl FnOnce(&Value) -> bool,
) -> Result<bool, crate::error::InternalError> {
    let actual = slots.required_value_by_contract_cow(field_slot)?;

    Ok(eval(actual.as_ref()))
}

// Evaluate one scalar `IS EMPTY` node directly through the scalar slot seam.
fn eval_scalar_is_empty(
    field_slot: usize,
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, crate::error::InternalError> {
    eval_required_scalar_slot(field_slot, slots, |actual| match actual {
        ScalarSlotValueRef::Value(ScalarValueRef::Text(text)) => text.is_empty(),
        ScalarSlotValueRef::Value(ScalarValueRef::Blob(bytes)) => bytes.is_empty(),
        ScalarSlotValueRef::Null | ScalarSlotValueRef::Value(_) => false,
    })
}

// Evaluate one scalar `IS NOT EMPTY` node directly through the scalar slot seam.
fn eval_scalar_is_not_empty(
    field_slot: usize,
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, crate::error::InternalError> {
    eval_scalar_is_empty(field_slot, slots).map(|empty| !empty)
}

// Evaluate one scalar text-contains node directly through the scalar slot seam.
fn eval_scalar_text_contains(
    field_slot: usize,
    needle: &str,
    mode: TextMode,
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, crate::error::InternalError> {
    eval_required_scalar_slot(field_slot, slots, |actual| match actual {
        ScalarSlotValueRef::Value(ScalarValueRef::Text(actual)) => {
            text_contains_scalar(actual, needle, mode)
        }
        ScalarSlotValueRef::Null | ScalarSlotValueRef::Value(_) => false,
    })
}

// Evaluate one executable comparison against one slot reader under the scalar fast path.
fn eval_scalar_executable_compare_predicate(
    cmp: &ExecutableComparePredicate,
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, crate::error::InternalError> {
    Ok(
        eval_scalar_compare_operands_fast_path(cmp, slots)?.unwrap_or_else(|| {
            debug_assert!(
                false,
                "scalar executable predicate path admitted unsupported compare node: op={:?} coercion={:?} left={:?} right={:?}",
                cmp.op,
                cmp.coercion,
                cmp.left,
                cmp.right,
            );
            false
        }),
    )
}

// Evaluate one executable predicate against one structural slot reader.
fn eval_with_structural_slots(
    predicate: &ExecutablePredicate,
    slots: &dyn CanonicalSlotReader,
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
        ExecutablePredicate::IsMissing { field_slot } => Ok(field_slot.is_none()),
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
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, crate::error::InternalError> {
    if scalar_compare_operands_supported_for_fast_path(cmp, slots)
        && let Some(result) = eval_scalar_compare_operands_fast_path(cmp, slots)?
    {
        return Ok(result);
    }

    with_compare_operands_structural(cmp, slots, |left, right| {
        eval_compare_values(left, cmp.op, right, &cmp.coercion)
    })
    .map(|result| result.unwrap_or(false))
}

// Share scalar compare dispatch across field-vs-literal and field-vs-field
// operand pairs once capability classification has already admitted the node.
fn eval_scalar_compare_operands_fast_path(
    cmp: &ExecutableComparePredicate,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<bool>, crate::error::InternalError> {
    match (
        cmp.left_field_slot(),
        cmp.right_literal(),
        cmp.right_field_slot(),
    ) {
        (Some(field_slot), Some(value), None) => {
            eval_scalar_compare_fast_path(field_slot, cmp.op, value, &cmp.coercion, slots)
        }
        (Some(left_field_slot), None, Some(right_field_slot)) => {
            eval_scalar_compare_slot_pair_fast_path(
                left_field_slot,
                cmp.op,
                right_field_slot,
                &cmp.coercion,
                slots,
            )
        }
        _ => Ok(None),
    }
}

// Admit scalar compare operand fast paths only when all referenced field slots
// are backed by scalar leaf codecs.
fn scalar_compare_operands_supported_for_fast_path(
    cmp: &ExecutableComparePredicate,
    slots: &dyn CanonicalSlotReader,
) -> bool {
    match (
        cmp.left_field_slot(),
        cmp.right_literal(),
        cmp.right_field_slot(),
    ) {
        (Some(field_slot), Some(_), None) => {
            compare_scalar_slot_fast_path_supported(slots, field_slot)
        }
        (Some(left_field_slot), None, Some(right_field_slot)) => {
            scalar_slot_fast_path_supported(slots, left_field_slot)
                && scalar_slot_fast_path_supported(slots, right_field_slot)
        }
        _ => false,
    }
}

// Reuse the scalar-leaf boundary check for compare fast paths.
fn scalar_slot_fast_path_supported(slots: &dyn CanonicalSlotReader, field_slot: usize) -> bool {
    slots
        .field_leaf_codec(field_slot)
        .ok()
        .is_some_and(|codec| matches!(codec, LeafCodec::Scalar(_)))
}

// Field-vs-literal compares can also use borrowed value-storage scalar views
// when the declared field kind is one of the primitive families currently
// admitted by `ValueStorageView`.
fn compare_scalar_slot_fast_path_supported(
    slots: &dyn CanonicalSlotReader,
    field_slot: usize,
) -> bool {
    scalar_slot_fast_path_supported(slots, field_slot)
        || slots
            .required_value_storage_scalar(field_slot)
            .ok()
            .flatten()
            .is_some()
}

// Share the scalar-slot read plus direct compare dispatch across the scalar-only
// executor lane and the structural fallback path.
fn eval_scalar_compare_fast_path(
    field_slot: usize,
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<bool>, crate::error::InternalError> {
    let Some(actual) = required_compare_scalar_slot(field_slot, slots)? else {
        return Ok(None);
    };

    Ok(eval_compare_scalar_slot(actual, op, value, coercion))
}

// Resolve the scalar view used by field-vs-literal compare fast paths. Scalar
// leaf fields keep using their compact scalar slot codec, while value-storage
// scalar fields may expose a borrowed scalar view without materializing `Value`.
fn required_compare_scalar_slot(
    field_slot: usize,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<ScalarSlotValueRef<'_>>, crate::error::InternalError> {
    let Ok(leaf_codec) = slots.field_leaf_codec(field_slot) else {
        return Ok(None);
    };

    if matches!(leaf_codec, LeafCodec::Scalar(_)) {
        return slots.required_scalar(field_slot).map(Some);
    }

    slots.required_value_storage_scalar(field_slot)
}

// Read two scalar slots once and route the resolved slot values through the
// shared compare semantics layer without dropping to structural value loading.
fn eval_scalar_compare_slot_pair_fast_path(
    left_field_slot: usize,
    op: CompareOp,
    right_field_slot: usize,
    coercion: &CoercionSpec,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<bool>, crate::error::InternalError> {
    let left = slots.required_scalar(left_field_slot)?;
    let right = slots.required_scalar(right_field_slot)?;

    Ok(Some(eval_compare_scalar_slot_pair(
        left, op, right, coercion,
    )))
}

// Keep two-slot scalar fast-path execution on the same compare semantics layer
// by translating slot refs into canonical values only after both scalar reads
// have already been resolved.
fn eval_compare_scalar_slot_pair(
    left: ScalarSlotValueRef<'_>,
    op: CompareOp,
    right: ScalarSlotValueRef<'_>,
    coercion: &CoercionSpec,
) -> bool {
    eval_compare_values(
        &scalar_slot_value_ref_into_value(left),
        op,
        &scalar_slot_value_ref_into_value(right),
        coercion,
    )
}

// Convert one scalar slot ref into the canonical compare-value shape after the
// fast path has already avoided structural slot loading.
fn scalar_slot_value_ref_into_value(value: ScalarSlotValueRef<'_>) -> Value {
    match value {
        ScalarSlotValueRef::Null => Value::Null,
        ScalarSlotValueRef::Value(value) => value.into_value(),
    }
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

// Resolve one structural field slot and route unary predicate evaluation onto
// either the scalar fast path or the generic value-by-contract path.
fn eval_structural_field_slot(
    field_slot: Option<usize>,
    slots: &dyn CanonicalSlotReader,
    eval_scalar: impl FnOnce(
        usize,
        &dyn CanonicalSlotReader,
    ) -> Result<bool, crate::error::InternalError>,
    eval_value: impl FnOnce(
        usize,
        &dyn CanonicalSlotReader,
    ) -> Result<bool, crate::error::InternalError>,
) -> Result<bool, crate::error::InternalError> {
    let Some(field_slot) = field_slot else {
        return Ok(false);
    };
    let Ok(leaf_codec) = slots.field_leaf_codec(field_slot) else {
        return Ok(false);
    };

    if matches!(leaf_codec, LeafCodec::Scalar(_)) {
        return eval_scalar(field_slot, slots);
    }

    eval_value(field_slot, slots)
}

// Evaluate `IS NULL` through the structural slot seam.
fn eval_is_null_with_structural_slots(
    field_slot: Option<usize>,
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, crate::error::InternalError> {
    eval_structural_field_slot(
        field_slot,
        slots,
        |field_slot, slots| {
            eval_required_scalar_slot(field_slot, slots, |actual| {
                matches!(actual, ScalarSlotValueRef::Null)
            })
        },
        |field_slot, slots| {
            eval_required_value_slot(field_slot, slots, |actual| matches!(actual, Value::Null))
        },
    )
}

// Evaluate `IS NOT NULL` through the structural slot seam.
fn eval_is_not_null_with_structural_slots(
    field_slot: Option<usize>,
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, crate::error::InternalError> {
    eval_structural_field_slot(
        field_slot,
        slots,
        |field_slot, slots| {
            eval_required_scalar_slot(field_slot, slots, |actual| {
                matches!(actual, ScalarSlotValueRef::Value(_))
            })
        },
        |field_slot, slots| {
            eval_required_value_slot(field_slot, slots, |actual| !matches!(actual, Value::Null))
        },
    )
}

// Evaluate `IS EMPTY` through the structural slot seam.
fn eval_is_empty_with_structural_slots(
    field_slot: Option<usize>,
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, crate::error::InternalError> {
    eval_structural_field_slot(
        field_slot,
        slots,
        eval_scalar_is_empty,
        |field_slot, slots| eval_required_value_slot(field_slot, slots, is_empty_value),
    )
}

// Evaluate `IS NOT EMPTY` through the structural slot seam.
fn eval_is_not_empty_with_structural_slots(
    field_slot: Option<usize>,
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(field_slot) = field_slot else {
        return Ok(false);
    };

    eval_is_empty_with_structural_slots(Some(field_slot), slots).map(|empty| !empty)
}

// Evaluate `TEXT CONTAINS` through the structural slot seam.
fn eval_text_contains_with_structural_slots(
    field_slot: Option<usize>,
    value: &Value,
    mode: TextMode,
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, crate::error::InternalError> {
    eval_structural_field_slot(
        field_slot,
        slots,
        |field_slot, slots| {
            let Value::Text(needle) = value else {
                return Ok(false);
            };

            eval_scalar_text_contains(field_slot, needle, mode, slots)
        },
        |field_slot, slots| {
            eval_required_value_slot(field_slot, slots, |actual| {
                actual.text_contains(value, mode).unwrap_or(false)
            })
        },
    )
}
