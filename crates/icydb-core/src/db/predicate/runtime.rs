//! Module: predicate::runtime
//! Responsibility: compile/evaluate slot-resolved predicates against entities.
//! Does not own: schema validation or normalization policy.
//! Boundary: executor row filtering uses this runtime program.

use crate::{
    db::{
        data::{ScalarSlotValueRef, ScalarValueRef, SlotReader, decode_slot_value_by_contract},
        predicate::{
            CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate,
            PredicateExecutionModel, ResolvedComparePredicate, ResolvedPredicate, TextOp,
            compare_eq, compare_order, compare_text,
        },
        scalar_expr::{
            ScalarValueProgram, compile_scalar_field_program, eval_scalar_value_program,
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
/// Slot-resolved predicate program for runtime row filtering.
/// Field names are resolved once during setup; evaluation is slot-only.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct PredicateProgram {
    resolved: ResolvedPredicate,
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
/// Marker for the resolved-predicate generic executor path.
/// The resolved tree remains the source of truth for generic evaluation and
/// for downstream consumers that still inspect `ResolvedPredicate`.
///

#[derive(Clone, Copy, Debug, Default)]
struct GenericPredicateProgram;

///
/// ScalarPredicateProgram
///
/// Scalar-only predicate tree compiled for slot-reader execution without
/// `Value` fallback.
///

#[derive(Clone, Debug)]
struct ScalarPredicateProgram {
    resolved: ScalarResolvedPredicate,
}

///
/// ScalarResolvedComparePredicate
///
/// One scalar-only comparison node with a guaranteed concrete field slot.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScalarResolvedComparePredicate {
    expr: ScalarValueProgram,
    op: CompareOp,
    value: ScalarCompareLiteral,
    coercion: CoercionSpec,
}

///
/// ScalarResolvedPredicate
///
/// Scalar-only predicate AST compiled for direct slot evaluation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum ScalarResolvedPredicate {
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Compare(ScalarResolvedComparePredicate),
    IsNull { field_slot: usize },
    IsNotNull { field_slot: usize },
    IsMissing { field_slot: Option<usize> },
    IsEmpty { field_slot: usize },
    IsNotEmpty { field_slot: usize },
    TextContains { field_slot: usize, needle: String },
    TextContainsCi { field_slot: usize, needle: String },
}

///
/// ScalarCompareLiteral
///
/// ScalarCompareLiteral is the owned literal payload for one compiled scalar
/// predicate node.
/// Scalar predicate execution reads only scalar slots and these precompiled
/// owned literals; it never consults `Value` on the hot path.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum ScalarCompareLiteral {
    One(ScalarLiteral),
    Many(Vec<ScalarLiteral>),
}

///
/// ScalarLiteral
///
/// ScalarLiteral is the owned scalar literal form admitted into compiled scalar
/// predicates.
/// Its variants intentionally mirror the scalar slot seam one-for-one so
/// runtime compare logic stays direct and allocation-free beyond compile time.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum ScalarLiteral {
    Null,
    Blob(Vec<u8>),
    Bool(bool),
    Date(crate::types::Date),
    Duration(crate::types::Duration),
    Float32(crate::types::Float32),
    Float64(crate::types::Float64),
    Int(i64),
    Principal(crate::types::Principal),
    Subaccount(crate::types::Subaccount),
    Text(String),
    Timestamp(crate::types::Timestamp),
    Uint(u64),
    Ulid(crate::types::Ulid),
    Unit,
}

impl PredicateProgram {
    /// Compile a predicate into a slot-based executable form using structural model data only.
    #[must_use]
    pub(in crate::db) fn compile_with_model(
        model: &'static EntityModel,
        predicate: &PredicateExecutionModel,
    ) -> Self {
        let resolved = compile_predicate_program(model, predicate);
        let compiled = compile_scalar_predicate_program(model, predicate).map_or(
            CompiledPredicate::Generic(GenericPredicateProgram),
            CompiledPredicate::Scalar,
        );

        Self { resolved, compiled }
    }

    /// Evaluate one precompiled predicate program against one slot-reader callback.
    #[must_use]
    pub(in crate::db) fn eval_with_slot_reader(
        &self,
        read_slot: &mut dyn FnMut(usize) -> Option<Value>,
    ) -> bool {
        eval_with_resolved_slots(&self.resolved, read_slot)
    }

    /// Evaluate one precompiled predicate program against one structural slot reader.
    pub(in crate::db) fn eval_with_structural_slot_reader(
        &self,
        slots: &mut dyn SlotReader,
    ) -> Result<bool, crate::error::InternalError> {
        match &self.compiled {
            CompiledPredicate::Scalar(program) => eval_scalar_predicate_program(program, slots),
            CompiledPredicate::Generic(_) => eval_with_structural_slots(&self.resolved, slots),
        }
    }

    /// Borrow the resolved predicate tree used by runtime evaluators.
    #[must_use]
    pub(in crate::db) const fn resolved(&self) -> &ResolvedPredicate {
        &self.resolved
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) const fn uses_scalar_program(&self) -> bool {
        matches!(self.compiled, CompiledPredicate::Scalar(_))
    }

    #[cfg(test)]
    #[must_use]
    fn compiled_scalar(&self) -> &ScalarPredicateProgram {
        match &self.compiled {
            CompiledPredicate::Scalar(program) => program,
            CompiledPredicate::Generic(_) => panic!("expected scalar-compiled predicate program"),
        }
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
) -> ResolvedPredicate {
    fn resolve_field(model: &'static EntityModel, field_name: &str) -> Option<usize> {
        resolve_field_slot(model, field_name)
    }

    // Compile field-name predicates into slot-index predicates once per query.
    match predicate {
        Predicate::True => ResolvedPredicate::True,
        Predicate::False => ResolvedPredicate::False,
        Predicate::And(children) => ResolvedPredicate::And(
            children
                .iter()
                .map(|child| compile_predicate_program(model, child))
                .collect::<Vec<_>>(),
        ),
        Predicate::Or(children) => ResolvedPredicate::Or(
            children
                .iter()
                .map(|child| compile_predicate_program(model, child))
                .collect::<Vec<_>>(),
        ),
        Predicate::Not(inner) => {
            ResolvedPredicate::Not(Box::new(compile_predicate_program(model, inner)))
        }
        Predicate::Compare(ComparePredicate {
            field,
            op,
            value,
            coercion,
        }) => ResolvedPredicate::Compare(ResolvedComparePredicate {
            field_slot: resolve_field(model, field),
            op: *op,
            value: value.clone(),
            coercion: coercion.clone(),
        }),
        Predicate::IsNull { field } => ResolvedPredicate::IsNull {
            field_slot: resolve_field(model, field),
        },
        Predicate::IsNotNull { field } => ResolvedPredicate::IsNotNull {
            field_slot: resolve_field(model, field),
        },
        Predicate::IsMissing { field } => ResolvedPredicate::IsMissing {
            field_slot: resolve_field(model, field),
        },
        Predicate::IsEmpty { field } => ResolvedPredicate::IsEmpty {
            field_slot: resolve_field(model, field),
        },
        Predicate::IsNotEmpty { field } => ResolvedPredicate::IsNotEmpty {
            field_slot: resolve_field(model, field),
        },
        Predicate::TextContains { field, value } => ResolvedPredicate::TextContains {
            field_slot: resolve_field(model, field),
            value: value.clone(),
        },
        Predicate::TextContainsCi { field, value } => ResolvedPredicate::TextContainsCi {
            field_slot: resolve_field(model, field),
            value: value.clone(),
        },
    }
}

// Compile one predicate into the scalar-only execution form when every node
// stays within the scalar comparison contract.
fn compile_scalar_predicate_program(
    model: &'static EntityModel,
    predicate: &PredicateExecutionModel,
) -> Option<ScalarPredicateProgram> {
    compile_scalar_predicate_node(model, predicate)
        .map(|resolved| ScalarPredicateProgram { resolved })
}

// Compile one predicate subtree into the scalar-only execution form.
fn compile_scalar_predicate_node(
    model: &'static EntityModel,
    predicate: &PredicateExecutionModel,
) -> Option<ScalarResolvedPredicate> {
    match predicate {
        Predicate::True => Some(ScalarResolvedPredicate::True),
        Predicate::False => Some(ScalarResolvedPredicate::False),
        Predicate::And(children) => children
            .iter()
            .map(|child| compile_scalar_predicate_node(model, child))
            .collect::<Option<Vec<_>>>()
            .map(ScalarResolvedPredicate::And),
        Predicate::Or(children) => children
            .iter()
            .map(|child| compile_scalar_predicate_node(model, child))
            .collect::<Option<Vec<_>>>()
            .map(ScalarResolvedPredicate::Or),
        Predicate::Not(inner) => compile_scalar_predicate_node(model, inner)
            .map(|inner| ScalarResolvedPredicate::Not(Box::new(inner))),
        Predicate::Compare(ComparePredicate {
            field,
            op,
            value,
            coercion,
        }) => compile_scalar_compare_predicate(model, field, *op, value, coercion)
            .map(ScalarResolvedPredicate::Compare),
        Predicate::IsNull { field } => compile_scalar_field_slot(model, field)
            .map(|field_slot| ScalarResolvedPredicate::IsNull { field_slot }),
        Predicate::IsNotNull { field } => compile_scalar_field_slot(model, field)
            .map(|field_slot| ScalarResolvedPredicate::IsNotNull { field_slot }),
        Predicate::IsMissing { field } => Some(ScalarResolvedPredicate::IsMissing {
            field_slot: resolve_field_slot(model, field),
        }),
        Predicate::IsEmpty { field } => compile_scalar_field_slot(model, field)
            .map(|field_slot| ScalarResolvedPredicate::IsEmpty { field_slot }),
        Predicate::IsNotEmpty { field } => compile_scalar_field_slot(model, field)
            .map(|field_slot| ScalarResolvedPredicate::IsNotEmpty { field_slot }),
        Predicate::TextContains { field, value } => {
            compile_scalar_text_contains_predicate(model, field, value, false)
        }
        Predicate::TextContainsCi { field, value } => {
            compile_scalar_text_contains_predicate(model, field, value, true)
        }
    }
}

// Compile one scalar-only comparison node with a guaranteed concrete field slot.
fn compile_scalar_compare_predicate(
    model: &'static EntityModel,
    field: &str,
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
) -> Option<ScalarResolvedComparePredicate> {
    if !scalar_compare_op_supported(op) || !scalar_compare_coercion_supported(coercion) {
        return None;
    }

    Some(ScalarResolvedComparePredicate {
        expr: compile_scalar_field_program(model, field)?,
        op,
        value: compile_scalar_compare_literal(op, value)?,
        coercion: coercion.clone(),
    })
}

// Resolve one field to a concrete scalar slot for scalar-only predicate nodes.
fn compile_scalar_field_slot(model: &'static EntityModel, field: &str) -> Option<usize> {
    let field_slot = resolve_field_slot(model, field)?;
    let field_model = model.fields().get(field_slot)?;
    if !matches!(field_model.leaf_codec(), LeafCodec::Scalar(_)) {
        return None;
    }

    Some(field_slot)
}

// Compile one scalar text-contains predicate when the source field stays on
// the scalar text seam.
fn compile_scalar_text_contains_predicate(
    model: &'static EntityModel,
    field: &str,
    value: &Value,
    casefold: bool,
) -> Option<ScalarResolvedPredicate> {
    let field_slot = compile_scalar_field_slot(model, field)?;
    let Value::Text(needle) = value else {
        return None;
    };

    Some(if casefold {
        ScalarResolvedPredicate::TextContainsCi {
            field_slot,
            needle: needle.clone(),
        }
    } else {
        ScalarResolvedPredicate::TextContains {
            field_slot,
            needle: needle.clone(),
        }
    })
}

const fn scalar_compare_op_supported(op: CompareOp) -> bool {
    matches!(
        op,
        CompareOp::Eq
            | CompareOp::Ne
            | CompareOp::Lt
            | CompareOp::Lte
            | CompareOp::Gt
            | CompareOp::Gte
            | CompareOp::In
            | CompareOp::NotIn
            | CompareOp::StartsWith
            | CompareOp::EndsWith
    )
}

const fn scalar_compare_coercion_supported(coercion: &CoercionSpec) -> bool {
    !matches!(coercion.id, CoercionId::NumericWiden)
}

// Compile one predicate literal into the owned scalar form used by scalar-only
// predicate programs.
fn compile_scalar_compare_literal(op: CompareOp, value: &Value) -> Option<ScalarCompareLiteral> {
    match op {
        CompareOp::In | CompareOp::NotIn => match value {
            Value::List(items) => items
                .iter()
                .map(compile_scalar_literal)
                .collect::<Option<Vec<_>>>()
                .map(ScalarCompareLiteral::Many),
            _ => None,
        },
        _ => compile_scalar_literal(value).map(ScalarCompareLiteral::One),
    }
}

// Compile one value literal admitted by the scalar slot seam into its owned
// scalar counterpart.
fn compile_scalar_literal(value: &Value) -> Option<ScalarLiteral> {
    match value {
        Value::Null => Some(ScalarLiteral::Null),
        Value::Blob(value) => Some(ScalarLiteral::Blob(value.clone())),
        Value::Bool(value) => Some(ScalarLiteral::Bool(*value)),
        Value::Date(value) => Some(ScalarLiteral::Date(*value)),
        Value::Duration(value) => Some(ScalarLiteral::Duration(*value)),
        Value::Float32(value) => Some(ScalarLiteral::Float32(*value)),
        Value::Float64(value) => Some(ScalarLiteral::Float64(*value)),
        Value::Int(value) => Some(ScalarLiteral::Int(*value)),
        Value::Principal(value) => Some(ScalarLiteral::Principal(*value)),
        Value::Subaccount(value) => Some(ScalarLiteral::Subaccount(*value)),
        Value::Text(value) => Some(ScalarLiteral::Text(value.clone())),
        Value::Timestamp(value) => Some(ScalarLiteral::Timestamp(*value)),
        Value::Uint(value) => Some(ScalarLiteral::Uint(*value)),
        Value::Ulid(value) => Some(ScalarLiteral::Ulid(*value)),
        Value::Unit => Some(ScalarLiteral::Unit),
        Value::Account(_)
        | Value::Decimal(_)
        | Value::Enum(_)
        | Value::Int128(_)
        | Value::IntBig(_)
        | Value::List(_)
        | Value::Map(_)
        | Value::Uint128(_)
        | Value::UintBig(_) => None,
    }
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

/// Evaluate one slot-resolved predicate against one runtime slot reader.
fn eval_with_resolved_slots(
    predicate: &ResolvedPredicate,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> bool {
    // Evaluate recursively against slot-resolved predicates.
    match predicate {
        ResolvedPredicate::True => true,
        ResolvedPredicate::False => false,
        ResolvedPredicate::And(children) => {
            for child in children {
                if !eval_with_resolved_slots(child, read_slot) {
                    return false;
                }
            }

            true
        }
        ResolvedPredicate::Or(children) => {
            for child in children {
                if eval_with_resolved_slots(child, read_slot) {
                    return true;
                }
            }

            false
        }
        ResolvedPredicate::Not(inner) => !eval_with_resolved_slots(inner, read_slot),
        ResolvedPredicate::Compare(cmp) => eval_compare_with_resolved_slots(cmp, read_slot),
        ResolvedPredicate::IsNull { field_slot } => {
            matches!(
                field_from_slot(*field_slot, read_slot),
                FieldPresence::Present(Value::Null)
            )
        }
        ResolvedPredicate::IsNotNull { field_slot } => {
            matches!(field_from_slot(*field_slot, read_slot), FieldPresence::Present(value) if !matches!(value, Value::Null))
        }
        ResolvedPredicate::IsMissing { field_slot } => {
            matches!(
                field_from_slot(*field_slot, read_slot),
                FieldPresence::Missing
            )
        }
        ResolvedPredicate::IsEmpty { field_slot } => {
            on_present_slot(*field_slot, read_slot, is_empty_value)
        }
        ResolvedPredicate::IsNotEmpty { field_slot } => {
            on_present_slot(*field_slot, read_slot, |value| !is_empty_value(value))
        }
        ResolvedPredicate::TextContains { field_slot, value } => {
            on_present_slot(*field_slot, read_slot, |actual| {
                actual.text_contains(value, TextMode::Cs).unwrap_or(false)
            })
        }
        ResolvedPredicate::TextContainsCi { field_slot, value } => {
            on_present_slot(*field_slot, read_slot, |actual| {
                actual.text_contains(value, TextMode::Ci).unwrap_or(false)
            })
        }
    }
}

/// Evaluate a slot-resolved comparison predicate against one runtime slot reader.
fn eval_compare_with_resolved_slots(
    cmp: &ResolvedComparePredicate,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> bool {
    let FieldPresence::Present(actual) = field_from_slot(cmp.field_slot, read_slot) else {
        return false;
    };

    eval_compare_values(&actual, cmp.op, &cmp.value, &cmp.coercion)
}

// Evaluate one scalar-only compiled predicate program without generic fallback.
fn eval_scalar_predicate_program(
    program: &ScalarPredicateProgram,
    slots: &dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    eval_scalar_predicate_node(&program.resolved, slots)
}

// Evaluate one scalar-only predicate tree against direct scalar slot reads.
fn eval_scalar_predicate_node(
    predicate: &ScalarResolvedPredicate,
    slots: &dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    match predicate {
        ScalarResolvedPredicate::True => Ok(true),
        ScalarResolvedPredicate::False => Ok(false),
        ScalarResolvedPredicate::And(children) => {
            for child in children {
                if !eval_scalar_predicate_node(child, slots)? {
                    return Ok(false);
                }
            }

            Ok(true)
        }
        ScalarResolvedPredicate::Or(children) => {
            for child in children {
                if eval_scalar_predicate_node(child, slots)? {
                    return Ok(true);
                }
            }

            Ok(false)
        }
        ScalarResolvedPredicate::Not(inner) => Ok(!eval_scalar_predicate_node(inner, slots)?),
        ScalarResolvedPredicate::Compare(cmp) => eval_scalar_compare_predicate(cmp, slots),
        ScalarResolvedPredicate::IsNull { field_slot } => Ok(matches!(
            slots.get_scalar(*field_slot)?,
            Some(ScalarSlotValueRef::Null)
        )),
        ScalarResolvedPredicate::IsNotNull { field_slot } => Ok(matches!(
            slots.get_scalar(*field_slot)?,
            Some(ScalarSlotValueRef::Value(_))
        )),
        ScalarResolvedPredicate::IsMissing { field_slot } => {
            Ok(field_slot.is_none_or(|field_slot| !slots.has(field_slot)))
        }
        ScalarResolvedPredicate::IsEmpty { field_slot } => eval_scalar_is_empty(*field_slot, slots),
        ScalarResolvedPredicate::IsNotEmpty { field_slot } => {
            eval_scalar_is_not_empty(*field_slot, slots)
        }
        ScalarResolvedPredicate::TextContains { field_slot, needle } => {
            eval_scalar_text_contains(*field_slot, needle, TextMode::Cs, slots)
        }
        ScalarResolvedPredicate::TextContainsCi { field_slot, needle } => {
            eval_scalar_text_contains(*field_slot, needle, TextMode::Ci, slots)
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

// Evaluate one scalar-only comparison against one slot reader.
fn eval_scalar_compare_predicate(
    cmp: &ScalarResolvedComparePredicate,
    slots: &dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(actual) = eval_scalar_value_program(&cmp.expr, slots)? else {
        return Ok(false);
    };

    Ok(eval_compare_scalar_literal_slot(
        actual.as_slot_value_ref(),
        cmp.op,
        &cmp.value,
        &cmp.coercion,
    )
    .unwrap_or_else(|| {
        debug_assert!(
            false,
            "scalar predicate compile admitted unsupported compare node: op={:?} coercion={:?} value={:?}",
            cmp.op,
            cmp.coercion,
            cmp.value,
        );
        false
    }))
}

// Evaluate one slot-resolved predicate against one structural slot reader.
fn eval_with_structural_slots(
    predicate: &ResolvedPredicate,
    slots: &mut dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    match predicate {
        ResolvedPredicate::True => Ok(true),
        ResolvedPredicate::False => Ok(false),
        ResolvedPredicate::And(children) => {
            for child in children {
                if !eval_with_structural_slots(child, slots)? {
                    return Ok(false);
                }
            }

            Ok(true)
        }
        ResolvedPredicate::Or(children) => {
            for child in children {
                if eval_with_structural_slots(child, slots)? {
                    return Ok(true);
                }
            }

            Ok(false)
        }
        ResolvedPredicate::Not(inner) => Ok(!eval_with_structural_slots(inner, slots)?),
        ResolvedPredicate::Compare(cmp) => eval_compare_with_structural_slots(cmp, slots),
        ResolvedPredicate::IsNull { field_slot } => {
            eval_is_null_with_structural_slots(*field_slot, slots)
        }
        ResolvedPredicate::IsNotNull { field_slot } => {
            eval_is_not_null_with_structural_slots(*field_slot, slots)
        }
        ResolvedPredicate::IsMissing { field_slot } => {
            Ok(field_slot.is_none_or(|slot| !slots.has(slot)))
        }
        ResolvedPredicate::IsEmpty { field_slot } => {
            eval_is_empty_with_structural_slots(*field_slot, slots)
        }
        ResolvedPredicate::IsNotEmpty { field_slot } => {
            eval_is_not_empty_with_structural_slots(*field_slot, slots)
        }
        ResolvedPredicate::TextContains { field_slot, value } => {
            eval_text_contains_with_structural_slots(*field_slot, value, TextMode::Cs, slots)
        }
        ResolvedPredicate::TextContainsCi { field_slot, value } => {
            eval_text_contains_with_structural_slots(*field_slot, value, TextMode::Ci, slots)
        }
    }
}

// Evaluate one comparison predicate through the structural slot seam.
fn eval_compare_with_structural_slots(
    cmp: &ResolvedComparePredicate,
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

// Evaluate one compare op directly against one scalar slot value and one
// precompiled scalar literal.
fn eval_compare_scalar_literal_slot(
    actual: ScalarSlotValueRef<'_>,
    op: CompareOp,
    value: &ScalarCompareLiteral,
    coercion: &CoercionSpec,
) -> Option<bool> {
    match actual {
        ScalarSlotValueRef::Null => eval_null_scalar_literal_compare(op, value, coercion),
        ScalarSlotValueRef::Value(actual) => {
            eval_compare_scalar_literal_value(actual, op, value, coercion)
        }
    }
}

// Evaluate one compare op directly against one non-null scalar slot value and
// one precompiled scalar literal.
fn eval_compare_scalar_literal_value(
    actual: ScalarValueRef<'_>,
    op: CompareOp,
    value: &ScalarCompareLiteral,
    coercion: &CoercionSpec,
) -> Option<bool> {
    match actual {
        ScalarValueRef::Text(actual) => {
            eval_text_scalar_literal_compare(actual, op, value, coercion)
        }
        ScalarValueRef::Blob(actual) => {
            eval_blob_scalar_literal_compare(actual, op, value, coercion)
        }
        ScalarValueRef::Bool(actual) => eval_direct_scalar_literal_compare(
            actual,
            op,
            value,
            coercion,
            scalar_bool_from_literal,
        ),
        ScalarValueRef::Date(actual) => eval_direct_scalar_literal_compare(
            actual,
            op,
            value,
            coercion,
            scalar_date_from_literal,
        ),
        ScalarValueRef::Duration(actual) => eval_direct_scalar_literal_compare(
            actual,
            op,
            value,
            coercion,
            scalar_duration_from_literal,
        ),
        ScalarValueRef::Float32(actual) => eval_direct_scalar_literal_compare(
            actual,
            op,
            value,
            coercion,
            scalar_float32_from_literal,
        ),
        ScalarValueRef::Float64(actual) => eval_direct_scalar_literal_compare(
            actual,
            op,
            value,
            coercion,
            scalar_float64_from_literal,
        ),
        ScalarValueRef::Int(actual) => {
            eval_direct_scalar_literal_compare(actual, op, value, coercion, scalar_int_from_literal)
        }
        ScalarValueRef::Principal(actual) => eval_direct_scalar_literal_compare(
            actual,
            op,
            value,
            coercion,
            scalar_principal_from_literal,
        ),
        ScalarValueRef::Subaccount(actual) => eval_direct_scalar_literal_compare(
            actual,
            op,
            value,
            coercion,
            scalar_subaccount_from_literal,
        ),
        ScalarValueRef::Timestamp(actual) => eval_direct_scalar_literal_compare(
            actual,
            op,
            value,
            coercion,
            scalar_timestamp_from_literal,
        ),
        ScalarValueRef::Uint(actual) => eval_direct_scalar_literal_compare(
            actual,
            op,
            value,
            coercion,
            scalar_uint_from_literal,
        ),
        ScalarValueRef::Ulid(actual) => eval_direct_scalar_literal_compare(
            actual,
            op,
            value,
            coercion,
            scalar_ulid_from_literal,
        ),
        ScalarValueRef::Unit => {
            eval_direct_scalar_literal_compare((), op, value, coercion, scalar_unit_from_literal)
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
            CompareOp::Eq => Some(decode(value).is_some_and(|expected| actual == expected)),
            CompareOp::Ne => Some(decode(value).is_some_and(|expected| actual != expected)),
            CompareOp::Lt => Some(decode(value).is_some_and(|expected| actual < expected)),
            CompareOp::Lte => Some(decode(value).is_some_and(|expected| actual <= expected)),
            CompareOp::Gt => Some(decode(value).is_some_and(|expected| actual > expected)),
            CompareOp::Gte => Some(decode(value).is_some_and(|expected| actual >= expected)),
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

// Evaluate one strict scalar compare directly against compiled scalar literals,
// leaving only unsupported coercions on the generic fallback.
fn eval_direct_scalar_literal_compare<T>(
    actual: T,
    op: CompareOp,
    value: &ScalarCompareLiteral,
    coercion: &CoercionSpec,
    decode: impl Fn(&ScalarLiteral) -> Option<T>,
) -> Option<bool>
where
    T: Copy + Eq + Ord,
{
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => match op {
            CompareOp::Eq => Some(
                single_scalar_literal(value, &decode).is_some_and(|expected| actual == expected),
            ),
            CompareOp::Ne => Some(
                single_scalar_literal(value, &decode).is_some_and(|expected| actual != expected),
            ),
            CompareOp::Lt => Some(
                single_scalar_literal(value, &decode).is_some_and(|expected| actual < expected),
            ),
            CompareOp::Lte => Some(
                single_scalar_literal(value, &decode).is_some_and(|expected| actual <= expected),
            ),
            CompareOp::Gt => Some(
                single_scalar_literal(value, &decode).is_some_and(|expected| actual > expected),
            ),
            CompareOp::Gte => Some(
                single_scalar_literal(value, &decode).is_some_and(|expected| actual >= expected),
            ),
            CompareOp::In => Some(scalar_literal_in_list(actual, value, decode).unwrap_or(false)),
            CompareOp::NotIn => {
                Some(scalar_literal_in_list(actual, value, decode).is_some_and(|matched| !matched))
            }
            CompareOp::Contains | CompareOp::StartsWith | CompareOp::EndsWith => Some(false),
        },
        CoercionId::TextCasefold => Some(false),
        CoercionId::NumericWiden => None,
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

// Evaluate direct blob equality/list membership against compiled scalar literals.
fn eval_blob_scalar_literal_compare(
    actual: &[u8],
    op: CompareOp,
    value: &ScalarCompareLiteral,
    coercion: &CoercionSpec,
) -> Option<bool> {
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => match op {
            CompareOp::Eq => Some(matches!(
                value,
                ScalarCompareLiteral::One(ScalarLiteral::Blob(expected)) if actual == expected.as_slice()
            )),
            CompareOp::Ne => Some(matches!(
                value,
                ScalarCompareLiteral::One(ScalarLiteral::Blob(expected)) if actual != expected.as_slice()
            )),
            CompareOp::In => Some(blob_literal_in_list(actual, value).unwrap_or(false)),
            CompareOp::NotIn => {
                Some(blob_literal_in_list(actual, value).is_some_and(|matched| !matched))
            }
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

// Evaluate direct null comparisons against compiled scalar literals.
fn eval_null_scalar_literal_compare(
    op: CompareOp,
    value: &ScalarCompareLiteral,
    coercion: &CoercionSpec,
) -> Option<bool> {
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => match op {
            CompareOp::Eq => Some(matches!(
                value,
                ScalarCompareLiteral::One(ScalarLiteral::Null)
            )),
            CompareOp::Ne
            | CompareOp::Lt
            | CompareOp::Lte
            | CompareOp::Gt
            | CompareOp::Gte
            | CompareOp::Contains
            | CompareOp::StartsWith
            | CompareOp::EndsWith => Some(false),
            CompareOp::In => Some(null_literal_in_list(value).unwrap_or(false)),
            CompareOp::NotIn => Some(null_literal_in_list(value).is_some_and(|matched| !matched)),
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
        CompareOp::Eq => Some(
            matches!(value, Value::Text(expected) if compare_scalar_text(actual, expected, mode) == Ordering::Equal),
        ),
        CompareOp::Ne => Some(
            matches!(value, Value::Text(expected) if compare_scalar_text(actual, expected, mode) != Ordering::Equal),
        ),
        CompareOp::Lt => Some(
            matches!(value, Value::Text(expected) if compare_scalar_text(actual, expected, mode).is_lt()),
        ),
        CompareOp::Lte => Some(
            matches!(value, Value::Text(expected) if compare_scalar_text(actual, expected, mode).is_le()),
        ),
        CompareOp::Gt => Some(
            matches!(value, Value::Text(expected) if compare_scalar_text(actual, expected, mode).is_gt()),
        ),
        CompareOp::Gte => Some(
            matches!(value, Value::Text(expected) if compare_scalar_text(actual, expected, mode).is_ge()),
        ),
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

// Evaluate one scalar text compare against compiled scalar literals without
// allocating owned `Value::Text`.
fn eval_text_scalar_literal_compare(
    actual: &str,
    op: CompareOp,
    value: &ScalarCompareLiteral,
    coercion: &CoercionSpec,
) -> Option<bool> {
    let mode = match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => TextMode::Cs,
        CoercionId::TextCasefold => TextMode::Ci,
        CoercionId::NumericWiden => return None,
    };

    match op {
        CompareOp::Eq => Some(matches!(
            value,
            ScalarCompareLiteral::One(ScalarLiteral::Text(expected))
                if compare_scalar_text(actual, expected, mode) == Ordering::Equal
        )),
        CompareOp::Ne => Some(matches!(
            value,
            ScalarCompareLiteral::One(ScalarLiteral::Text(expected))
                if compare_scalar_text(actual, expected, mode) != Ordering::Equal
        )),
        CompareOp::Lt => Some(matches!(
            value,
            ScalarCompareLiteral::One(ScalarLiteral::Text(expected))
                if compare_scalar_text(actual, expected, mode).is_lt()
        )),
        CompareOp::Lte => Some(matches!(
            value,
            ScalarCompareLiteral::One(ScalarLiteral::Text(expected))
                if compare_scalar_text(actual, expected, mode).is_le()
        )),
        CompareOp::Gt => Some(matches!(
            value,
            ScalarCompareLiteral::One(ScalarLiteral::Text(expected))
                if compare_scalar_text(actual, expected, mode).is_gt()
        )),
        CompareOp::Gte => Some(matches!(
            value,
            ScalarCompareLiteral::One(ScalarLiteral::Text(expected))
                if compare_scalar_text(actual, expected, mode).is_ge()
        )),
        CompareOp::StartsWith => Some(matches!(
            value,
            ScalarCompareLiteral::One(ScalarLiteral::Text(expected))
                if text_starts_with_scalar(actual, expected, mode)
        )),
        CompareOp::EndsWith => Some(matches!(
            value,
            ScalarCompareLiteral::One(ScalarLiteral::Text(expected))
                if text_ends_with_scalar(actual, expected, mode)
        )),
        CompareOp::In => Some(text_literal_in_list(actual, value, mode).unwrap_or(false)),
        CompareOp::NotIn => {
            Some(text_literal_in_list(actual, value, mode).is_some_and(|matched| !matched))
        }
        CompareOp::Contains => Some(false),
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

fn scalar_literal_in_list<T>(
    actual: T,
    list: &ScalarCompareLiteral,
    decode: impl Fn(&ScalarLiteral) -> Option<T>,
) -> Option<bool>
where
    T: Copy + Eq,
{
    let ScalarCompareLiteral::Many(items) = list else {
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

fn blob_literal_in_list(actual: &[u8], list: &ScalarCompareLiteral) -> Option<bool> {
    let ScalarCompareLiteral::Many(items) = list else {
        return None;
    };

    let mut saw_valid = false;
    for item in items {
        if let ScalarLiteral::Blob(expected) = item {
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

fn null_literal_in_list(list: &ScalarCompareLiteral) -> Option<bool> {
    let ScalarCompareLiteral::Many(items) = list else {
        return None;
    };

    for item in items {
        if matches!(item, ScalarLiteral::Null) {
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

fn text_literal_in_list(actual: &str, list: &ScalarCompareLiteral, mode: TextMode) -> Option<bool> {
    let ScalarCompareLiteral::Many(items) = list else {
        return None;
    };

    let mut saw_valid = false;
    for item in items {
        if let ScalarLiteral::Text(expected) = item {
            if compare_scalar_text(actual, expected, mode) == Ordering::Equal {
                return Some(true);
            }
            saw_valid = true;
        }
    }

    saw_valid.then_some(false)
}

fn single_scalar_literal<'a, T>(
    value: &'a ScalarCompareLiteral,
    decode: &impl Fn(&'a ScalarLiteral) -> Option<T>,
) -> Option<T> {
    let ScalarCompareLiteral::One(value) = value else {
        return None;
    };

    decode(value)
}

const fn scalar_bool_from_value(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_bool_from_literal(value: &ScalarLiteral) -> Option<bool> {
    match value {
        ScalarLiteral::Bool(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_date_from_value(value: &Value) -> Option<crate::types::Date> {
    match value {
        Value::Date(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_date_from_literal(value: &ScalarLiteral) -> Option<crate::types::Date> {
    match value {
        ScalarLiteral::Date(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_duration_from_value(value: &Value) -> Option<crate::types::Duration> {
    match value {
        Value::Duration(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_duration_from_literal(value: &ScalarLiteral) -> Option<crate::types::Duration> {
    match value {
        ScalarLiteral::Duration(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_float32_from_value(value: &Value) -> Option<crate::types::Float32> {
    match value {
        Value::Float32(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_float32_from_literal(value: &ScalarLiteral) -> Option<crate::types::Float32> {
    match value {
        ScalarLiteral::Float32(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_float64_from_value(value: &Value) -> Option<crate::types::Float64> {
    match value {
        Value::Float64(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_float64_from_literal(value: &ScalarLiteral) -> Option<crate::types::Float64> {
    match value {
        ScalarLiteral::Float64(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_int_from_value(value: &Value) -> Option<i64> {
    match value {
        Value::Int(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_int_from_literal(value: &ScalarLiteral) -> Option<i64> {
    match value {
        ScalarLiteral::Int(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_principal_from_value(value: &Value) -> Option<crate::types::Principal> {
    match value {
        Value::Principal(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_principal_from_literal(value: &ScalarLiteral) -> Option<crate::types::Principal> {
    match value {
        ScalarLiteral::Principal(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_subaccount_from_value(value: &Value) -> Option<crate::types::Subaccount> {
    match value {
        Value::Subaccount(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_subaccount_from_literal(value: &ScalarLiteral) -> Option<crate::types::Subaccount> {
    match value {
        ScalarLiteral::Subaccount(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_timestamp_from_value(value: &Value) -> Option<crate::types::Timestamp> {
    match value {
        Value::Timestamp(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_timestamp_from_literal(value: &ScalarLiteral) -> Option<crate::types::Timestamp> {
    match value {
        ScalarLiteral::Timestamp(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_uint_from_value(value: &Value) -> Option<u64> {
    match value {
        Value::Uint(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_uint_from_literal(value: &ScalarLiteral) -> Option<u64> {
    match value {
        ScalarLiteral::Uint(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_ulid_from_value(value: &Value) -> Option<crate::types::Ulid> {
    match value {
        Value::Ulid(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_ulid_from_literal(value: &ScalarLiteral) -> Option<crate::types::Ulid> {
    match value {
        ScalarLiteral::Ulid(value) => Some(*value),
        _ => None,
    }
}

const fn scalar_unit_from_value(value: &Value) -> Option<()> {
    match value {
        Value::Unit => Some(()),
        _ => None,
    }
}

const fn scalar_unit_from_literal(value: &ScalarLiteral) -> Option<()> {
    match value {
        ScalarLiteral::Unit => Some(()),
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
    use super::{
        PredicateProgram, ScalarCompareLiteral, ScalarLiteral, ScalarResolvedPredicate,
        eval_compare_scalar_literal_slot, eval_compare_scalar_slot, eval_compare_values,
    };
    use crate::{
        db::{
            data::{ScalarSlotValueRef, ScalarValueRef, SlotReader},
            predicate::{CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate},
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
    fn scalar_predicate_program_compiles_owned_scalar_literals() {
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
        let ScalarResolvedPredicate::And(children) = &program.compiled_scalar().resolved else {
            panic!("expected scalar and-predicate");
        };

        let ScalarResolvedPredicate::Compare(eq) = &children[0] else {
            panic!("expected eq compare");
        };
        let ScalarResolvedPredicate::Compare(in_list) = &children[1] else {
            panic!("expected in-list compare");
        };

        assert_eq!(eq.value, ScalarCompareLiteral::One(ScalarLiteral::Int(10)));
        assert_eq!(
            in_list.value,
            ScalarCompareLiteral::Many(vec![ScalarLiteral::Int(1), ScalarLiteral::Int(2)]),
        );
    }

    #[test]
    fn scalar_literal_predicate_path_preserves_null_and_variant_mismatch_semantics() {
        let strict = CoercionSpec::new(CoercionId::Strict);

        let null_eq = eval_compare_scalar_literal_slot(
            ScalarSlotValueRef::Null,
            CompareOp::Eq,
            &ScalarCompareLiteral::One(ScalarLiteral::Null),
            &strict,
        );
        let null_in = eval_compare_scalar_literal_slot(
            ScalarSlotValueRef::Null,
            CompareOp::In,
            &ScalarCompareLiteral::Many(vec![ScalarLiteral::Null]),
            &strict,
        );
        let mismatch = eval_compare_scalar_literal_slot(
            ScalarSlotValueRef::Value(ScalarValueRef::Int(7)),
            CompareOp::Eq,
            &ScalarCompareLiteral::One(ScalarLiteral::Text("x".to_string())),
            &strict,
        );

        assert_eq!(null_eq, Some(true));
        assert_eq!(null_in, Some(true));
        assert_eq!(mismatch, Some(false));
    }

    #[test]
    fn scalar_literal_predicate_path_matches_text_prefix_suffix_semantics() {
        let strict = CoercionSpec::new(CoercionId::Strict);
        let casefold = CoercionSpec::new(CoercionId::TextCasefold);
        let actual = ScalarSlotValueRef::Value(ScalarValueRef::Text("Alpha"));

        let strict_prefix = eval_compare_scalar_literal_slot(
            actual,
            CompareOp::StartsWith,
            &ScalarCompareLiteral::One(ScalarLiteral::Text("Al".to_string())),
            &strict,
        );
        let ci_suffix = eval_compare_scalar_literal_slot(
            actual,
            CompareOp::EndsWith,
            &ScalarCompareLiteral::One(ScalarLiteral::Text("HA".to_string())),
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
        let mut slots = PredicateTestSlotReader {
            score: Some(ScalarSlotValueRef::Value(ScalarValueRef::Int(7))),
            name: Some(ScalarSlotValueRef::Value(ScalarValueRef::Text("Alpha"))),
        };

        assert!(program.uses_scalar_program());
        assert!(
            program
                .eval_with_structural_slot_reader(&mut slots)
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
        let mut slots = PredicateTestSlotReader {
            score: None,
            name: Some(ScalarSlotValueRef::Value(ScalarValueRef::Text("Alpha"))),
        };

        assert!(program.uses_scalar_program());
        assert!(
            program
                .eval_with_structural_slot_reader(&mut slots)
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
