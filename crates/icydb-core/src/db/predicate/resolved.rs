//! Module: predicate::resolved
//! Responsibility: canonical executable predicate representation for runtime execution.
//! Does not own: field-name schema mapping itself.
//! Boundary: produced once at predicate compile time and consumed by runtime and index execution.

use crate::{
    db::predicate::{coercion::CoercionSpec, model::CompareOp},
    value::Value,
};

///
/// ExecutableCompareOperand
///
/// One compiled compare operand carried by the canonical executable predicate
/// tree. Operands are either pre-resolved field slots or embedded literal
/// payloads.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutableCompareOperand {
    FieldSlot(Option<usize>),
    Literal(Value),
}

///
/// ExecutableComparePredicate
///
/// One canonical executable comparison node with compiled operands.
/// Runtime filtering and index-only compilation both consume this single
/// compare shape instead of carrying separate field-vs-literal and
/// field-vs-field execution nodes.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExecutableComparePredicate {
    pub(in crate::db) left: ExecutableCompareOperand,
    pub(in crate::db) op: CompareOp,
    pub(in crate::db) right: ExecutableCompareOperand,
    pub(in crate::db) coercion: CoercionSpec,
}

impl ExecutableComparePredicate {
    /// Construct one field-vs-literal executable compare node.
    #[must_use]
    pub(in crate::db) const fn field_literal(
        field_slot: Option<usize>,
        op: CompareOp,
        value: Value,
        coercion: CoercionSpec,
    ) -> Self {
        Self {
            left: ExecutableCompareOperand::FieldSlot(field_slot),
            op,
            right: ExecutableCompareOperand::Literal(value),
            coercion,
        }
    }

    /// Construct one field-vs-field executable compare node.
    #[must_use]
    pub(in crate::db) const fn field_field(
        left_field_slot: Option<usize>,
        op: CompareOp,
        right_field_slot: Option<usize>,
        coercion: CoercionSpec,
    ) -> Self {
        Self {
            left: ExecutableCompareOperand::FieldSlot(left_field_slot),
            op,
            right: ExecutableCompareOperand::FieldSlot(right_field_slot),
            coercion,
        }
    }

    /// Return the left operand when it is a field slot.
    #[must_use]
    pub(in crate::db) const fn left_field_slot(&self) -> Option<usize> {
        match self.left {
            ExecutableCompareOperand::FieldSlot(slot) => slot,
            ExecutableCompareOperand::Literal(_) => None,
        }
    }

    /// Return the right operand when it is a field slot.
    #[must_use]
    pub(in crate::db) const fn right_field_slot(&self) -> Option<usize> {
        match self.right {
            ExecutableCompareOperand::FieldSlot(slot) => slot,
            ExecutableCompareOperand::Literal(_) => None,
        }
    }

    /// Borrow the right operand when it is a literal payload.
    #[must_use]
    pub(in crate::db) const fn right_literal(&self) -> Option<&Value> {
        match &self.right {
            ExecutableCompareOperand::Literal(value) => Some(value),
            ExecutableCompareOperand::FieldSlot(_) => None,
        }
    }
}

///
/// ExecutablePredicate
///
/// Canonical predicate execution tree emitted by planning-time compilation.
/// Runtime row filtering and index-only compilation both consume this one
/// structural form instead of maintaining parallel execution representations.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutablePredicate {
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Compare(ExecutableComparePredicate),
    IsNull {
        field_slot: Option<usize>,
    },
    IsNotNull {
        field_slot: Option<usize>,
    },
    IsMissing {
        field_slot: Option<usize>,
    },
    IsEmpty {
        field_slot: Option<usize>,
    },
    IsNotEmpty {
        field_slot: Option<usize>,
    },
    TextContains {
        field_slot: Option<usize>,
        value: Value,
    },
    TextContainsCi {
        field_slot: Option<usize>,
        value: Value,
    },
}
