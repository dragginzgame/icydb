//! Content-based admission for the existing CASE-to-boolean rewrite.
//! This bounds one expansion, not query admission, rendering or total planning.

use crate::{
    db::{
        QueryError,
        query::{
            plan::expr::{CaseWhenArm, Expr},
            preparation::PreparationWork,
        },
    },
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

// Fixed logical units keep canonical identity independent of pointer width,
// allocation capacity and request state. Each visited node costs one unit;
// variable payload costs an additional unit per started 64 bytes.
const MAX_EXPANSION_UNITS: usize = 256;
const PAYLOAD_BYTES_PER_UNIT: usize = 64;
const MAX_ARMS: usize = 8;

/// Short-lived evidence from the existing admission walk, never a cached plan.
#[derive(Debug)]
pub(super) struct AdmittedExpansion {
    pub(super) condition_copies: [ConditionCopy; MAX_ARMS],
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ConditionCopy {
    units: usize,
    values: u64,
}

impl ConditionCopy {
    // Every expression/value node owns at most one parent cell; vector pairs
    // and CASE arms have two children and therefore two units. Path segments
    // own String cells. Each payload unit covers 64 bytes, including rounding
    // big-integer magnitude to a 32-/64-bit limb. Charging the largest cell per
    // unit thus bounds a fresh Clone, not source capacity or allocator overhead.
    fn bytes(self) -> u64 {
        let cell = size_of::<Expr>()
            .max(size_of::<Value>())
            .max(size_of::<String>())
            .max(PAYLOAD_BYTES_PER_UNIT);
        (self.units as u64).saturating_mul(cell as u64)
    }

    /// Reserve a conservative backing/copy-work allowance immediately before
    /// the necessary condition clone. Admission refusal never charges a copy.
    pub(super) fn charge(self, work: &PreparationWork<'_>) -> Result<(), QueryError> {
        let bytes = self.bytes();
        work.charge(Resource::TemporaryBytes, bytes)?;
        work.charge(
            Resource::PredicateExpressionSteps,
            bytes.saturating_add(self.units as u64),
        )?;
        if self.values != 0 {
            work.charge(Resource::NestedValueSteps, self.values)?;
        }
        Ok(())
    }
}

struct ExpansionBudget<'work, 'request> {
    remaining: usize,
    values: u64,
    work: &'work PreparationWork<'request>,
}

impl ExpansionBudget<'_, '_> {
    // Each fixed-budget accounting operation consumes current request work,
    // including the operation that declines expansion. Request failure is an
    // error, never a different content-based canonicalization decision.
    fn charge(&mut self, units: usize) -> Result<bool, QueryError> {
        self.work.charge(Resource::PredicateExpressionSteps, 1)?;
        let Some(remaining) = self.remaining.checked_sub(units) else {
            return Ok(false);
        };
        self.remaining = remaining;
        Ok(true)
    }

    fn payload(&mut self, bytes: usize, copies: usize) -> Result<bool, QueryError> {
        let Some(units) = bytes.div_ceil(PAYLOAD_BYTES_PER_UNIT).checked_mul(copies) else {
            return Ok(false);
        };
        self.charge(units)
    }

    // Charge before descending, so even an ineligible deep/wide operand needs
    // only a bounded walk. This does not clone, format or encode operands.
    fn expr(&mut self, expr: &Expr, copies: usize) -> Result<bool, QueryError> {
        if !self.charge(copies)? {
            return Ok(false);
        }
        match expr {
            Expr::Field(field) => self.payload(field.as_str().len(), copies),
            Expr::FieldPath(path) => {
                if !self.payload(path.root().as_str().len(), copies)? {
                    return Ok(false);
                }
                for segment in path.segments() {
                    if !self.charge(copies)? || !self.payload(segment.len(), copies)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Expr::Literal(value) => self.value(value, copies),
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    if !self.expr(arg, copies)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Expr::Binary { left, right, .. } => {
                if !self.expr(left, copies)? {
                    return Ok(false);
                }
                self.expr(right, copies)
            }
            Expr::Unary { expr, .. } => self.expr(expr, copies),
            Expr::Case {
                when_then_arms,
                else_expr,
            } => {
                for arm in when_then_arms {
                    if !self.expr(arm.condition(), copies)? || !self.expr(arm.result(), copies)? {
                        return Ok(false);
                    }
                }
                self.expr(else_expr, copies)
            }
            Expr::Aggregate(aggregate) => {
                if let Some(input) = aggregate.input_expr()
                    && !self.expr(input, copies)?
                {
                    return Ok(false);
                }
                if let Some(filter) = aggregate.filter_expr()
                    && !self.expr(filter, copies)?
                {
                    return Ok(false);
                }
                Ok(true)
            }
            #[cfg(test)]
            Expr::Alias { expr, name } => {
                if !self.payload(name.as_str().len(), copies)? {
                    return Ok(false);
                }
                self.expr(expr, copies)
            }
        }
    }

    fn value(&mut self, value: &Value, copies: usize) -> Result<bool, QueryError> {
        self.work.charge(Resource::NestedValueSteps, 1)?;
        self.values = self.values.saturating_add(1);
        if !self.charge(copies)? {
            return Ok(false);
        }
        match value {
            Value::Text(text) => self.payload(text.len(), copies),
            Value::Blob(blob) => self.payload(blob.len(), copies),
            Value::IntBig(value) => self.big_payload(value.magnitude_bits(), copies),
            Value::NatBig(value) => self.big_payload(value.magnitude_bits(), copies),
            Value::List(values) => {
                for value in values {
                    if !self.value(value, copies)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Value::Map(entries) => {
                for (key, value) in entries {
                    if !self.value(key, copies)? || !self.value(value, copies)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Value::Enum(value) => {
                if let Some(payload) = value.payload()
                    && !self.value(payload, copies)?
                {
                    return Ok(false);
                }
                Ok(true)
            }
            Value::Account(_)
            | Value::Bool(_)
            | Value::Date(_)
            | Value::Decimal(_)
            | Value::Duration(_)
            | Value::Float32(_)
            | Value::Float64(_)
            | Value::Int64(_)
            | Value::Int128(_)
            | Value::Nat64(_)
            | Value::Nat128(_)
            | Value::Null
            | Value::Principal(_)
            | Value::Subaccount(_)
            | Value::Timestamp(_)
            | Value::U256(_)
            | Value::Ulid(_)
            | Value::Unit => Ok(true),
        }
    }

    fn big_payload(&mut self, bits: u64, copies: usize) -> Result<bool, QueryError> {
        let Ok(bytes) = usize::try_from(bits.div_ceil(8)) else {
            return Ok(false);
        };
        self.payload(bytes, copies)
    }
}

pub(super) fn admit_expansion(
    arms: &[CaseWhenArm],
    else_expr: &Expr,
    work: &PreparationWork<'_>,
) -> Result<Option<AdmittedExpansion>, QueryError> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    if arms.is_empty() || arms.len() > MAX_ARMS {
        return Ok(None);
    }
    let mut budget = ExpansionBudget {
        remaining: MAX_EXPANSION_UNITS,
        values: 0,
        work,
    };
    // Per arm: OR, two ANDs, NOT, two COALESCEs, and two false literals
    // (each literal includes its Value). Count even wrappers simplified away.
    let Some(units) = arms.len().checked_mul(10) else {
        return Ok(None);
    };
    if !budget.charge(units)? || !budget.expr(else_expr, 1)? {
        return Ok(None);
    }
    let mut condition_copies = [ConditionCopy::default(); MAX_ARMS];
    // The arm-count check above makes the fixed stack receipt complete; zip
    // cannot discard an admitted arm. Capture one clone's logical units from
    // the existing two-occurrence condition charge, without another tree walk.
    for (arm, copy) in arms.iter().zip(&mut condition_copies) {
        let remaining = budget.remaining;
        let values = budget.values;
        if !budget.expr(arm.condition(), 2)? {
            return Ok(None);
        }
        *copy = ConditionCopy {
            units: remaining.saturating_sub(budget.remaining) / 2,
            values: budget.values.saturating_sub(values),
        };
        if !budget.expr(arm.result(), 1)? {
            return Ok(None);
        }
    }
    Ok(Some(AdmittedExpansion { condition_copies }))
}
