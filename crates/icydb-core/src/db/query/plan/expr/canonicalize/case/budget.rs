//! Content-based admission for the existing CASE-to-boolean rewrite.
//! This bounds one expansion, not query admission, rendering or total planning.

use crate::{
    db::query::plan::expr::{CaseWhenArm, Expr},
    value::Value,
};

// Fixed logical units keep canonical identity independent of pointer width,
// allocation capacity and request state. Each visited node costs one unit;
// variable payload costs an additional unit per started 64 bytes.
const MAX_EXPANSION_UNITS: usize = 256;
const PAYLOAD_BYTES_PER_UNIT: usize = 64;

struct ExpansionBudget {
    remaining: usize,
}

impl ExpansionBudget {
    fn charge(&mut self, units: usize) -> Option<()> {
        self.remaining = self.remaining.checked_sub(units)?;
        Some(())
    }

    fn payload(&mut self, bytes: usize, copies: usize) -> Option<()> {
        self.charge(bytes.div_ceil(PAYLOAD_BYTES_PER_UNIT).checked_mul(copies)?)
    }

    // Charge before descending, so even an ineligible deep/wide operand needs
    // only a bounded walk. This does not clone, format or encode operands.
    fn expr(&mut self, expr: &Expr, copies: usize) -> Option<()> {
        self.charge(copies)?;
        match expr {
            Expr::Field(field) => self.payload(field.as_str().len(), copies),
            Expr::FieldPath(path) => {
                self.payload(path.root().as_str().len(), copies)?;
                for segment in path.segments() {
                    self.charge(copies)?;
                    self.payload(segment.len(), copies)?;
                }
                Some(())
            }
            Expr::Literal(value) => self.value(value, copies),
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    self.expr(arg, copies)?;
                }
                Some(())
            }
            Expr::Binary { left, right, .. } => {
                self.expr(left, copies)?;
                self.expr(right, copies)
            }
            Expr::Unary { expr, .. } => self.expr(expr, copies),
            Expr::Case {
                when_then_arms,
                else_expr,
            } => {
                for arm in when_then_arms {
                    self.expr(arm.condition(), copies)?;
                    self.expr(arm.result(), copies)?;
                }
                self.expr(else_expr, copies)
            }
            Expr::Aggregate(aggregate) => {
                if let Some(input) = aggregate.input_expr() {
                    self.expr(input, copies)?;
                }
                if let Some(filter) = aggregate.filter_expr() {
                    self.expr(filter, copies)?;
                }
                Some(())
            }
            #[cfg(test)]
            Expr::Alias { expr, name } => {
                self.payload(name.as_str().len(), copies)?;
                self.expr(expr, copies)
            }
        }
    }

    fn value(&mut self, value: &Value, copies: usize) -> Option<()> {
        self.charge(copies)?;
        match value {
            Value::Text(text) => self.payload(text.len(), copies),
            Value::Blob(blob) => self.payload(blob.len(), copies),
            Value::IntBig(value) => self.big_payload(value.magnitude_bits(), copies),
            Value::NatBig(value) => self.big_payload(value.magnitude_bits(), copies),
            Value::List(values) => {
                for value in values {
                    self.value(value, copies)?;
                }
                Some(())
            }
            Value::Map(entries) => {
                for (key, value) in entries {
                    self.value(key, copies)?;
                    self.value(value, copies)?;
                }
                Some(())
            }
            Value::Enum(value) => {
                if let Some(payload) = value.payload() {
                    self.value(payload, copies)?;
                }
                Some(())
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
            | Value::Unit => Some(()),
        }
    }

    fn big_payload(&mut self, bits: u64, copies: usize) -> Option<()> {
        self.payload(usize::try_from(bits.div_ceil(8)).ok()?, copies)
    }
}

pub(super) fn expansion_fits(arms: &[CaseWhenArm], else_expr: &Expr) -> bool {
    let mut budget = ExpansionBudget {
        remaining: MAX_EXPANSION_UNITS,
    };
    // Per arm: OR, two ANDs, NOT, two COALESCEs, and two false literals
    // (each literal includes its Value). Count even wrappers simplified away.
    let Some(()) = arms
        .len()
        .checked_mul(10)
        .and_then(|units| budget.charge(units))
    else {
        return false;
    };
    if budget.expr(else_expr, 1).is_none() {
        return false;
    }
    arms.iter().all(|arm| {
        budget.expr(arm.condition(), 2).is_some() && budget.expr(arm.result(), 1).is_some()
    })
}
