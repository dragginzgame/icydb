//! Copy admitted values without a separate sizing walk or semantic conversion.

use crate::{
    db::{QueryError, query::preparation::PreparationWork},
    value::{CanonicalEnumBody, Value, ValueEnum},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

impl PreparationWork<'_> {
    /// Copy one admitted operand, charging each visit and requested backing
    /// before allocation. Preserve tags, order, duplicates and enum identities;
    /// this is neither value admission nor retained-cache capacity accounting.
    pub(in crate::db) fn copy_value(&self, value: &Value) -> Result<Value, QueryError> {
        self.charge(Resource::NestedValueSteps, 1)?;
        let bytes = match value {
            Value::Text(text) => text.len() as u64,
            Value::Blob(blob) => blob.len() as u64,
            // Clone requests initialized magnitude limbs, not source spare
            // capacity. Whole 64-bit words cover both 32- and 64-bit limbs.
            Value::IntBig(integer) => integer.magnitude_bits().div_ceil(64).saturating_mul(8),
            Value::NatBig(integer) => integer.magnitude_bits().div_ceil(64).saturating_mul(8),
            Value::List(values) => {
                self.charge(
                    Resource::TemporaryBytes,
                    (values.len() as u64).saturating_mul(size_of::<Value>() as u64),
                )?;
                let mut copied = Vec::with_capacity(values.len());
                for value in values {
                    copied.push(self.copy_value(value)?);
                }
                return Ok(Value::List(copied));
            }
            Value::Map(entries) => {
                self.charge(
                    Resource::TemporaryBytes,
                    (entries.len() as u64).saturating_mul(size_of::<(Value, Value)>() as u64),
                )?;
                let mut copied = Vec::with_capacity(entries.len());
                for (key, value) in entries {
                    copied.push((self.copy_value(key)?, self.copy_value(value)?));
                }
                return Ok(Value::Map(copied));
            }
            Value::Enum(value) => {
                let body = match value.body() {
                    CanonicalEnumBody::Unit => CanonicalEnumBody::Unit,
                    CanonicalEnumBody::Payload(payload) => {
                        let copied = self.copy_value(payload)?;
                        self.charge(Resource::TemporaryBytes, size_of::<Value>() as u64)?;
                        CanonicalEnumBody::Payload(Box::new(copied))
                    }
                };
                return Ok(Value::Enum(ValueEnum::new(
                    value.type_id(),
                    value.variant_id(),
                    body,
                )));
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
            | Value::Unit => 0,
        };
        // Byte-copy work is separate from the per-value visit. Conservative
        // big-integer storage allowance also bounds its initialized limb copy.
        if bytes != 0 {
            self.charge(Resource::PredicateExpressionSteps, bytes)?;
            self.charge(Resource::TemporaryBytes, bytes)?;
        }
        Ok(value.clone())
    }
}

#[cfg(test)]
mod tests;
