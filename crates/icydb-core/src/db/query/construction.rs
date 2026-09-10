//! Construction accounting borrowed from the caller's existing budget owner.
//! This interface owns neither request lifetimes nor instruction intervals.

#[cfg(test)]
mod tests;
mod value;

use crate::error::InternalError;
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

/// Required accounting authority for fallible query construction.
pub(in crate::db) trait ConstructionBudget {
    /// Charge before work; preserve the owner's typed exhaustion error.
    fn charge(&self, resource: Resource, amount: u64) -> Result<(), InternalError>;
}

impl dyn ConstructionBudget + '_ {
    /// Copy an admitted label, including its visit, bytes and destination backing.
    pub(in crate::db) fn copy_text(&self, text: &str) -> Result<String, InternalError> {
        self.charge(Resource::PredicateExpressionSteps, 1 + text.len() as u64)?;
        self.charge(Resource::TemporaryBytes, text.len() as u64)?;
        Ok(text.to_string())
    }

    /// Admit known destination backing before allocation; children own payload charges.
    pub(in crate::db) fn vec_with_capacity<T>(&self, len: usize) -> Result<Vec<T>, InternalError> {
        self.charge(
            Resource::TemporaryBytes,
            (len as u64).saturating_mul(size_of::<T>() as u64),
        )?;
        Ok(Vec::with_capacity(len))
    }

    /// Admit one owned operand box before allocation.
    pub(in crate::db) fn boxed<T>(&self, value: T) -> Result<Box<T>, InternalError> {
        self.charge(Resource::TemporaryBytes, size_of::<T>() as u64)?;
        Ok(Box::new(value))
    }

    /// Charge new backing, including retained-prefix copies on growth, before
    /// reserving a container. This is cumulative construction, not live heap.
    pub(in crate::db) fn reserve_vec<T>(
        &self,
        values: &mut Vec<T>,
        additional: usize,
    ) -> Result<(), InternalError> {
        let capacity =
            self.reserve_capacity(values.len(), values.capacity(), additional, size_of::<T>())?;
        if capacity > values.capacity() {
            values.reserve_exact(capacity - values.len());
        }
        Ok(())
    }

    /// Reserve text backing under the same cumulative construction policy.
    pub(in crate::db) fn reserve_string(
        &self,
        text: &mut String,
        additional: usize,
    ) -> Result<(), InternalError> {
        let capacity = self.reserve_capacity(text.len(), text.capacity(), additional, 1)?;
        if capacity > text.capacity() {
            text.reserve_exact(capacity - text.len());
        }
        Ok(())
    }

    fn reserve_capacity(
        &self,
        len: usize,
        capacity: usize,
        additional: usize,
        element_bytes: usize,
    ) -> Result<usize, InternalError> {
        let required = len
            .checked_add(additional)
            .ok_or_else(InternalError::query_executor_invariant)?;
        if required <= capacity {
            return Ok(capacity);
        }
        let next = required.max(capacity.saturating_mul(2)).max(4);
        self.charge(
            Resource::TemporaryBytes,
            (next as u64).saturating_mul(element_bytes as u64),
        )?;
        Ok(next)
    }

    /// Append label bytes, charging growth (including the retained prefix) first.
    pub(in crate::db) fn push_text(
        &self,
        out: &mut String,
        text: &str,
    ) -> Result<(), InternalError> {
        self.charge(Resource::PredicateExpressionSteps, text.len() as u64)?;
        self.reserve_string(out, text.len())?;
        out.push_str(text);
        Ok(())
    }
}
