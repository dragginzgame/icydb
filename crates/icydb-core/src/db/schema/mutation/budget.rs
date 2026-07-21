//! Module: db::schema::mutation::budget
//! Responsibility: bound authoritative source rows consumed by complete schema work.
//! Does not own: operation-specific derived-state or publication accounting.
//! Boundary: schema mutation controllers consume one shared row/byte budget.

#[cfg(feature = "sql")]
use crate::error::SchemaTransitionBudgetResource;

const MAX_SOURCE_ROWS: usize = 65_536;
const MAX_SOURCE_ROW_BYTES: usize = 256 * 1024 * 1024;

///
/// SchemaTransitionSourceBudgetError
///
/// Exact resource rejected by the shared source-domain budget.
/// Schema mutation owns this classification before any physical write.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaTransitionSourceBudgetError {
    /// Cumulative authoritative source-row bytes exceeded the cap.
    RowBytes,

    /// Number of authoritative source rows exceeded the cap.
    Rows,
}

impl SchemaTransitionSourceBudgetError {
    /// Return the public schema-transition resource identity.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) const fn resource(self) -> SchemaTransitionBudgetResource {
        match self {
            Self::Rows => SchemaTransitionBudgetResource::SourceRows,
            Self::RowBytes => SchemaTransitionBudgetResource::SourceRowBytes,
        }
    }
}

///
/// SchemaTransitionSourceBudget
///
/// Incremental exact source-domain budget shared by complete schema operations.
/// Schema mutation owns its counters; operation-specific stages only consume it.
///

pub(in crate::db) struct SchemaTransitionSourceBudget {
    source_row_bytes: usize,
    source_rows: usize,
}

impl SchemaTransitionSourceBudget {
    /// Build the maintained complete-domain source budget.
    #[must_use]
    pub(in crate::db) const fn standard() -> Self {
        Self {
            source_row_bytes: 0,
            source_rows: 0,
        }
    }

    /// Consume one authoritative row before operation-specific work begins.
    pub(in crate::db) fn consume_source_row(
        &mut self,
        encoded_row_bytes: usize,
    ) -> Result<(), SchemaTransitionSourceBudgetError> {
        self.source_rows = self
            .source_rows
            .checked_add(1)
            .ok_or(SchemaTransitionSourceBudgetError::Rows)?;
        if self.source_rows > MAX_SOURCE_ROWS {
            return Err(SchemaTransitionSourceBudgetError::Rows);
        }

        self.source_row_bytes = self
            .source_row_bytes
            .checked_add(encoded_row_bytes)
            .ok_or(SchemaTransitionSourceBudgetError::RowBytes)?;
        if self.source_row_bytes > MAX_SOURCE_ROW_BYTES {
            return Err(SchemaTransitionSourceBudgetError::RowBytes);
        }

        Ok(())
    }

    /// Return the exact number of source rows consumed so far.
    #[must_use]
    pub(in crate::db) const fn source_rows(&self) -> usize {
        self.source_rows
    }

    /// Return the exact cumulative source-row bytes consumed so far.
    #[must_use]
    pub(in crate::db) const fn source_row_bytes(&self) -> usize {
        self.source_row_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_budget_matches_the_maintained_complete_domain_caps() {
        let mut rows = SchemaTransitionSourceBudget::standard();
        for _ in 0..MAX_SOURCE_ROWS {
            rows.consume_source_row(0)
                .expect("rows through the exact cap should admit");
        }
        assert_eq!(
            rows.consume_source_row(0),
            Err(SchemaTransitionSourceBudgetError::Rows),
        );

        let mut bytes = SchemaTransitionSourceBudget::standard();
        assert_eq!(
            bytes.consume_source_row(MAX_SOURCE_ROW_BYTES + 1),
            Err(SchemaTransitionSourceBudgetError::RowBytes),
        );
    }
}
