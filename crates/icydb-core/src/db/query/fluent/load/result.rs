use crate::{
    db::{EntityResponse, PagedGroupedExecutionWithTrace, query::intent::QueryError},
    traits::EntityKind,
};

///
/// LoadQueryResult
///
/// Unified fluent load execution payload for scalar and grouped query shapes.
/// Scalar queries materialize typed entity rows.
/// Grouped queries materialize grouped rows plus continuation metadata.
///
#[derive(Debug)]
pub enum LoadQueryResult<E: EntityKind> {
    Rows(EntityResponse<E>),
    Grouped(PagedGroupedExecutionWithTrace),
}

impl<E: EntityKind> LoadQueryResult<E> {
    // Shared constructors keep the outward fluent load result contract owned
    // by this facade instead of open-coding enum variants in session helpers.
    pub(crate) const fn rows(rows: EntityResponse<E>) -> Self {
        Self::Rows(rows)
    }

    pub(crate) const fn grouped(grouped: PagedGroupedExecutionWithTrace) -> Self {
        Self::Grouped(grouped)
    }

    // Grouped results expose row slices, so count conversion must saturate at
    // the fluent facade boundary instead of leaking usize into the API.
    fn grouped_count(grouped: &PagedGroupedExecutionWithTrace) -> u32 {
        u32::try_from(grouped.rows().len()).unwrap_or(u32::MAX)
    }

    // Shared grouped-vs-scalar mismatch errors keep the user-facing result
    // contract owned by this facade instead of rebuilding messages per method.
    fn grouped_rows_required_error() -> QueryError {
        QueryError::unsupported_query(
            "grouped queries return grouped rows; call execute() and inspect the grouped result",
        )
    }

    fn scalar_rows_required_error() -> QueryError {
        QueryError::unsupported_query(
            "scalar queries return entity rows; grouped results are not available",
        )
    }

    /// Return the number of emitted rows or groups.
    #[must_use]
    pub fn count(&self) -> u32 {
        match self {
            Self::Rows(rows) => rows.count(),
            Self::Grouped(grouped) => Self::grouped_count(grouped),
        }
    }

    /// Return whether no rows or groups were emitted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// Consume this result and require scalar entity rows.
    pub fn into_rows(self) -> Result<EntityResponse<E>, QueryError> {
        match self {
            Self::Rows(rows) => Ok(rows),
            Self::Grouped(_) => Err(Self::grouped_rows_required_error()),
        }
    }

    /// Consume this result and require grouped rows.
    pub fn into_grouped(self) -> Result<PagedGroupedExecutionWithTrace, QueryError> {
        match self {
            Self::Grouped(grouped) => Ok(grouped),
            Self::Rows(_) => Err(Self::scalar_rows_required_error()),
        }
    }
}
