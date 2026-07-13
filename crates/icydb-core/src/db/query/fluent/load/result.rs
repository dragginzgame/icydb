use crate::{
    db::{EntityResponse, PagedGroupedExecutionWithTrace, query::intent::QueryError},
    entity::EntityKind,
};
use icydb_diagnostic_code::QueryResultShapeCode;

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
    /// Return the number of emitted rows or groups.
    #[must_use]
    pub fn count(&self) -> u32 {
        match self {
            Self::Rows(rows) => rows.count(),
            Self::Grouped(grouped) => u32::try_from(grouped.rows().len()).unwrap_or(u32::MAX),
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
            Self::Grouped(_) => Err(QueryError::result_shape_mismatch(
                QueryResultShapeCode::ExpectedRows,
            )),
        }
    }

    /// Consume this result and require grouped rows.
    pub fn into_grouped(self) -> Result<PagedGroupedExecutionWithTrace, QueryError> {
        match self {
            Self::Grouped(grouped) => Ok(grouped),
            Self::Rows(_) => Err(QueryError::result_shape_mismatch(
                QueryResultShapeCode::ExpectedGroupedRows,
            )),
        }
    }
}
