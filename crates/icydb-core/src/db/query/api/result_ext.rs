use crate::{
    db::{EntityResponse, ResponseError, Row},
    prelude::*,
    types::Id,
};

///
/// ResponseCardinalityExt
///
/// Query/session-layer cardinality helpers for scalar `EntityResponse<E>` payloads.
/// These methods intentionally live outside `db::response` so cardinality
/// semantics remain owned by the query/session API boundary.
///

pub trait ResponseCardinalityExt<E: EntityKind> {
    /// Require exactly one row in this response.
    fn require_one(&self) -> Result<(), ResponseError>;

    /// Require at least one row in this response.
    fn require_some(&self) -> Result<(), ResponseError>;

    /// Consume and return `None` for empty, `Some(row)` for one row, or error for many rows.
    fn try_row(self) -> Result<Option<Row<E>>, ResponseError>;

    /// Consume and return the single row, or fail on zero/many rows.
    fn row(self) -> Result<Row<E>, ResponseError>;

    /// Consume and return the single entity or `None`, failing on many rows.
    fn try_entity(self) -> Result<Option<E>, ResponseError>;

    /// Consume and return the single entity, failing on zero/many rows.
    fn entity(self) -> Result<E, ResponseError>;

    /// Require exactly one row and return its identifier.
    fn require_id(self) -> Result<Id<E>, ResponseError>;
}

impl<E: EntityKind> ResponseCardinalityExt<E> for EntityResponse<E> {
    fn require_one(&self) -> Result<(), ResponseError> {
        match self.count() {
            1 => Ok(()),
            0 => Err(ResponseError::NotFound { entity: E::PATH }),
            n => Err(ResponseError::NotUnique {
                entity: E::PATH,
                count: n,
            }),
        }
    }

    fn require_some(&self) -> Result<(), ResponseError> {
        if self.is_empty() {
            Err(ResponseError::NotFound { entity: E::PATH })
        } else {
            Ok(())
        }
    }

    #[expect(clippy::cast_possible_truncation)]
    fn try_row(self) -> Result<Option<Row<E>>, ResponseError> {
        let mut rows = self.rows();

        match rows.len() {
            0 => Ok(None),
            1 => Ok(rows.pop()),
            n => Err(ResponseError::NotUnique {
                entity: E::PATH,
                count: n as u32,
            }),
        }
    }

    fn row(self) -> Result<Row<E>, ResponseError> {
        self.try_row()?
            .ok_or(ResponseError::NotFound { entity: E::PATH })
    }

    fn try_entity(self) -> Result<Option<E>, ResponseError> {
        Ok(self.try_row()?.map(Row::entity))
    }

    fn entity(self) -> Result<E, ResponseError> {
        self.try_entity()?
            .ok_or(ResponseError::NotFound { entity: E::PATH })
    }

    fn require_id(self) -> Result<Id<E>, ResponseError> {
        self.row().map(|row| row.id())
    }
}
