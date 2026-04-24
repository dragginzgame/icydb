use icydb_core::db::LoadQueryResult as CoreLoadQueryResult;

use crate::{
    db::response::{PagedGroupedResponse, Response},
    error::{Error, ErrorKind, ErrorOrigin, RuntimeErrorKind},
    traits::EntityKind,
};

///
/// QueryResponse
///
/// Unified fluent query response payload for scalar and grouped query shapes.
/// Scalar queries return typed entity rows.
/// Grouped queries return grouped rows plus continuation metadata.
///

#[derive(Debug)]
pub enum QueryResponse<E: EntityKind> {
    Rows(Response<E>),
    Grouped(PagedGroupedResponse),
}

impl<E: EntityKind> QueryResponse<E> {
    pub(crate) fn from_core(inner: CoreLoadQueryResult<E>) -> Self {
        match inner {
            CoreLoadQueryResult::Rows(rows) => Self::Rows(Response::from_core(rows)),
            CoreLoadQueryResult::Grouped(grouped) => {
                let (rows, continuation_cursor, execution_trace) = grouped.into_parts();
                let next_cursor = continuation_cursor
                    .as_deref()
                    .map(icydb_core::db::encode_cursor);

                Self::Grouped(PagedGroupedResponse::new(
                    rows,
                    next_cursor,
                    execution_trace,
                ))
            }
        }
    }

    /// Return whether this query produced scalar entity rows.
    #[must_use]
    pub const fn is_rows(&self) -> bool {
        matches!(self, Self::Rows(_))
    }

    /// Return whether this query produced grouped rows.
    #[must_use]
    pub const fn is_grouped(&self) -> bool {
        matches!(self, Self::Grouped(_))
    }

    /// Consume this response and require scalar entity rows.
    pub fn into_rows(self) -> Result<Response<E>, Error> {
        match self {
            Self::Rows(rows) => Ok(rows),
            Self::Grouped(_) => Err(Error::new(
                ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
                ErrorOrigin::Query,
                "grouped queries return grouped rows; inspect QueryResponse::Grouped",
            )),
        }
    }

    /// Consume this response and require grouped rows.
    pub fn into_grouped(self) -> Result<PagedGroupedResponse, Error> {
        match self {
            Self::Grouped(grouped) => Ok(grouped),
            Self::Rows(_) => Err(Error::new(
                ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
                ErrorOrigin::Query,
                "scalar queries return entity rows; grouped results are not available",
            )),
        }
    }
}
