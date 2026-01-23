mod delete;
mod load;
mod planner;
mod save;
pub mod v2;

pub use delete::*;
pub use load::*;
pub use planner::*;
pub use save::*;

///
/// Query Prelude
///

pub mod prelude {
    pub use crate::db::{
        primitives::{
            filter::{FilterDsl, FilterExt as _},
            limit::LimitExt as _,
            sort::SortExt as _,
        },
        query,
    };
}

use crate::{
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::EntityKind,
};
use thiserror::Error as ThisError;

///
/// QueryError
///

#[derive(Debug, ThisError)]
pub enum QueryError {
    #[error("invalid filter field '{0}'")]
    InvalidFilterField(String),

    #[error("invalid filter value: {0}")]
    InvalidFilterValue(String),

    #[error("invalid sort field '{0}'")]
    InvalidSortField(String),
}

impl QueryError {
    pub(crate) const fn class(&self) -> ErrorClass {
        match self {
            Self::InvalidFilterField(_)
            | Self::InvalidFilterValue(_)
            | Self::InvalidSortField(_) => ErrorClass::Unsupported,
        }
    }
}

impl From<QueryError> for InternalError {
    fn from(err: QueryError) -> Self {
        Self::new(err.class(), ErrorOrigin::Query, err.to_string())
    }
}

///
/// QueryValidate Trait
///

pub trait QueryValidate<E: EntityKind> {
    fn validate(&self) -> Result<(), QueryError>;
}

impl<E: EntityKind, T: QueryValidate<E>> QueryValidate<E> for Box<T> {
    fn validate(&self) -> Result<(), QueryError> {
        (**self).validate()
    }
}

// load
#[must_use]
/// Start building a `LoadQuery`.
pub fn load() -> LoadQuery {
    LoadQuery::new()
}

// delete
#[must_use]
/// Start building a `DeleteQuery`.
pub fn delete() -> DeleteQuery {
    DeleteQuery::new()
}

// create
#[must_use]
/// Build an insert `SaveQuery`.
pub fn insert() -> SaveQuery {
    SaveQuery::new(SaveMode::Insert)
}

// update
#[must_use]
/// Build an update `SaveQuery`.
pub fn update() -> SaveQuery {
    SaveQuery::new(SaveMode::Update)
}

// replace
#[must_use]
/// Build a replace `SaveQuery`.
pub fn replace() -> SaveQuery {
    SaveQuery::new(SaveMode::Replace)
}
