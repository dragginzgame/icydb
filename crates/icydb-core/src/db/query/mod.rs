mod delete;
mod load;
mod planner;
mod save;

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

use crate::{Error, ThisError, db::DbError, traits::EntityKind};

///
/// QueryError
///

#[derive(Debug, ThisError)]
pub enum QueryError {
    #[error("invalid filter field '{0}'")]
    InvalidFilterField(String),

    #[error("invalid index field '{0}'")]
    InvalidIndexField(String),

    #[error("invalid sort field '{0}'")]
    InvalidSortField(String),

    #[error("invalid filter value: {0}")]
    InvalidFilterValue(String),

    #[error("invalid comparator usage: {0}")]
    InvalidComparator(String),
}

impl From<QueryError> for Error {
    fn from(err: QueryError) -> Self {
        DbError::from(err).into()
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
pub fn load() -> LoadQuery {
    LoadQuery::new()
}

// delete
#[must_use]
pub fn delete() -> DeleteQuery {
    DeleteQuery::new()
}

// create
#[must_use]
pub fn insert() -> SaveQuery {
    SaveQuery::new(SaveMode::Insert)
}

// update
#[must_use]
pub fn update() -> SaveQuery {
    SaveQuery::new(SaveMode::Update)
}

// replace
#[must_use]
pub fn replace() -> SaveQuery {
    SaveQuery::new(SaveMode::Replace)
}
