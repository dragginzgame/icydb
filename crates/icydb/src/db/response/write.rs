use crate::{
    error::{Error, ErrorKind, ErrorOrigin, RuntimeErrorKind},
    traits::Entity,
    types::Id,
};
use icydb_core::db::{ResponseError, WriteBatchResponse as CoreWriteBatchResponse};

///
/// MutationResult
///
/// Unified facade result for authored write operations.
/// This keeps insert, create, update, replace, structural mutation, batch
/// mutation, and count-first delete under one public result family instead of
/// exposing separate single-row, batch-row, and bare-count payload types.
///

#[derive(Debug)]
pub enum MutationResult<E: Entity> {
    Count { row_count: u32 },
    Entity(E),
    Entities(Vec<E>),
}

impl<E: Entity> MutationResult<E> {
    /// Construct one count-only mutation result.
    #[must_use]
    pub const fn from_count(row_count: u32) -> Self {
        Self::Count { row_count }
    }

    /// Construct one single-entity mutation result.
    #[must_use]
    pub const fn from_entity(entity: E) -> Self {
        Self::Entity(entity)
    }

    /// Construct one multi-entity mutation result.
    #[must_use]
    pub const fn from_entities(entities: Vec<E>) -> Self {
        Self::Entities(entities)
    }

    /// Construct one multi-entity mutation result from the core batch surface.
    #[must_use]
    pub fn from_core_batch(inner: CoreWriteBatchResponse<E>) -> Self {
        Self::from_entities(inner.into_iter().collect())
    }

    /// Return the number of rows represented by this mutation result.
    #[must_use]
    pub fn row_count(&self) -> u32 {
        match self {
            Self::Count { row_count } => *row_count,
            Self::Entity(_) => 1,
            Self::Entities(entities) => u32::try_from(entities.len()).unwrap_or(u32::MAX),
        }
    }

    /// Return the number of rows represented by this mutation result.
    #[must_use]
    pub fn count(&self) -> u32 {
        self.row_count()
    }

    /// Return whether this result contains no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.row_count() == 0
    }

    /// Return whether this result contains at least one row.
    #[must_use]
    pub fn exists(&self) -> bool {
        !self.is_empty()
    }

    /// Consume and return exactly one entity.
    pub fn entity(self) -> Result<E, Error> {
        match self {
            Self::Entity(entity) => Ok(entity),
            Self::Entities(mut entities) => match entities.len() {
                0 => Err(Error::from(ResponseError::not_found(E::PATH))),
                1 => Ok(entities.remove(0)),
                count => Err(Error::from(ResponseError::not_unique(
                    E::PATH,
                    u32::try_from(count).unwrap_or(u32::MAX),
                ))),
            },
            Self::Count { .. } => Err(Self::unsupported_shape_error("entity", "count")),
        }
    }

    /// Consume and return all entities represented by this result.
    pub fn entities(self) -> Result<Vec<E>, Error> {
        match self {
            Self::Entity(entity) => Ok(vec![entity]),
            Self::Entities(entities) => Ok(entities),
            Self::Count { .. } => Err(Self::unsupported_shape_error("entities", "count")),
        }
    }

    fn unsupported_shape_error(expected: &str, actual: &str) -> Error {
        Error::new(
            ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
            ErrorOrigin::Response,
            format!("mutation result does not contain {expected}; actual shape={actual}"),
        )
    }
}

impl<E: Entity> MutationResult<E> {
    /// Borrow exactly one primary identity from this mutation result.
    pub fn id(&self) -> Result<Id<E>, Error> {
        match self {
            Self::Entity(entity) => Ok(entity.id()),
            Self::Entities(entities) => match entities.as_slice() {
                [] => Err(Error::from(ResponseError::not_found(E::PATH))),
                [entity] => Ok(entity.id()),
                many => Err(Error::from(ResponseError::not_unique(
                    E::PATH,
                    u32::try_from(many.len()).unwrap_or(u32::MAX),
                ))),
            },
            Self::Count { .. } => Err(Self::unsupported_shape_error("id", "count")),
        }
    }

    /// Borrow all primary identities from this mutation result.
    pub fn ids(&self) -> Result<Vec<Id<E>>, Error> {
        match self {
            Self::Entity(entity) => Ok(vec![entity.id()]),
            Self::Entities(entities) => Ok(entities
                .iter()
                .map(icydb_core::traits::EntityValue::id)
                .collect()),
            Self::Count { .. } => Err(Self::unsupported_shape_error("ids", "count")),
        }
    }
}
