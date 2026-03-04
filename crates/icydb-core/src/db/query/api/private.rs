use crate::{db::EntityResponse, prelude::*};

///
/// SealedResponseCardinalityExt
///
/// Internal marker used to seal query-layer response cardinality extension
/// trait implementations to canonical response DTOs.
///

pub trait SealedResponseCardinalityExt<E: EntityKind> {}

impl<E: EntityKind> SealedResponseCardinalityExt<E> for EntityResponse<E> {}
