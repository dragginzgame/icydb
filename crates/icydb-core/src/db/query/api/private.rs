//! Module: db::query::api::private
//! Responsibility: module-local ownership and contracts for db::query::api::private.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{db::EntityResponse, prelude::*};

///
/// SealedResponseCardinalityExt
///
/// Internal marker used to seal query-layer response cardinality extension
/// trait implementations to canonical response DTOs.
///

pub trait SealedResponseCardinalityExt<E: EntityKind> {}

impl<E: EntityKind> SealedResponseCardinalityExt<E> for EntityResponse<E> {}
