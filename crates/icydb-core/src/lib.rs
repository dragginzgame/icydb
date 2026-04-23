//! Module: lib
//! Responsibility: crate root for the IcyDB core runtime surface.
//! Does not own: canister-facing facade APIs from the public `icydb` crate.
//! Boundary: exposes the engine subsystems used by schema, query, executor, and storage layers.

//! Core runtime for IcyDB: entity traits, values, executors, visitors, and
//! the ergonomics exported via the `prelude`.
#![warn(unreachable_pub)] // too complex to adhere to right now

extern crate self as icydb;

#[macro_use]
pub(crate) mod scalar_registry;

// public exports are one module level down
pub mod db;
pub mod error;
pub mod metrics;
pub mod model;
pub mod sanitize;
pub mod traits;
pub mod types;
pub mod validate;
pub mod value;
pub mod visitor;

// testing
#[cfg(test)]
pub(crate) mod testing;

///
/// CONSTANTS
///

/// Maximum number of indexed fields allowed on an entity.
///
/// This limit keeps hashed index keys within bounded, storable sizes and
/// simplifies sizing tests in the stores.
pub const MAX_INDEX_FIELDS: usize = 4;

///
/// Prelude
///
/// Prelude contains only domain vocabulary.
/// No errors, executors, stores, serializers, or helpers are re-exported here.
///

pub mod prelude {
    pub use crate::{
        model::{entity::EntityModel, index::IndexModel},
        traits::{EntityKind, Path},
        value::{InputValue, OutputValue},
    };
}

// Macro/runtime wiring surface used by generated code in local core tests.
// This mirrors the facade crate's hidden generated-code boundary so derive
// output can target one stable path regardless of which workspace crate owns
// the test harness.
#[doc(hidden)]
pub mod __macro {
    pub use crate::db::{GeneratedStructuralEnumPayload, GeneratedStructuralMapPayloadSlices};
    pub use crate::error::InternalError;
    pub use crate::traits::{
        EnumValue, FieldProjection, PersistedByKindCodec, PersistedFieldMetaCodec,
        PersistedStructuredFieldCodec, RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind,
        RuntimeValueMeta, runtime_value_btree_map_from_value, runtime_value_btree_set_from_value,
        runtime_value_collection_to_value, runtime_value_from_value, runtime_value_from_vec_into,
        runtime_value_from_vec_into_btree_map, runtime_value_from_vec_into_btree_set,
        runtime_value_into, runtime_value_map_collection_to_value, runtime_value_to_value,
        runtime_value_vec_from_value,
    };
    pub use crate::value::{Value, ValueEnum};

    #[doc(hidden)]
    #[must_use]
    pub fn encode_generated_structural_text_payload_bytes(value: &str) -> Vec<u8> {
        crate::db::encode_generated_structural_text_payload_bytes(value)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn encode_generated_structural_list_payload_bytes(items: &[&[u8]]) -> Vec<u8> {
        crate::db::encode_generated_structural_list_payload_bytes(items)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn encode_generated_structural_map_payload_bytes(entries: &[(&[u8], &[u8])]) -> Vec<u8> {
        crate::db::encode_generated_structural_map_payload_bytes(entries)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn encode_generated_structural_enum_payload_bytes(
        variant: &str,
        path: Option<&str>,
        payload: Option<&[u8]>,
    ) -> Vec<u8> {
        crate::db::encode_generated_structural_enum_payload_bytes(variant, path, payload)
    }

    #[doc(hidden)]
    pub fn decode_generated_structural_text_payload_bytes(
        raw_bytes: &[u8],
    ) -> Result<String, crate::error::InternalError> {
        crate::db::decode_generated_structural_text_payload_bytes(raw_bytes)
    }

    #[doc(hidden)]
    pub fn decode_generated_structural_list_payload_bytes(
        raw_bytes: &[u8],
    ) -> Result<Vec<&[u8]>, crate::error::InternalError> {
        crate::db::decode_generated_structural_list_payload_bytes(raw_bytes)
    }

    #[doc(hidden)]
    pub fn decode_generated_structural_map_payload_bytes(
        raw_bytes: &[u8],
    ) -> Result<crate::db::GeneratedStructuralMapPayloadSlices<'_>, crate::error::InternalError>
    {
        crate::db::decode_generated_structural_map_payload_bytes(raw_bytes)
    }

    #[doc(hidden)]
    pub fn decode_generated_structural_enum_payload_bytes(
        raw_bytes: &[u8],
    ) -> Result<crate::db::GeneratedStructuralEnumPayload<'_>, crate::error::InternalError> {
        crate::db::decode_generated_structural_enum_payload_bytes(raw_bytes)
    }

    #[doc(hidden)]
    pub fn generated_persisted_structured_payload_decode_failed(
        detail: impl std::fmt::Display,
    ) -> crate::error::InternalError {
        crate::db::generated_persisted_structured_payload_decode_failed(detail)
    }
}
