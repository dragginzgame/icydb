//! Module: lib
//! Responsibility: crate root for the IcyDB core runtime surface.
//! Does not own: canister-facing facade APIs from the public `icydb` crate.
//! Boundary: exposes the engine subsystems used by schema, query, executor, and storage layers.

//! Core runtime for IcyDB: entity traits, values, executors, visitors, and
//! the ergonomics exported via the `prelude`.
#![warn(unreachable_pub)]
// The no-default test target intentionally type-checks shared test/helper
// surfaces whose consuming tests live behind SQL, SQL-explain, or diagnostics
// features. Keep production and all-features dead-code linting strict.
#![cfg_attr(all(test, not(feature = "sql")), allow(dead_code))]

extern crate self as icydb;

#[macro_use]
pub(crate) mod scalar_registry;

// public exports are one module level down
pub mod db;
pub mod entity;
pub mod error;
pub mod metrics;
pub mod model;
pub(crate) mod runtime;
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
        entity::EntityKind,
        model::{entity::EntityModel, index::IndexModel},
        traits::Path,
        value::{InputValue, OutputValue},
    };
}

// Macro/runtime wiring surface used by generated code in local core tests.
// This mirrors the facade crate's hidden generated-code boundary so derive
// output can target one stable path regardless of which workspace crate owns
// the test harness.
#[doc(hidden)]
pub mod __macro {
    #[doc(hidden)]
    pub fn decode_generated_runtime_field_value<T>(
        value: &crate::value::Value,
        context: Option<&dyn crate::value::RuntimeEnumContext>,
        field_name: &'static str,
    ) -> Result<T, crate::error::InternalError>
    where
        T: crate::value::RuntimeValueDecode,
    {
        crate::value::runtime_value_from_value_with_optional_enum_context(value, context)
            .ok_or_else(|| {
                crate::error::InternalError::persisted_row_field_decode_failed(field_name, ())
            })
    }

    pub use crate::db::{
        CompositePrimaryKeyValue, CompositePrimaryKeyValueError, EntityKey, EntityKeyBytes,
        EntityKeyBytesError, GeneratedStructuralMapPayloadSlices, JournalTailStore, KeyValueCodec,
        PersistedByKindCodec, PersistedRow, PersistedScalar, PersistedStructuralValueCodec,
        PrimaryKeyComponent, PrimaryKeyDecode, PrimaryKeyEncode, PrimaryKeyEncodeError,
        PrimaryKeyValue, ScalarRelationTargetKey, ScalarRelationTargetKeyMatchesDeclaredPrimitive,
        ScalarSlotValueRef, ScalarValueRef, SlotReader, StoreRuntimeStorageCapabilities,
        validate_entity_key_bytes_buffer,
    };
    pub use crate::entity::{
        EntityCreateInput, EntityCreateMaterialization, EntityCreateType, EntityDeclaration,
        EntityKind, EntityPlacement, EntityValue,
    };
    pub use crate::error::{ErrorClass, ErrorOrigin, InternalError};
    pub use crate::traits::{
        AuthoredFieldProjection, CanisterKind, FieldProjection, FieldTypeMeta, Inner, Path,
        StoreKind,
    };
    pub use crate::types::NumericValue;
    pub use crate::value::{
        InputValue, InputValueEnum, RuntimeEnumContext, RuntimeEnumSelection, RuntimeValueDecode,
        RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value, ValueEnum,
        runtime_value_btree_map_from_value, runtime_value_btree_set_from_value,
        runtime_value_collection_to_value, runtime_value_from_value,
        runtime_value_from_value_with_enum_context,
        runtime_value_from_value_with_optional_enum_context, runtime_value_from_vec_into,
        runtime_value_from_vec_into_btree_map, runtime_value_from_vec_into_btree_set,
        runtime_value_into, runtime_value_map_collection_to_value, runtime_value_to_value,
        runtime_value_vec_from_value,
    };
    pub use ic_memory::{
        bootstrap_default_memory_manager, ic_memory_declaration, ic_memory_key, ic_memory_range,
    };
    pub use serde::Deserialize;
    pub use std::{
        clone::Clone,
        cmp::{Eq, Ord, PartialEq, PartialOrd},
        convert::From,
        default::Default,
        fmt::{Debug, Display},
        hash::Hash,
        iter::Sum,
        marker::Copy,
        ops::{
            Add, AddAssign, Deref, DerefMut, Div, DivAssign, Mul, MulAssign, Rem, Sub, SubAssign,
        },
    };

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
    pub fn generated_persisted_structured_payload_decode_failed(
        detail: impl std::fmt::Display,
    ) -> crate::error::InternalError {
        crate::db::generated_persisted_structured_payload_decode_failed(detail)
    }

    #[doc(hidden)]
    pub fn encode_non_enum_protocol_value_bytes(
        value: &crate::value::Value,
    ) -> Result<Vec<u8>, crate::error::InternalError> {
        crate::db::encode_non_enum_protocol_value_bytes(value)
    }

    #[doc(hidden)]
    pub fn decode_non_enum_protocol_value_bytes(
        raw_bytes: &[u8],
    ) -> Result<crate::value::Value, crate::error::InternalError> {
        crate::db::decode_non_enum_protocol_value_bytes(raw_bytes)
    }

    #[doc(hidden)]
    pub fn encode_persisted_structured_many_slot_payload<T>(
        value: &[T],
        field_name: &'static str,
    ) -> Result<Vec<u8>, crate::error::InternalError>
    where
        T: PersistedStructuralValueCodec,
    {
        crate::db::encode_persisted_structured_many_slot_payload(value, field_name)
    }

    #[doc(hidden)]
    pub fn decode_persisted_structured_many_slot_payload<T>(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Vec<T>, crate::error::InternalError>
    where
        T: PersistedStructuralValueCodec,
    {
        crate::db::decode_persisted_structured_many_slot_payload(bytes, field_name)
    }

    #[doc(hidden)]
    pub fn encode_persisted_structured_slot_payload<T>(
        value: &T,
        field_name: &'static str,
    ) -> Result<Vec<u8>, crate::error::InternalError>
    where
        T: PersistedStructuralValueCodec,
    {
        crate::db::encode_persisted_structured_slot_payload(value, field_name)
    }

    #[doc(hidden)]
    pub fn decode_persisted_structured_slot_payload<T>(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<T, crate::error::InternalError>
    where
        T: PersistedStructuralValueCodec,
    {
        crate::db::decode_persisted_structured_slot_payload(bytes, field_name)
    }

    #[doc(hidden)]
    pub fn encode_persisted_option_scalar_slot_payload<T>(
        value: &Option<T>,
        field_name: &'static str,
    ) -> Result<Vec<u8>, crate::error::InternalError>
    where
        T: PersistedScalar,
    {
        crate::db::encode_persisted_option_scalar_slot_payload(value, field_name)
    }

    #[doc(hidden)]
    pub fn decode_persisted_option_scalar_slot_payload<T>(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Option<T>, crate::error::InternalError>
    where
        T: PersistedScalar,
    {
        crate::db::decode_persisted_option_scalar_slot_payload(bytes, field_name)
    }

    #[doc(hidden)]
    pub fn encode_persisted_scalar_slot_payload<T>(
        value: &T,
        field_name: &'static str,
    ) -> Result<Vec<u8>, crate::error::InternalError>
    where
        T: PersistedScalar,
    {
        crate::db::encode_persisted_scalar_slot_payload(value, field_name)
    }

    #[doc(hidden)]
    pub fn decode_persisted_scalar_slot_payload<T>(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<T, crate::error::InternalError>
    where
        T: PersistedScalar,
    {
        crate::db::decode_persisted_scalar_slot_payload(bytes, field_name)
    }

    #[doc(hidden)]
    pub fn encode_persisted_slot_payload_by_kind<T>(
        value: &T,
        kind: crate::model::field::FieldKind,
        field_name: &'static str,
    ) -> Result<Vec<u8>, crate::error::InternalError>
    where
        T: PersistedByKindCodec,
    {
        crate::db::encode_persisted_slot_payload_by_kind(value, kind, field_name)
    }

    #[doc(hidden)]
    pub fn decode_persisted_slot_payload_by_kind<T>(
        bytes: &[u8],
        kind: crate::model::field::FieldKind,
        field_name: &'static str,
    ) -> Result<T, crate::error::InternalError>
    where
        T: PersistedByKindCodec,
    {
        crate::db::decode_persisted_slot_payload_by_kind(bytes, kind, field_name)
    }

    #[doc(hidden)]
    pub fn decode_persisted_option_slot_payload_by_kind<T>(
        bytes: &[u8],
        kind: crate::model::field::FieldKind,
        field_name: &'static str,
    ) -> Result<Option<T>, crate::error::InternalError>
    where
        T: PersistedByKindCodec,
    {
        crate::db::decode_persisted_option_slot_payload_by_kind(bytes, kind, field_name)
    }
}
