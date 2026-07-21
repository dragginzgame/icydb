//! Schema-owned SQL DDL field type contract selection.

use crate::db::schema::AcceptedFieldKind;
use crate::model::field::{DEFAULT_BIG_INT_MAX_BYTES, FieldStorageDecode, LeafCodec, ScalarCodec};

/// Persisted field contract selected for one SQL DDL column type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaDdlFieldTypeContract {
    kind: AcceptedFieldKind,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
}

impl SchemaDdlFieldTypeContract {
    const fn new(
        kind: AcceptedFieldKind,
        storage_decode: FieldStorageDecode,
        leaf_codec: LeafCodec,
    ) -> Self {
        Self {
            kind,
            storage_decode,
            leaf_codec,
        }
    }

    /// Borrow the accepted persisted field kind for the SQL DDL type.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> &AcceptedFieldKind {
        &self.kind
    }

    /// Return the accepted persisted field storage decode contract.
    #[must_use]
    pub(in crate::db) const fn storage_decode(&self) -> FieldStorageDecode {
        self.storage_decode
    }

    /// Return the accepted persisted field leaf codec.
    #[must_use]
    pub(in crate::db) const fn leaf_codec(&self) -> LeafCodec {
        self.leaf_codec
    }

    /// Consume the selected contract into allocation components.
    #[must_use]
    pub(in crate::db) fn into_parts(self) -> (AcceptedFieldKind, FieldStorageDecode, LeafCodec) {
        (self.kind, self.storage_decode, self.leaf_codec)
    }
}

/// Resolve a SQL DDL column type into the persisted field contract that schema
/// mutation may publish.
pub(in crate::db) fn resolve_sql_ddl_field_type_contract(
    column_type: &str,
) -> Option<SchemaDdlFieldTypeContract> {
    let normalized = column_type.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "bool" | "boolean" => Some(SchemaDdlFieldTypeContract::new(
            AcceptedFieldKind::Bool,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Bool),
        )),
        "int8" => Some(SchemaDdlFieldTypeContract::new(
            AcceptedFieldKind::Int8,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Int64),
        )),
        "int16" => Some(SchemaDdlFieldTypeContract::new(
            AcceptedFieldKind::Int16,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Int64),
        )),
        "int32" => Some(SchemaDdlFieldTypeContract::new(
            AcceptedFieldKind::Int32,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Int64),
        )),
        "int64" => Some(SchemaDdlFieldTypeContract::new(
            AcceptedFieldKind::Int64,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Int64),
        )),
        "int128" => Some(SchemaDdlFieldTypeContract::new(
            AcceptedFieldKind::Int128,
            FieldStorageDecode::ByKind,
            LeafCodec::Structural,
        )),
        "nat8" => Some(SchemaDdlFieldTypeContract::new(
            AcceptedFieldKind::Nat8,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )),
        "nat16" => Some(SchemaDdlFieldTypeContract::new(
            AcceptedFieldKind::Nat16,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )),
        "nat32" => Some(SchemaDdlFieldTypeContract::new(
            AcceptedFieldKind::Nat32,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )),
        "nat64" => Some(SchemaDdlFieldTypeContract::new(
            AcceptedFieldKind::Nat64,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )),
        "nat128" => Some(SchemaDdlFieldTypeContract::new(
            AcceptedFieldKind::Nat128,
            FieldStorageDecode::ByKind,
            LeafCodec::Structural,
        )),
        "text" | "string" => Some(SchemaDdlFieldTypeContract::new(
            AcceptedFieldKind::Text { max_len: None },
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
        )),
        _ => persisted_big_int_contract_for_sql_column_type(&normalized),
    }
}

fn persisted_big_int_contract_for_sql_column_type(
    normalized: &str,
) -> Option<SchemaDdlFieldTypeContract> {
    if let Some(max_bytes) = sql_big_int_type_max_bytes(normalized, "int_big") {
        return Some(SchemaDdlFieldTypeContract::new(
            AcceptedFieldKind::IntBig { max_bytes },
            FieldStorageDecode::ByKind,
            LeafCodec::Structural,
        ));
    }

    sql_big_int_type_max_bytes(normalized, "nat_big").map(|max_bytes| {
        SchemaDdlFieldTypeContract::new(
            AcceptedFieldKind::NatBig { max_bytes },
            FieldStorageDecode::ByKind,
            LeafCodec::Structural,
        )
    })
}

fn sql_big_int_type_max_bytes(normalized: &str, type_name: &str) -> Option<u32> {
    if normalized == type_name {
        return Some(DEFAULT_BIG_INT_MAX_BYTES);
    }

    let inner = normalized
        .strip_prefix(type_name)?
        .strip_prefix("(max_bytes=")?
        .strip_suffix(')')?;
    let max_bytes = inner.parse::<u32>().ok()?;
    if max_bytes == 0 {
        return None;
    }

    Some(max_bytes)
}
