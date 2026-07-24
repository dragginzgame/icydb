//! Store-free reusable schema fragments.

use std::collections::BTreeSet;

use candid::CandidType;
use serde::{Deserialize, Serialize};

use crate::{
    ConstraintSourceKey, Decimal, EntitySourceKey, FieldSourceKey, IndexSourceKey,
    MAX_FRAGMENT_CONSTRAINTS, MAX_FRAGMENT_ENTITIES, MAX_FRAGMENT_FIELDS, MAX_FRAGMENT_INDEXES,
    MAX_FRAGMENT_RELATIONS, MAX_FRAGMENT_TYPES, RelationSourceKey, ScalarKind, ScalarLiteral,
    SchemaContractError, SchemaName, SourceCheckExpr, TypeSourceKey,
};

/// Logical type reference in a proposal fragment.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FieldType {
    /// Exact built-in scalar contract.
    Scalar(ScalarType),
    /// Named record, enum, newtype, or collection definition.
    Named(TypeSourceKey),
}

/// Exact scalar field contract required by accepted-schema lowering.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ScalarType {
    /// ICRC account.
    Account,
    /// Binary value with an optional maximum byte length.
    Blob {
        /// Maximum bytes, or no field-specific maximum.
        max_len: Option<u32>,
    },
    /// Boolean.
    Bool,
    /// Day-precision date.
    Date,
    /// Fixed-point decimal with exact accepted scale.
    Decimal {
        /// Accepted decimal scale.
        scale: u32,
    },
    /// Millisecond duration.
    Duration,
    /// Finite 32-bit float.
    Float32,
    /// Finite 64-bit float.
    Float64,
    /// Signed 8-bit integer.
    Int8,
    /// Signed 16-bit integer.
    Int16,
    /// Signed 32-bit integer.
    Int32,
    /// Signed 64-bit integer.
    Int64,
    /// Signed 128-bit integer.
    Int128,
    /// Arbitrary-precision signed integer with an encoded-byte bound.
    IntBig {
        /// Maximum canonical encoded bytes.
        max_bytes: u32,
    },
    /// Principal.
    Principal,
    /// Fixed-width subaccount.
    Subaccount,
    /// Text with an optional maximum Unicode-scalar length.
    Text {
        /// Maximum Unicode scalar count, or no field-specific maximum.
        max_len: Option<u32>,
    },
    /// Millisecond timestamp.
    Timestamp,
    /// Unsigned 8-bit integer.
    Nat8,
    /// Unsigned 16-bit integer.
    Nat16,
    /// Unsigned 32-bit integer.
    Nat32,
    /// Unsigned 64-bit integer.
    Nat64,
    /// Unsigned 128-bit integer.
    Nat128,
    /// Arbitrary-precision unsigned integer with an encoded-byte bound.
    NatBig {
        /// Maximum canonical encoded bytes.
        max_bytes: u32,
    },
    /// ULID.
    Ulid,
    /// Unit.
    Unit,
}

impl ScalarType {
    /// Return the intrinsic scalar capability kind.
    #[must_use]
    pub const fn kind(self) -> ScalarKind {
        match self {
            Self::Account => ScalarKind::Account,
            Self::Blob { .. } => ScalarKind::Blob,
            Self::Bool => ScalarKind::Bool,
            Self::Date => ScalarKind::Date,
            Self::Decimal { .. } => ScalarKind::Decimal,
            Self::Duration => ScalarKind::Duration,
            Self::Float32 => ScalarKind::Float32,
            Self::Float64 => ScalarKind::Float64,
            Self::Int8 | Self::Int16 | Self::Int32 | Self::Int64 => ScalarKind::Int,
            Self::Int128 => ScalarKind::Int128,
            Self::IntBig { .. } => ScalarKind::IntBig,
            Self::Principal => ScalarKind::Principal,
            Self::Subaccount => ScalarKind::Subaccount,
            Self::Text { .. } => ScalarKind::Text,
            Self::Timestamp => ScalarKind::Timestamp,
            Self::Nat8 | Self::Nat16 | Self::Nat32 | Self::Nat64 => ScalarKind::Nat,
            Self::Nat128 => ScalarKind::Nat128,
            Self::NatBig { .. } => ScalarKind::NatBig,
            Self::Ulid => ScalarKind::Ulid,
            Self::Unit => ScalarKind::Unit,
        }
    }

    pub(crate) const fn validate(self) -> Result<(), SchemaContractError> {
        match self {
            Self::Decimal { scale } if scale > Decimal::max_supported_scale() => {
                Err(SchemaContractError::InvalidFieldType)
            }
            Self::IntBig { max_bytes: 0 } | Self::NatBig { max_bytes: 0 } => {
                Err(SchemaContractError::InvalidFieldType)
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn accepts_literal(self, literal: &ScalarLiteral) -> bool {
        match (self, literal) {
            (Self::Account, ScalarLiteral::Account(_))
            | (Self::Bool, ScalarLiteral::Bool(_))
            | (Self::Date, ScalarLiteral::Date(_))
            | (Self::Duration, ScalarLiteral::Duration(_))
            | (Self::Float32, ScalarLiteral::Float32(_))
            | (Self::Float64, ScalarLiteral::Float64(_))
            | (Self::Int128, ScalarLiteral::Int(_))
            | (Self::Principal, ScalarLiteral::Principal(_))
            | (Self::Subaccount, ScalarLiteral::Subaccount(_))
            | (Self::Timestamp, ScalarLiteral::Timestamp(_))
            | (Self::Nat128, ScalarLiteral::Nat(_))
            | (Self::Ulid, ScalarLiteral::Ulid(_))
            | (Self::Unit, ScalarLiteral::Unit(_)) => true,
            (Self::Blob { max_len }, ScalarLiteral::Blob(value)) => {
                max_len.is_none_or(|max| value.len() <= max as usize)
            }
            (Self::Text { max_len }, ScalarLiteral::Text(value)) => {
                max_len.is_none_or(|max| value.chars().count() <= max as usize)
            }
            (Self::Int8, ScalarLiteral::Int(value)) => i8::try_from(*value).is_ok(),
            (Self::Int16, ScalarLiteral::Int(value)) => i16::try_from(*value).is_ok(),
            (Self::Int32, ScalarLiteral::Int(value)) => i32::try_from(*value).is_ok(),
            (Self::Int64, ScalarLiteral::Int(value)) => i64::try_from(*value).is_ok(),
            (Self::IntBig { max_bytes }, ScalarLiteral::IntBig(value)) => {
                value.to_leb128().len() <= max_bytes as usize
            }
            (Self::Nat8, ScalarLiteral::Nat(value)) => u8::try_from(*value).is_ok(),
            (Self::Nat16, ScalarLiteral::Nat(value)) => u16::try_from(*value).is_ok(),
            (Self::Nat32, ScalarLiteral::Nat(value)) => u32::try_from(*value).is_ok(),
            (Self::Nat64, ScalarLiteral::Nat(value)) => u64::try_from(*value).is_ok(),
            (Self::NatBig { max_bytes }, ScalarLiteral::NatBig(value)) => {
                value.to_leb128().len() <= max_bytes as usize
            }
            (Self::Decimal { scale }, ScalarLiteral::Decimal(value)) => {
                decimal_fits_scale(*value, scale)
            }
            _ => false,
        }
    }
}

impl FieldType {
    pub(crate) const fn validate(&self) -> Result<(), SchemaContractError> {
        match self {
            Self::Scalar(scalar) => scalar.validate(),
            Self::Named(_) => Ok(()),
        }
    }
}

/// Insert policy authored for one field.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FieldInsertPolicy {
    /// Omission rejects.
    Required,
    /// Omission resolves to explicit null.
    Nullable,
    /// Omission or explicit database `DEFAULT` resolves to this constant.
    Default(ScalarLiteral),
    /// IcyDB generates the value.
    Generated,
}

/// Accepted database-owned lifecycle policy.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FieldManagementPolicy {
    /// Set once on insert and preserve thereafter.
    CreatedAt,
    /// Set on insert and on each logical row change.
    UpdatedAt,
}

/// One field definition keyed by immutable authorship identity.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FieldFragment {
    source_key: FieldSourceKey,
    name: SchemaName,
    field_type: FieldType,
    nullable: bool,
    insert_policy: FieldInsertPolicy,
    management: Option<FieldManagementPolicy>,
}

impl FieldFragment {
    /// Construct one field definition.
    #[must_use]
    pub const fn new(
        source_key: FieldSourceKey,
        name: SchemaName,
        field_type: FieldType,
        nullable: bool,
        insert_policy: FieldInsertPolicy,
        management: Option<FieldManagementPolicy>,
    ) -> Self {
        Self {
            source_key,
            name,
            field_type,
            nullable,
            insert_policy,
            management,
        }
    }

    /// Borrow the immutable source key.
    #[must_use]
    pub const fn source_key(&self) -> &FieldSourceKey {
        &self.source_key
    }

    /// Borrow the editable field name.
    #[must_use]
    pub const fn name(&self) -> &SchemaName {
        &self.name
    }

    /// Borrow the logical field type.
    #[must_use]
    pub const fn field_type(&self) -> &FieldType {
        &self.field_type
    }

    /// Return whether the field admits authored null.
    #[must_use]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Borrow the future-write insert policy.
    #[must_use]
    pub const fn insert_policy(&self) -> &FieldInsertPolicy {
        &self.insert_policy
    }

    /// Return the optional accepted management policy.
    #[must_use]
    pub const fn management(&self) -> Option<FieldManagementPolicy> {
        self.management
    }

    pub(crate) fn validate(&self) -> Result<(), SchemaContractError> {
        self.field_type.validate()?;
        if let FieldInsertPolicy::Default(literal) = &self.insert_policy {
            literal.validate()?;
            match &self.field_type {
                FieldType::Scalar(scalar) if scalar.accepts_literal(literal) => {}
                FieldType::Named(_) if matches!(literal, ScalarLiteral::EnumUnit { .. }) => {}
                FieldType::Scalar(_) | FieldType::Named(_) => {
                    return Err(SchemaContractError::LiteralTypeMismatch);
                }
            }
        }
        if matches!(self.insert_policy, FieldInsertPolicy::Nullable) && !self.nullable {
            return Err(SchemaContractError::InvalidFieldPolicy);
        }
        if self.management.is_some()
            && (!matches!(self.field_type, FieldType::Scalar(ScalarType::Timestamp))
                || self.nullable
                || !matches!(self.insert_policy, FieldInsertPolicy::Required))
        {
            return Err(SchemaContractError::InvalidFieldPolicy);
        }
        Ok(())
    }
}

/// One ordered index key component.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum IndexKeyFragment {
    /// Direct field key.
    Field(FieldSourceKey),
    /// Lower-cased text expression.
    Lower(FieldSourceKey),
    /// Upper-cased text expression.
    Upper(FieldSourceKey),
    /// Trimmed text expression.
    Trim(FieldSourceKey),
    /// Lower-cased and trimmed text expression.
    LowerTrim(FieldSourceKey),
    /// Date extraction expression.
    Date(FieldSourceKey),
    /// Year extraction expression.
    Year(FieldSourceKey),
    /// Month extraction expression.
    Month(FieldSourceKey),
    /// Day extraction expression.
    Day(FieldSourceKey),
}

impl IndexKeyFragment {
    /// Borrow the field source key consumed by this component.
    #[must_use]
    pub const fn field(&self) -> &FieldSourceKey {
        match self {
            Self::Field(field)
            | Self::Lower(field)
            | Self::Upper(field)
            | Self::Trim(field)
            | Self::LowerTrim(field)
            | Self::Date(field)
            | Self::Year(field)
            | Self::Month(field)
            | Self::Day(field) => field,
        }
    }
}

/// One secondary-index proposal definition.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IndexFragment {
    source_key: IndexSourceKey,
    name: SchemaName,
    key: Vec<IndexKeyFragment>,
    unique: bool,
    predicate: Option<SourceCheckExpr>,
}

impl IndexFragment {
    /// Construct one bounded index definition.
    ///
    /// # Errors
    ///
    /// Returns a typed reference-list error when no key component is present.
    pub fn try_new(
        source_key: IndexSourceKey,
        name: SchemaName,
        key: Vec<IndexKeyFragment>,
        unique: bool,
        predicate: Option<SourceCheckExpr>,
    ) -> Result<Self, SchemaContractError> {
        if key.is_empty() {
            return Err(SchemaContractError::InvalidReferenceList);
        }
        if let Some(predicate) = &predicate {
            predicate.validate()?;
        }
        Ok(Self {
            source_key,
            name,
            key,
            unique,
            predicate,
        })
    }

    /// Borrow the immutable source key.
    #[must_use]
    pub const fn source_key(&self) -> &IndexSourceKey {
        &self.source_key
    }

    /// Borrow the editable index name.
    #[must_use]
    pub const fn name(&self) -> &SchemaName {
        &self.name
    }

    /// Borrow ordered index key components.
    #[must_use]
    pub fn key(&self) -> &[IndexKeyFragment] {
        &self.key
    }

    /// Return whether the index enforces uniqueness.
    #[must_use]
    pub const fn unique(&self) -> bool {
        self.unique
    }

    /// Borrow the optional source predicate.
    #[must_use]
    pub const fn predicate(&self) -> Option<&SourceCheckExpr> {
        self.predicate.as_ref()
    }

    fn validate(&self) -> Result<(), SchemaContractError> {
        Self::try_new(
            self.source_key.clone(),
            self.name.clone(),
            self.key.clone(),
            self.unique,
            self.predicate.clone(),
        )
        .map(|_| ())
    }
}

/// Maintained referential action in proposal version 1.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RelationDeleteAction {
    /// Reject deletion while a source row refers to the target.
    Restrict,
}

/// One source-owned relation definition.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RelationFragment {
    source_key: RelationSourceKey,
    name: SchemaName,
    local_fields: Vec<FieldSourceKey>,
    target_entity: EntitySourceKey,
    target_fields: Vec<FieldSourceKey>,
    on_delete: RelationDeleteAction,
}

impl RelationFragment {
    /// Construct one relation with ordered source/target components.
    ///
    /// # Errors
    ///
    /// Returns a typed reference-list error for empty or arity-mismatched
    /// components.
    pub fn try_new(
        source_key: RelationSourceKey,
        name: SchemaName,
        local_fields: Vec<FieldSourceKey>,
        target_entity: EntitySourceKey,
        target_fields: Vec<FieldSourceKey>,
        on_delete: RelationDeleteAction,
    ) -> Result<Self, SchemaContractError> {
        if local_fields.is_empty() || local_fields.len() != target_fields.len() {
            return Err(SchemaContractError::InvalidReferenceList);
        }
        ensure_unique(&local_fields)?;
        ensure_unique(&target_fields)?;
        Ok(Self {
            source_key,
            name,
            local_fields,
            target_entity,
            target_fields,
            on_delete,
        })
    }

    /// Borrow the immutable source key.
    #[must_use]
    pub const fn source_key(&self) -> &RelationSourceKey {
        &self.source_key
    }

    /// Borrow the editable relation name.
    #[must_use]
    pub const fn name(&self) -> &SchemaName {
        &self.name
    }

    /// Borrow ordered source fields.
    #[must_use]
    pub fn local_fields(&self) -> &[FieldSourceKey] {
        &self.local_fields
    }

    /// Borrow the target entity source key.
    #[must_use]
    pub const fn target_entity(&self) -> &EntitySourceKey {
        &self.target_entity
    }

    /// Borrow ordered target fields.
    #[must_use]
    pub fn target_fields(&self) -> &[FieldSourceKey] {
        &self.target_fields
    }

    /// Return the maintained delete action.
    #[must_use]
    pub const fn on_delete(&self) -> RelationDeleteAction {
        self.on_delete
    }

    fn validate(&self) -> Result<(), SchemaContractError> {
        Self::try_new(
            self.source_key.clone(),
            self.name.clone(),
            self.local_fields.clone(),
            self.target_entity.clone(),
            self.target_fields.clone(),
            self.on_delete,
        )
        .map(|_| ())
    }
}

/// One accepted-check declaration in source-key form.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConstraintFragment {
    source_key: ConstraintSourceKey,
    name: SchemaName,
    expression: SourceCheckExpr,
}

impl ConstraintFragment {
    /// Construct one source constraint.
    #[must_use]
    pub const fn new(
        source_key: ConstraintSourceKey,
        name: SchemaName,
        expression: SourceCheckExpr,
    ) -> Self {
        Self {
            source_key,
            name,
            expression,
        }
    }

    /// Borrow the immutable source key.
    #[must_use]
    pub const fn source_key(&self) -> &ConstraintSourceKey {
        &self.source_key
    }

    /// Borrow the editable constraint name.
    #[must_use]
    pub const fn name(&self) -> &SchemaName {
        &self.name
    }

    /// Borrow the source expression.
    #[must_use]
    pub const fn expression(&self) -> &SourceCheckExpr {
        &self.expression
    }
}

/// Store-free logical entity definition.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EntityFragment {
    source_key: EntitySourceKey,
    name: SchemaName,
    fields: Vec<FieldFragment>,
    primary_key: Vec<FieldSourceKey>,
    indexes: Vec<IndexFragment>,
    relations: Vec<RelationFragment>,
    constraints: Vec<ConstraintFragment>,
}

impl EntityFragment {
    /// Construct and canonicalize one entity definition.
    ///
    /// # Errors
    ///
    /// Returns a typed contract error for collection overflow, duplicate
    /// source keys, malformed policy, expression, or primary-key references.
    pub fn try_new(
        source_key: EntitySourceKey,
        name: SchemaName,
        mut fields: Vec<FieldFragment>,
        primary_key: Vec<FieldSourceKey>,
        mut indexes: Vec<IndexFragment>,
        mut relations: Vec<RelationFragment>,
        mut constraints: Vec<ConstraintFragment>,
    ) -> Result<Self, SchemaContractError> {
        check_len("entity fields", fields.len(), MAX_FRAGMENT_FIELDS)?;
        check_len("entity indexes", indexes.len(), MAX_FRAGMENT_INDEXES)?;
        check_len("entity relations", relations.len(), MAX_FRAGMENT_RELATIONS)?;
        check_len(
            "entity constraints",
            constraints.len(),
            MAX_FRAGMENT_CONSTRAINTS,
        )?;
        if primary_key.is_empty() {
            return Err(SchemaContractError::InvalidReferenceList);
        }
        ensure_unique(&primary_key)?;
        fields.sort_by(|left, right| left.source_key.cmp(&right.source_key));
        indexes.sort_by(|left, right| left.source_key.cmp(&right.source_key));
        relations.sort_by(|left, right| left.source_key.cmp(&right.source_key));
        constraints.sort_by(|left, right| left.source_key.cmp(&right.source_key));
        ensure_unique_sorted_by(&fields, FieldFragment::source_key)?;
        ensure_unique_sorted_by(&indexes, IndexFragment::source_key)?;
        ensure_unique_sorted_by(&relations, RelationFragment::source_key)?;
        ensure_unique_sorted_by(&constraints, ConstraintFragment::source_key)?;
        ensure_unique_names(fields.iter().map(FieldFragment::name))?;
        ensure_unique_names(indexes.iter().map(IndexFragment::name))?;
        ensure_unique_names(relations.iter().map(RelationFragment::name))?;
        ensure_unique_names(constraints.iter().map(ConstraintFragment::name))?;
        for field in &fields {
            field.validate()?;
        }
        validate_management_cardinality(&fields)?;
        for index in &indexes {
            index.validate()?;
        }
        for relation in &relations {
            relation.validate()?;
        }
        for constraint in &constraints {
            constraint.expression.validate()?;
        }
        let field_keys = fields
            .iter()
            .map(|field| field.source_key.clone())
            .collect::<BTreeSet<_>>();
        if primary_key.iter().any(|field| !field_keys.contains(field)) {
            return Err(SchemaContractError::InvalidLocalReference);
        }
        for index in &indexes {
            if index
                .key()
                .iter()
                .any(|component| !field_keys.contains(component.field()))
                || index.predicate().is_some_and(|predicate| {
                    predicate
                        .dependencies()
                        .iter()
                        .any(|field| !field_keys.contains(field))
                })
            {
                return Err(SchemaContractError::InvalidLocalReference);
            }
        }
        for relation in &relations {
            if relation
                .local_fields()
                .iter()
                .any(|field| !field_keys.contains(field))
                || (relation.target_entity() == &source_key
                    && relation
                        .target_fields()
                        .iter()
                        .any(|field| !field_keys.contains(field)))
            {
                return Err(SchemaContractError::InvalidLocalReference);
            }
        }
        for constraint in &constraints {
            if constraint
                .expression()
                .dependencies()
                .iter()
                .any(|field| !field_keys.contains(field))
            {
                return Err(SchemaContractError::InvalidLocalReference);
            }
        }
        Ok(Self {
            source_key,
            name,
            fields,
            primary_key,
            indexes,
            relations,
            constraints,
        })
    }

    /// Borrow the immutable source key.
    #[must_use]
    pub const fn source_key(&self) -> &EntitySourceKey {
        &self.source_key
    }

    /// Borrow the editable entity name.
    #[must_use]
    pub const fn name(&self) -> &SchemaName {
        &self.name
    }

    /// Borrow canonical field definitions.
    #[must_use]
    pub fn fields(&self) -> &[FieldFragment] {
        &self.fields
    }

    /// Borrow ordered primary-key fields.
    #[must_use]
    pub fn primary_key(&self) -> &[FieldSourceKey] {
        &self.primary_key
    }

    /// Borrow canonical secondary-index definitions.
    #[must_use]
    pub fn indexes(&self) -> &[IndexFragment] {
        &self.indexes
    }

    /// Borrow canonical relation definitions.
    #[must_use]
    pub fn relations(&self) -> &[RelationFragment] {
        &self.relations
    }

    /// Borrow canonical accepted-check definitions.
    #[must_use]
    pub fn constraints(&self) -> &[ConstraintFragment] {
        &self.constraints
    }

    pub(crate) fn validate(&self) -> Result<(), SchemaContractError> {
        Self::try_new(
            self.source_key.clone(),
            self.name.clone(),
            self.fields.clone(),
            self.primary_key.clone(),
            self.indexes.clone(),
            self.relations.clone(),
            self.constraints.clone(),
        )
        .map(|_| ())
    }
}

/// One structural field in a named record type.
///
/// Composite fields carry exact type and nullability facts only. Insert,
/// generation, and management policies belong to persisted entity fields.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecordFieldFragment {
    source_key: FieldSourceKey,
    name: SchemaName,
    field_type: FieldType,
    nullable: bool,
}

impl RecordFieldFragment {
    /// Construct one exact structural record field.
    #[must_use]
    pub const fn new(
        source_key: FieldSourceKey,
        name: SchemaName,
        field_type: FieldType,
        nullable: bool,
    ) -> Self {
        Self {
            source_key,
            name,
            field_type,
            nullable,
        }
    }

    /// Borrow the immutable source key.
    #[must_use]
    pub const fn source_key(&self) -> &FieldSourceKey {
        &self.source_key
    }

    /// Borrow the editable field name.
    #[must_use]
    pub const fn name(&self) -> &SchemaName {
        &self.name
    }

    /// Borrow the exact logical field type.
    #[must_use]
    pub const fn field_type(&self) -> &FieldType {
        &self.field_type
    }

    /// Return whether the structural field admits null.
    #[must_use]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    const fn validate(&self) -> Result<(), SchemaContractError> {
        self.field_type.validate()
    }
}

/// Named record-type definition.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecordTypeFragment {
    source_key: TypeSourceKey,
    name: SchemaName,
    fields: Vec<RecordFieldFragment>,
}

impl RecordTypeFragment {
    /// Construct and canonicalize a record definition.
    ///
    /// # Errors
    ///
    /// Returns a typed contract error for overflow, duplicates, or malformed
    /// fields.
    pub fn try_new(
        source_key: TypeSourceKey,
        name: SchemaName,
        mut fields: Vec<RecordFieldFragment>,
    ) -> Result<Self, SchemaContractError> {
        check_len("record fields", fields.len(), MAX_FRAGMENT_FIELDS)?;
        fields.sort_by(|left, right| left.source_key.cmp(&right.source_key));
        ensure_unique_sorted_by(&fields, RecordFieldFragment::source_key)?;
        ensure_unique_names(fields.iter().map(RecordFieldFragment::name))?;
        for field in &fields {
            field.validate()?;
        }
        Ok(Self {
            source_key,
            name,
            fields,
        })
    }

    /// Borrow the immutable source key.
    #[must_use]
    pub const fn source_key(&self) -> &TypeSourceKey {
        &self.source_key
    }

    /// Borrow the editable record name.
    #[must_use]
    pub const fn name(&self) -> &SchemaName {
        &self.name
    }

    /// Borrow canonical record fields.
    #[must_use]
    pub fn fields(&self) -> &[RecordFieldFragment] {
        &self.fields
    }

    fn validate(&self) -> Result<(), SchemaContractError> {
        let rebuilt = Self::try_new(
            self.source_key.clone(),
            self.name.clone(),
            self.fields.clone(),
        )?;
        if rebuilt != *self {
            return Err(SchemaContractError::NonCanonical);
        }
        Ok(())
    }
}

/// One named enum variant.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EnumVariantFragment {
    source_key: TypeSourceKey,
    name: SchemaName,
}

impl EnumVariantFragment {
    /// Construct one variant.
    #[must_use]
    pub const fn new(source_key: TypeSourceKey, name: SchemaName) -> Self {
        Self { source_key, name }
    }

    /// Borrow the immutable variant source key.
    #[must_use]
    pub const fn source_key(&self) -> &TypeSourceKey {
        &self.source_key
    }

    /// Borrow the editable variant name.
    #[must_use]
    pub const fn name(&self) -> &SchemaName {
        &self.name
    }
}

/// Named enum-type definition.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EnumTypeFragment {
    source_key: TypeSourceKey,
    name: SchemaName,
    variants: Vec<EnumVariantFragment>,
}

impl EnumTypeFragment {
    /// Construct and canonicalize an enum definition.
    ///
    /// # Errors
    ///
    /// Returns a typed contract error for empty, oversized, or duplicate
    /// variants.
    pub fn try_new(
        source_key: TypeSourceKey,
        name: SchemaName,
        mut variants: Vec<EnumVariantFragment>,
    ) -> Result<Self, SchemaContractError> {
        if variants.is_empty() {
            return Err(SchemaContractError::InvalidReferenceList);
        }
        check_len("enum variants", variants.len(), MAX_FRAGMENT_FIELDS)?;
        variants.sort_by(|left, right| left.source_key.cmp(&right.source_key));
        ensure_unique_sorted_by(&variants, |variant| &variant.source_key)?;
        ensure_unique_names(variants.iter().map(EnumVariantFragment::name))?;
        Ok(Self {
            source_key,
            name,
            variants,
        })
    }

    /// Borrow the immutable source key.
    #[must_use]
    pub const fn source_key(&self) -> &TypeSourceKey {
        &self.source_key
    }

    /// Borrow the editable enum name.
    #[must_use]
    pub const fn name(&self) -> &SchemaName {
        &self.name
    }

    /// Borrow canonical enum variants.
    #[must_use]
    pub fn variants(&self) -> &[EnumVariantFragment] {
        &self.variants
    }

    fn validate(&self) -> Result<(), SchemaContractError> {
        let rebuilt = Self::try_new(
            self.source_key.clone(),
            self.name.clone(),
            self.variants.clone(),
        )?;
        if rebuilt != *self {
            return Err(SchemaContractError::NonCanonical);
        }
        Ok(())
    }
}

/// Named reusable type definition.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum NamedTypeFragment {
    /// Record type.
    Record(RecordTypeFragment),
    /// Enum type.
    Enum(EnumTypeFragment),
    /// Transparent named wrapper.
    Newtype {
        /// Immutable type identity.
        source_key: TypeSourceKey,
        /// Editable display name.
        name: SchemaName,
        /// Wrapped logical type.
        inner: FieldType,
    },
    /// Homogeneous ordered collection.
    List {
        /// Immutable type identity.
        source_key: TypeSourceKey,
        /// Editable display name.
        name: SchemaName,
        /// Element type.
        item: FieldType,
    },
    /// Homogeneous unique collection.
    Set {
        /// Immutable type identity.
        source_key: TypeSourceKey,
        /// Editable display name.
        name: SchemaName,
        /// Element type.
        item: FieldType,
    },
    /// Homogeneous key/value collection.
    Map {
        /// Immutable type identity.
        source_key: TypeSourceKey,
        /// Editable display name.
        name: SchemaName,
        /// Key type.
        key: FieldType,
        /// Value type.
        value: FieldType,
    },
    /// Ordered heterogeneous product.
    Tuple {
        /// Immutable type identity.
        source_key: TypeSourceKey,
        /// Editable display name.
        name: SchemaName,
        /// Ordered member types.
        members: Vec<FieldType>,
    },
}

impl NamedTypeFragment {
    /// Borrow the immutable type source key.
    #[must_use]
    pub const fn source_key(&self) -> &TypeSourceKey {
        match self {
            Self::Record(record) => record.source_key(),
            Self::Enum(r#enum) => r#enum.source_key(),
            Self::Newtype { source_key, .. }
            | Self::List { source_key, .. }
            | Self::Set { source_key, .. }
            | Self::Map { source_key, .. }
            | Self::Tuple { source_key, .. } => source_key,
        }
    }

    /// Borrow the editable type name.
    #[must_use]
    pub const fn name(&self) -> &SchemaName {
        match self {
            Self::Record(record) => record.name(),
            Self::Enum(r#enum) => r#enum.name(),
            Self::Newtype { name, .. }
            | Self::List { name, .. }
            | Self::Set { name, .. }
            | Self::Map { name, .. }
            | Self::Tuple { name, .. } => name,
        }
    }

    fn validate(&self) -> Result<(), SchemaContractError> {
        match self {
            Self::Record(record) => record.validate(),
            Self::Enum(r#enum) => r#enum.validate(),
            Self::Newtype { inner, .. }
            | Self::List { item: inner, .. }
            | Self::Set { item: inner, .. } => inner.validate(),
            Self::Map { key, value, .. } => {
                key.validate()?;
                value.validate()
            }
            Self::Tuple { members, .. } => {
                if members.is_empty() {
                    return Err(SchemaContractError::InvalidReferenceList);
                }
                check_len("tuple members", members.len(), MAX_FRAGMENT_FIELDS)?;
                members.iter().try_for_each(FieldType::validate)
            }
        }
    }
}

/// Reusable store-free collection of entity and type definitions.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SchemaFragment {
    entities: Vec<EntityFragment>,
    types: Vec<NamedTypeFragment>,
}

impl SchemaFragment {
    /// Construct and canonicalize one reusable fragment.
    ///
    /// # Errors
    ///
    /// Returns a typed contract error for overflow, duplicate definitions, or
    /// malformed nested definitions.
    pub fn try_new(
        mut entities: Vec<EntityFragment>,
        mut types: Vec<NamedTypeFragment>,
    ) -> Result<Self, SchemaContractError> {
        check_len("fragment entities", entities.len(), MAX_FRAGMENT_ENTITIES)?;
        check_len("fragment types", types.len(), MAX_FRAGMENT_TYPES)?;
        entities.sort_by(|left, right| left.source_key.cmp(&right.source_key));
        types.sort_by(|left, right| left.source_key().cmp(right.source_key()));
        ensure_unique_sorted_by(&entities, EntityFragment::source_key)?;
        ensure_unique_sorted_by(&types, NamedTypeFragment::source_key)?;
        ensure_unique_names(entities.iter().map(EntityFragment::name))?;
        ensure_unique_names(types.iter().map(NamedTypeFragment::name))?;
        for entity in &entities {
            entity.validate()?;
        }
        for r#type in &types {
            r#type.validate()?;
        }
        Ok(Self { entities, types })
    }

    /// Borrow entity definitions.
    #[must_use]
    pub fn entities(&self) -> &[EntityFragment] {
        &self.entities
    }

    /// Borrow named type definitions.
    #[must_use]
    pub fn types(&self) -> &[NamedTypeFragment] {
        &self.types
    }

    pub(crate) fn validate(&self) -> Result<(), SchemaContractError> {
        for r#type in &self.types {
            r#type.validate()?;
        }
        let rebuilt = Self::try_new(self.entities.clone(), self.types.clone())?;
        if rebuilt != *self {
            return Err(SchemaContractError::NonCanonical);
        }
        Ok(())
    }
}

pub(crate) const fn check_len(
    kind: &'static str,
    len: usize,
    max: usize,
) -> Result<(), SchemaContractError> {
    if len > max {
        return Err(SchemaContractError::TooManyItems { kind, len, max });
    }
    Ok(())
}

fn ensure_unique<T>(values: &[T]) -> Result<(), SchemaContractError>
where
    T: Ord,
{
    let mut seen = BTreeSet::new();
    if values.iter().any(|value| !seen.insert(value)) {
        return Err(SchemaContractError::InvalidReferenceList);
    }
    Ok(())
}

fn ensure_unique_sorted_by<T, K>(
    values: &[T],
    key: impl Fn(&T) -> &K,
) -> Result<(), SchemaContractError>
where
    K: Eq,
{
    if values.windows(2).any(|pair| key(&pair[0]) == key(&pair[1])) {
        return Err(SchemaContractError::DuplicateSourceKey);
    }
    Ok(())
}

fn ensure_unique_names<'a>(
    names: impl IntoIterator<Item = &'a SchemaName>,
) -> Result<(), SchemaContractError> {
    let mut seen = BTreeSet::new();
    if names.into_iter().any(|name| !seen.insert(name)) {
        return Err(SchemaContractError::DuplicateEditableName);
    }
    Ok(())
}

fn validate_management_cardinality(fields: &[FieldFragment]) -> Result<(), SchemaContractError> {
    for policy in [
        FieldManagementPolicy::CreatedAt,
        FieldManagementPolicy::UpdatedAt,
    ] {
        if fields
            .iter()
            .filter(|field| field.management() == Some(policy))
            .count()
            > 1
        {
            return Err(SchemaContractError::InvalidFieldPolicy);
        }
    }
    Ok(())
}

fn decimal_fits_scale(value: Decimal, scale: u32) -> bool {
    match value.scale().cmp(&scale) {
        std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => value.scale_to_integer(scale).is_some(),
    }
}
