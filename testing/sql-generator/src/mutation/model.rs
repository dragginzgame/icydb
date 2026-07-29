//! Module: sql_generator::mutation::model
//! Responsibility: exact accepted mutation profiles, write intent, rendering, and independent atomic state transitions.
//! Does not own: product parsing, planning, storage, callbacks, or reference-engine execution.
//! Boundary: resolves authored/default intent only from the embedded accepted test snapshot.

use crate::{SqlGeneratorError, SqlGeneratorErrorKind, StructuralSignature};

use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

/// Required pull-request mutation budgets.
pub const TIER_A_MUTATION_BUDGETS: MutationBudgets = MutationBudgets::new(16, 8, 256, 512, 262_144);

/// Required scheduled mutation budgets.
pub const TIER_C_MUTATION_BUDGETS: MutationBudgets =
    MutationBudgets::new(64, 32, 4_096, 8_192, 1_048_576);

///
/// MutationBudgets
///
/// Deterministic bounds for one generated mutation witness and failure artifact.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[expect(
    clippy::struct_field_names,
    reason = "each field is a distinct hard ceiling and the max prefix is contractual"
)]
pub struct MutationBudgets {
    max_fixture_rows: u32,
    max_statements: u32,
    max_shrink_candidates: u32,
    max_evaluations: u32,
    max_artifact_bytes: u32,
}

impl MutationBudgets {
    /// Build explicit deterministic mutation budgets.
    #[must_use]
    pub const fn new(
        max_fixture_rows: u32,
        max_statements: u32,
        max_shrink_candidates: u32,
        max_evaluations: u32,
        max_artifact_bytes: u32,
    ) -> Self {
        Self {
            max_fixture_rows,
            max_statements,
            max_shrink_candidates,
            max_evaluations,
            max_artifact_bytes,
        }
    }

    /// Return the initial fixture-row bound.
    #[must_use]
    pub const fn max_fixture_rows(self) -> u32 {
        self.max_fixture_rows
    }

    /// Return the statement-count bound.
    #[must_use]
    pub const fn max_statements(self) -> u32 {
        self.max_statements
    }

    /// Return the shrink-candidate bound.
    #[must_use]
    pub const fn max_shrink_candidates(self) -> u32 {
        self.max_shrink_candidates
    }

    /// Return the complete-evaluation bound.
    #[must_use]
    pub const fn max_evaluations(self) -> u32 {
        self.max_evaluations
    }

    /// Return the canonical replay byte bound.
    #[must_use]
    pub const fn max_artifact_bytes(self) -> u32 {
        self.max_artifact_bytes
    }

    fn validate(self) -> Result<(), SqlGeneratorError> {
        if self.max_fixture_rows == 0
            || self.max_statements == 0
            || self.max_shrink_candidates == 0
            || self.max_evaluations == 0
            || self.max_artifact_bytes == 0
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "mutation budgets must all be non-zero",
            ));
        }
        Ok(())
    }
}

///
/// MutationSchemaProfile
///
/// The only two accepted test snapshots admitted by current mutation generation.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationSchemaProfile {
    /// Required key plus caller-authored text and unsigned number.
    AuthoredScalar,

    /// Required key/name plus accepted defaults for tier, score, and nullable note.
    AcceptedDefault,
}

impl MutationSchemaProfile {
    /// Borrow the stable accepted-profile identity.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::AuthoredScalar => "authored_scalar",
            Self::AcceptedDefault => "accepted_default",
        }
    }
}

///
/// MutationIngress
///
/// Frozen frontend participation for one scheduled mutation witness.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationIngress {
    /// SQL is the sole required ingress.
    Sql,

    /// SQL and typed structural ingress must share the same modeled transition.
    SqlAndTyped,
}

impl MutationIngress {
    /// Borrow the stable catalog vocabulary.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::SqlAndTyped => "sql_and_typed",
        }
    }
}

///
/// MutationIntentClass
///
/// Primary frozen intent class for one scheduled witness.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationIntentClass {
    /// All supplied values are caller-authored.
    Authored,

    /// At least one insert field explicitly requests its accepted default.
    ExplicitDefault,

    /// One atomic batch contains authored, omitted, and explicit-default fields.
    MixedBatch,

    /// At least one insert field is omitted.
    Omitted,

    /// Unassigned update fields preserve their prior values.
    Preserve,
}

impl MutationIntentClass {
    /// Borrow the stable catalog vocabulary.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Authored => "authored",
            Self::ExplicitDefault => "explicit_default",
            Self::MixedBatch => "mixed_batch",
            Self::Omitted => "omitted",
            Self::Preserve => "preserve",
        }
    }
}

///
/// MutationFieldKind
///
/// Scalar kinds admitted by both exact mutation profiles.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationFieldKind {
    /// UTF-8 text.
    Text,

    /// Exact unsigned 64-bit integer.
    UnsignedInteger,
}

///
/// MutationFieldRole
///
/// Closed semantic field roles across both accepted profiles.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationFieldRole {
    /// Sole primary key.
    Key,

    /// Authored-scalar text field.
    Text,

    /// Authored-scalar unsigned field.
    Number,

    /// Default-aware required name.
    Name,

    /// Default-aware indexed tier.
    Tier,

    /// Default-aware score.
    Score,

    /// Default-aware nullable note.
    Note,
}

///
/// MutationDefaultValue
///
/// Accepted constant default or null policy owned by the test snapshot.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum MutationDefaultValue {
    /// Nullable text defaults to null.
    NullText,

    /// Constant text default.
    Text(String),

    /// Constant unsigned integer default.
    UnsignedInteger(#[serde(with = "crate::model::tagged_u64")] u64),
}

///
/// MutationField
///
/// Exact accepted field policy used by generation, rendering, and reference setup.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutationField {
    id: u32,
    name: String,
    kind: MutationFieldKind,
    role: MutationFieldRole,
    nullable: bool,
    default: Option<MutationDefaultValue>,
    primary_key: bool,
    indexed: bool,
}

impl MutationField {
    #[expect(
        clippy::too_many_arguments,
        reason = "accepted test fields keep every policy fact explicit"
    )]
    fn new(
        id: u32,
        name: &str,
        kind: MutationFieldKind,
        role: MutationFieldRole,
        nullable: bool,
        default: Option<MutationDefaultValue>,
        primary_key: bool,
        indexed: bool,
    ) -> Self {
        Self {
            id,
            name: name.to_string(),
            kind,
            role,
            nullable,
            default,
            primary_key,
            indexed,
        }
    }

    /// Return the accepted field identifier.
    #[must_use]
    pub const fn id(&self) -> u32 {
        self.id
    }

    /// Borrow the accepted field name.
    #[must_use]
    pub const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return the accepted field kind.
    #[must_use]
    pub const fn kind(&self) -> MutationFieldKind {
        self.kind
    }

    /// Return the semantic field role.
    #[must_use]
    pub const fn role(&self) -> MutationFieldRole {
        self.role
    }

    /// Return whether the field is nullable.
    #[must_use]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Borrow the accepted default policy.
    #[must_use]
    pub const fn default(&self) -> Option<&MutationDefaultValue> {
        self.default.as_ref()
    }

    /// Return whether this is the sole primary key.
    #[must_use]
    pub const fn primary_key(&self) -> bool {
        self.primary_key
    }

    /// Return whether this field owns the accepted secondary index.
    #[must_use]
    pub const fn indexed(&self) -> bool {
        self.indexed
    }
}

///
/// MutationSnapshot
///
/// Accepted test-schema authority embedded in every generated mutation witness.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutationSnapshot {
    profile: MutationSchemaProfile,
    fixture_family: String,
    entity_path: String,
    entity_name: String,
    version: u32,
    fields: Vec<MutationField>,
}

impl MutationSnapshot {
    pub(crate) fn for_profile(profile: MutationSchemaProfile) -> Result<Self, SqlGeneratorError> {
        let snapshot = Self::for_profile_unchecked(profile);
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Return the exact accepted profile.
    #[must_use]
    pub const fn profile(&self) -> MutationSchemaProfile {
        self.profile
    }

    /// Borrow the stable fixture-family identity.
    #[must_use]
    pub const fn fixture_family(&self) -> &str {
        self.fixture_family.as_str()
    }

    /// Borrow the accepted entity path.
    #[must_use]
    pub const fn entity_path(&self) -> &str {
        self.entity_path.as_str()
    }

    /// Borrow the accepted SQL entity name.
    #[must_use]
    pub const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Return the accepted schema version.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Borrow accepted fields in canonical field-ID order.
    #[must_use]
    pub const fn fields(&self) -> &[MutationField] {
        self.fields.as_slice()
    }

    /// Borrow the unique accepted field for a semantic role.
    #[must_use]
    pub fn field(&self, role: MutationFieldRole) -> Option<&MutationField> {
        self.fields.iter().find(|field| field.role == role)
    }

    /// Compute the canonical accepted-snapshot fingerprint embedded in replay.
    ///
    /// # Errors
    ///
    /// Returns a typed serialization error when canonical encoding fails.
    pub fn fingerprint(&self) -> Result<String, SqlGeneratorError> {
        let bytes = crate::replay::canonical_json_bytes(self)?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }

    /// Derive the exact active secondary-index entries from complete modeled state.
    ///
    /// # Errors
    ///
    /// Returns a typed error if a row disagrees with this accepted profile.
    pub fn secondary_index_entries(
        &self,
        rows: &[MutationRow],
    ) -> Result<Vec<MutationIndexEntry>, SqlGeneratorError> {
        let Some(indexed) = self.fields.iter().find(|field| field.indexed) else {
            return Ok(Vec::new());
        };
        let mut entries = rows
            .iter()
            .map(|row| {
                row.validate(self.profile)?;
                let value = row.value(indexed.role).ok_or_else(|| {
                    SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        "modeled row lacks the accepted indexed field",
                    )
                })?;
                Ok(MutationIndexEntry {
                    value,
                    key: row.key,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        entries.sort();
        Ok(entries)
    }

    pub(crate) fn validate(&self) -> Result<(), SqlGeneratorError> {
        let expected = Self::for_profile_unchecked(self.profile);
        if self != &expected {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidSnapshot,
                "mutation snapshot differs from its sole current accepted profile",
            ));
        }
        Ok(())
    }

    fn for_profile_unchecked(profile: MutationSchemaProfile) -> Self {
        // This constructor cannot recurse through validation.
        match profile {
            MutationSchemaProfile::AuthoredScalar => Self {
                profile,
                fixture_family: "authored_scalar".to_string(),
                entity_path: "sql_generator::mutation::authored_scalar".to_string(),
                entity_name: "GeneratedAuthoredMutation".to_string(),
                version: 1,
                fields: vec![
                    MutationField::new(
                        1,
                        "id",
                        MutationFieldKind::UnsignedInteger,
                        MutationFieldRole::Key,
                        false,
                        None,
                        true,
                        false,
                    ),
                    MutationField::new(
                        2,
                        "name",
                        MutationFieldKind::Text,
                        MutationFieldRole::Text,
                        false,
                        None,
                        false,
                        false,
                    ),
                    MutationField::new(
                        3,
                        "age",
                        MutationFieldKind::UnsignedInteger,
                        MutationFieldRole::Number,
                        false,
                        None,
                        false,
                        false,
                    ),
                ],
            },
            MutationSchemaProfile::AcceptedDefault => Self {
                profile,
                fixture_family: "accepted_default".to_string(),
                entity_path: "sql_generator::mutation::accepted_default".to_string(),
                entity_name: "GeneratedDefaultMutation".to_string(),
                version: 1,
                fields: vec![
                    MutationField::new(
                        1,
                        "id",
                        MutationFieldKind::UnsignedInteger,
                        MutationFieldRole::Key,
                        false,
                        None,
                        true,
                        false,
                    ),
                    MutationField::new(
                        2,
                        "name",
                        MutationFieldKind::Text,
                        MutationFieldRole::Name,
                        false,
                        None,
                        false,
                        false,
                    ),
                    MutationField::new(
                        3,
                        "tier",
                        MutationFieldKind::Text,
                        MutationFieldRole::Tier,
                        false,
                        Some(MutationDefaultValue::Text("bronze".to_string())),
                        false,
                        true,
                    ),
                    MutationField::new(
                        4,
                        "score",
                        MutationFieldKind::UnsignedInteger,
                        MutationFieldRole::Score,
                        false,
                        Some(MutationDefaultValue::UnsignedInteger(7)),
                        false,
                        false,
                    ),
                    MutationField::new(
                        5,
                        "note",
                        MutationFieldKind::Text,
                        MutationFieldRole::Note,
                        true,
                        Some(MutationDefaultValue::NullText),
                        false,
                        false,
                    ),
                ],
            },
        }
    }

    fn required_field(&self, role: MutationFieldRole) -> Result<&MutationField, SqlGeneratorError> {
        self.field(role).ok_or_else(|| {
            SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidSnapshot,
                "validated mutation snapshot is missing a required field role",
            )
        })
    }

    const fn predicate_text_role(&self) -> MutationFieldRole {
        match self.profile {
            MutationSchemaProfile::AuthoredScalar => MutationFieldRole::Text,
            MutationSchemaProfile::AcceptedDefault => MutationFieldRole::Name,
        }
    }

    const fn predicate_number_role(&self) -> MutationFieldRole {
        match self.profile {
            MutationSchemaProfile::AuthoredScalar => MutationFieldRole::Number,
            MutationSchemaProfile::AcceptedDefault => MutationFieldRole::Score,
        }
    }
}

///
/// MutationValue
///
/// Exact scalar value used by projected rows and secondary-index evidence.
///

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum MutationValue {
    /// SQL NULL.
    Null,

    /// UTF-8 text.
    Text(String),

    /// Exact unsigned integer.
    UnsignedInteger(#[serde(with = "crate::model::tagged_u64")] u64),
}

///
/// MutationIndexEntry
///
/// Canonical logical entry in the accepted default profile's tier index.
///

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutationIndexEntry {
    value: MutationValue,
    #[serde(with = "crate::model::tagged_u64")]
    key: u64,
}

impl MutationIndexEntry {
    /// Build one normalized logical secondary-index entry.
    #[must_use]
    pub const fn new(value: MutationValue, key: u64) -> Self {
        Self { value, key }
    }

    /// Borrow the indexed scalar value.
    #[must_use]
    pub const fn value(&self) -> &MutationValue {
        &self.value
    }

    /// Return the indexed row key.
    #[must_use]
    pub const fn key(&self) -> u64 {
        self.key
    }
}

///
/// MutationRowPayload
///
/// Complete row payload for exactly one accepted profile.
///

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "profile", rename_all = "snake_case")]
pub enum MutationRowPayload {
    /// Authored scalar values.
    AuthoredScalar {
        /// Mutable text.
        text: String,
        /// Mutable unsigned number.
        #[serde(with = "crate::model::tagged_u64")]
        number: u64,
    },

    /// Default-aware complete values after intent resolution.
    AcceptedDefault {
        /// Required name.
        name: String,
        /// Indexed tier.
        tier: String,
        /// Score.
        #[serde(with = "crate::model::tagged_u64")]
        score: u64,
        /// Nullable note.
        note: Option<String>,
    },
}

///
/// MutationRow
///
/// Canonical complete row tracked independently by the state model.
///

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutationRow {
    #[serde(with = "crate::model::tagged_u64")]
    key: u64,
    payload: MutationRowPayload,
}

impl MutationRow {
    /// Build one authored-scalar complete row.
    #[must_use]
    pub fn authored_scalar(key: u64, text: impl Into<String>, number: u64) -> Self {
        Self {
            key,
            payload: MutationRowPayload::AuthoredScalar {
                text: text.into(),
                number,
            },
        }
    }

    /// Build one accepted-default complete row.
    #[must_use]
    pub fn accepted_default(
        key: u64,
        name: impl Into<String>,
        tier: impl Into<String>,
        score: u64,
        note: Option<String>,
    ) -> Self {
        Self {
            key,
            payload: MutationRowPayload::AcceptedDefault {
                name: name.into(),
                tier: tier.into(),
                score,
                note,
            },
        }
    }

    /// Return the primary-key value.
    #[must_use]
    pub const fn key(&self) -> u64 {
        self.key
    }

    /// Borrow the profile-specific complete payload.
    #[must_use]
    pub const fn payload(&self) -> &MutationRowPayload {
        &self.payload
    }

    /// Borrow the authored/default-aware text used by maintained predicates.
    #[must_use]
    pub const fn predicate_text(&self) -> &str {
        match &self.payload {
            MutationRowPayload::AuthoredScalar { text, .. } => text.as_str(),
            MutationRowPayload::AcceptedDefault { name, .. } => name.as_str(),
        }
    }

    /// Return the authored/default-aware unsigned value used by maintained predicates.
    #[must_use]
    pub const fn predicate_number(&self) -> u64 {
        match self.payload {
            MutationRowPayload::AuthoredScalar { number, .. } => number,
            MutationRowPayload::AcceptedDefault { score, .. } => score,
        }
    }

    /// Borrow the default-aware tier, when present.
    #[must_use]
    pub const fn tier(&self) -> Option<&str> {
        match &self.payload {
            MutationRowPayload::AcceptedDefault { tier, .. } => Some(tier.as_str()),
            MutationRowPayload::AuthoredScalar { .. } => None,
        }
    }

    /// Return the default-aware score, when present.
    #[must_use]
    pub const fn score(&self) -> Option<u64> {
        match self.payload {
            MutationRowPayload::AcceptedDefault { score, .. } => Some(score),
            MutationRowPayload::AuthoredScalar { .. } => None,
        }
    }

    /// Borrow the default-aware nullable note. The outer option denotes profile membership.
    #[must_use]
    pub fn note(&self) -> Option<Option<&str>> {
        match &self.payload {
            MutationRowPayload::AcceptedDefault { note, .. } => Some(note.as_deref()),
            MutationRowPayload::AuthoredScalar { .. } => None,
        }
    }

    const fn profile(&self) -> MutationSchemaProfile {
        match self.payload {
            MutationRowPayload::AuthoredScalar { .. } => MutationSchemaProfile::AuthoredScalar,
            MutationRowPayload::AcceptedDefault { .. } => MutationSchemaProfile::AcceptedDefault,
        }
    }

    fn validate(&self, profile: MutationSchemaProfile) -> Result<(), SqlGeneratorError> {
        if self.profile() != profile {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "modeled row payload disagrees with the accepted mutation profile",
            ));
        }
        Ok(())
    }

    fn value(&self, role: MutationFieldRole) -> Option<MutationValue> {
        match (&self.payload, role) {
            (_, MutationFieldRole::Key) => Some(MutationValue::UnsignedInteger(self.key)),
            (MutationRowPayload::AuthoredScalar { text, .. }, MutationFieldRole::Text) => {
                Some(MutationValue::Text(text.clone()))
            }
            (MutationRowPayload::AuthoredScalar { number, .. }, MutationFieldRole::Number) => {
                Some(MutationValue::UnsignedInteger(*number))
            }
            (MutationRowPayload::AcceptedDefault { name, .. }, MutationFieldRole::Name) => {
                Some(MutationValue::Text(name.clone()))
            }
            (MutationRowPayload::AcceptedDefault { tier, .. }, MutationFieldRole::Tier) => {
                Some(MutationValue::Text(tier.clone()))
            }
            (MutationRowPayload::AcceptedDefault { score, .. }, MutationFieldRole::Score) => {
                Some(MutationValue::UnsignedInteger(*score))
            }
            (MutationRowPayload::AcceptedDefault { note, .. }, MutationFieldRole::Note) => {
                Some(note.as_ref().map_or(MutationValue::Null, |note| {
                    MutationValue::Text(note.clone())
                }))
            }
            _ => None,
        }
    }
}

///
/// MutationWriteIntent
///
/// Exact insert authorship before accepted default resolution.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "intent", content = "value", rename_all = "snake_case")]
pub enum MutationWriteIntent<T> {
    /// Caller supplied a concrete value.
    Authored(T),

    /// Field was absent from the authored insert shape.
    Omitted,

    /// SQL explicitly requested `DEFAULT`.
    Default,
}

///
/// MutationUpdateIntent
///
/// Exact update assignment intent before accepted default resolution.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "intent", content = "value", rename_all = "snake_case")]
pub enum MutationUpdateIntent<T> {
    /// Caller supplied a replacement value.
    Authored(T),

    /// SQL explicitly requested `DEFAULT`.
    Default,

    /// Field was not assigned and must retain its prior value.
    Preserve,
}

///
/// MutationIntentKind
///
/// Per-field provenance emitted by the typed mutation AST.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationIntentKind {
    /// Concrete caller-authored value.
    Authored,

    /// Insert field omission.
    Omitted,

    /// Explicit insert `DEFAULT`.
    InsertDefault,

    /// Explicit update `DEFAULT`.
    UpdateDefault,

    /// Absent update assignment.
    Preserve,
}

impl MutationIntentKind {
    /// Borrow the stable distribution label.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Authored => "authored",
            Self::Omitted => "omitted",
            Self::InsertDefault => "insert_default",
            Self::UpdateDefault => "update_default",
            Self::Preserve => "preserve",
        }
    }
}

///
/// MutationInsertRow
///
/// One typed insert row before accepted policy resolution.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "profile", rename_all = "snake_case")]
pub enum MutationInsertRow {
    /// Complete authored-scalar row.
    AuthoredScalar {
        /// Primary key.
        #[serde(with = "crate::model::tagged_u64")]
        key: u64,
        /// Text value.
        text: String,
        /// Unsigned value.
        #[serde(with = "crate::model::tagged_u64")]
        number: u64,
    },

    /// Default-aware row with exact per-field source intent.
    AcceptedDefault {
        /// Required key intent.
        key: MutationWriteIntent<u64>,
        /// Required name intent.
        name: MutationWriteIntent<String>,
        /// Tier intent.
        tier: MutationWriteIntent<String>,
        /// Score intent.
        score: MutationWriteIntent<u64>,
        /// Nullable note intent.
        note: MutationWriteIntent<Option<String>>,
    },
}

impl MutationInsertRow {
    /// Build one complete authored-scalar insert row.
    #[must_use]
    pub fn authored_scalar(key: u64, text: impl Into<String>, number: u64) -> Self {
        Self::AuthoredScalar {
            key,
            text: text.into(),
            number,
        }
    }

    /// Build one default-aware row with explicit source intent.
    #[must_use]
    pub const fn accepted_default(
        key: MutationWriteIntent<u64>,
        name: MutationWriteIntent<String>,
        tier: MutationWriteIntent<String>,
        score: MutationWriteIntent<u64>,
        note: MutationWriteIntent<Option<String>>,
    ) -> Self {
        Self::AcceptedDefault {
            key,
            name,
            tier,
            score,
            note,
        }
    }

    const fn profile(&self) -> MutationSchemaProfile {
        match self {
            Self::AuthoredScalar { .. } => MutationSchemaProfile::AuthoredScalar,
            Self::AcceptedDefault { .. } => MutationSchemaProfile::AcceptedDefault,
        }
    }

    fn intents(&self) -> Vec<MutationIntentKind> {
        match self {
            Self::AuthoredScalar { .. } => vec![MutationIntentKind::Authored; 3],
            Self::AcceptedDefault {
                key,
                name,
                tier,
                score,
                note,
            } => [
                intent_kind(key),
                intent_kind(name),
                intent_kind(tier),
                intent_kind(score),
                intent_kind(note),
            ]
            .into_iter()
            .collect(),
        }
    }
}

const fn intent_kind<T>(intent: &MutationWriteIntent<T>) -> MutationIntentKind {
    match intent {
        MutationWriteIntent::Authored(_) => MutationIntentKind::Authored,
        MutationWriteIntent::Omitted => MutationIntentKind::Omitted,
        MutationWriteIntent::Default => MutationIntentKind::InsertDefault,
    }
}

///
/// MutationPredicate
///
/// Typed predicate subset independently evaluated by the state model.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MutationPredicate {
    /// Match every row.
    All,

    /// Require both nested predicates.
    And {
        /// Left predicate.
        left: Box<Self>,
        /// Right predicate.
        right: Box<Self>,
    },

    /// Match one primary key.
    KeyEqual {
        /// Required key.
        #[serde(with = "crate::model::tagged_u64")]
        value: u64,
    },

    /// Match a half-open unsigned range.
    NumberRange {
        /// Inclusive lower bound.
        #[serde(with = "crate::model::tagged_u64")]
        min_inclusive: u64,
        /// Exclusive upper bound.
        #[serde(with = "crate::model::tagged_u64")]
        max_exclusive: u64,
    },

    /// Match the profile's maintained text field.
    TextEqual {
        /// Exact text.
        value: String,
    },
}

impl MutationPredicate {
    fn matches(&self, row: &MutationRow) -> bool {
        match self {
            Self::All => true,
            Self::And { left, right } => left.matches(row) && right.matches(row),
            Self::KeyEqual { value } => row.key == *value,
            Self::NumberRange {
                min_inclusive,
                max_exclusive,
            } => {
                let number = row.predicate_number();
                number >= *min_inclusive && number < *max_exclusive
            }
            Self::TextEqual { value } => row.predicate_text() == value,
        }
    }

    fn validate(&self) -> Result<(), SqlGeneratorError> {
        match self {
            Self::And { left, right } => {
                left.validate()?;
                right.validate()
            }
            Self::NumberRange {
                min_inclusive,
                max_exclusive,
            } if min_inclusive >= max_exclusive => Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "mutation numeric range must be non-empty",
            )),
            Self::All
            | Self::KeyEqual { .. }
            | Self::NumberRange { .. }
            | Self::TextEqual { .. } => Ok(()),
        }
    }

    fn render(&self, snapshot: &MutationSnapshot) -> Result<String, SqlGeneratorError> {
        match self {
            Self::All => Ok("1 = 1".to_string()),
            Self::And { left, right } => Ok(format!(
                "({}) AND ({})",
                left.render(snapshot)?,
                right.render(snapshot)?
            )),
            Self::KeyEqual { value } => Ok(format!(
                "{} = {value}",
                snapshot.required_field(MutationFieldRole::Key)?.name()
            )),
            Self::NumberRange {
                min_inclusive,
                max_exclusive,
            } => {
                let field = snapshot
                    .required_field(snapshot.predicate_number_role())?
                    .name();
                Ok(format!(
                    "{field} >= {min_inclusive} AND {field} < {max_exclusive}"
                ))
            }
            Self::TextEqual { value } => Ok(format!(
                "{} = '{}'",
                snapshot
                    .required_field(snapshot.predicate_text_role())?
                    .name(),
                quote_text(value)
            )),
        }
    }
}

///
/// MutationAssignment
///
/// Profile-typed update assignments with explicit default and preserve provenance.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "profile", rename_all = "snake_case")]
pub enum MutationAssignment {
    /// Replace the authored numeric field.
    AuthoredNumber {
        /// New value.
        #[serde(with = "crate::model::tagged_u64")]
        value: u64,
    },

    /// Replace the authored text field.
    AuthoredText {
        /// New value.
        value: String,
    },

    /// Replace both authored mutable fields.
    AuthoredTextAndNumber {
        /// New text.
        text: String,
        /// New number.
        #[serde(with = "crate::model::tagged_u64")]
        number: u64,
    },

    /// Default-aware update over every non-key field.
    AcceptedDefault {
        /// Name intent.
        name: MutationUpdateIntent<String>,
        /// Tier intent.
        tier: MutationUpdateIntent<String>,
        /// Score intent.
        score: MutationUpdateIntent<u64>,
        /// Nullable note intent.
        note: MutationUpdateIntent<Option<String>>,
    },
}

impl MutationAssignment {
    const fn profile(&self) -> MutationSchemaProfile {
        match self {
            Self::AuthoredNumber { .. }
            | Self::AuthoredText { .. }
            | Self::AuthoredTextAndNumber { .. } => MutationSchemaProfile::AuthoredScalar,
            Self::AcceptedDefault { .. } => MutationSchemaProfile::AcceptedDefault,
        }
    }

    fn intents(&self) -> Vec<MutationIntentKind> {
        match self {
            Self::AuthoredNumber { .. } | Self::AuthoredText { .. } => {
                vec![MutationIntentKind::Authored]
            }
            Self::AuthoredTextAndNumber { .. } => vec![MutationIntentKind::Authored; 2],
            Self::AcceptedDefault {
                name,
                tier,
                score,
                note,
            } => [
                update_intent_kind(name),
                update_intent_kind(tier),
                update_intent_kind(score),
                update_intent_kind(note),
            ]
            .into_iter()
            .collect(),
        }
    }

    fn apply(
        &self,
        snapshot: &MutationSnapshot,
        row: &mut MutationRow,
    ) -> Result<(), SqlGeneratorError> {
        if self.profile() != snapshot.profile || row.profile() != snapshot.profile {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "mutation assignment profile disagrees with accepted row state",
            ));
        }
        match (self, &mut row.payload) {
            (Self::AuthoredNumber { value }, MutationRowPayload::AuthoredScalar { number, .. }) => {
                *number = *value;
            }
            (Self::AuthoredText { value }, MutationRowPayload::AuthoredScalar { text, .. }) => {
                text.clone_from(value);
            }
            (
                Self::AuthoredTextAndNumber {
                    text: value,
                    number: replacement,
                },
                MutationRowPayload::AuthoredScalar { text, number },
            ) => {
                text.clone_from(value);
                *number = *replacement;
            }
            (
                Self::AcceptedDefault {
                    name: name_intent,
                    tier: tier_intent,
                    score: score_intent,
                    note: note_intent,
                },
                MutationRowPayload::AcceptedDefault {
                    name,
                    tier,
                    score,
                    note,
                },
            ) => {
                apply_text_update(
                    name,
                    name_intent,
                    snapshot.required_field(MutationFieldRole::Name)?,
                )?;
                apply_text_update(
                    tier,
                    tier_intent,
                    snapshot.required_field(MutationFieldRole::Tier)?,
                )?;
                apply_unsigned_update(
                    score,
                    score_intent,
                    snapshot.required_field(MutationFieldRole::Score)?,
                )?;
                apply_nullable_text_update(
                    note,
                    note_intent,
                    snapshot.required_field(MutationFieldRole::Note)?,
                )?;
            }
            _ => {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidCase,
                    "mutation assignment and row payload variants disagree",
                ));
            }
        }
        Ok(())
    }

    fn render(&self, snapshot: &MutationSnapshot) -> Result<String, SqlGeneratorError> {
        match self {
            Self::AuthoredNumber { value } => Ok(format!(
                "{} = {value}",
                snapshot.required_field(MutationFieldRole::Number)?.name()
            )),
            Self::AuthoredText { value } => Ok(format!(
                "{} = '{}'",
                snapshot.required_field(MutationFieldRole::Text)?.name(),
                quote_text(value)
            )),
            Self::AuthoredTextAndNumber { text, number } => Ok(format!(
                "{} = '{}', {} = {number}",
                snapshot.required_field(MutationFieldRole::Text)?.name(),
                quote_text(text),
                snapshot.required_field(MutationFieldRole::Number)?.name(),
            )),
            Self::AcceptedDefault {
                name,
                tier,
                score,
                note,
            } => {
                let assignments = [
                    render_text_update(snapshot.required_field(MutationFieldRole::Name)?, name),
                    render_text_update(snapshot.required_field(MutationFieldRole::Tier)?, tier),
                    render_unsigned_update(
                        snapshot.required_field(MutationFieldRole::Score)?,
                        score,
                    ),
                    render_nullable_text_update(
                        snapshot.required_field(MutationFieldRole::Note)?,
                        note,
                    ),
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
                if assignments.is_empty() {
                    return Err(SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        "default-aware UPDATE must assign at least one field",
                    ));
                }
                Ok(assignments.join(", "))
            }
        }
    }
}

const fn update_intent_kind<T>(intent: &MutationUpdateIntent<T>) -> MutationIntentKind {
    match intent {
        MutationUpdateIntent::Authored(_) => MutationIntentKind::Authored,
        MutationUpdateIntent::Default => MutationIntentKind::UpdateDefault,
        MutationUpdateIntent::Preserve => MutationIntentKind::Preserve,
    }
}

///
/// MutationOrder
///
/// Deterministic primary-key order for bounded mutations.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationOrder {
    /// Ascending key.
    KeyAscending,

    /// Descending key.
    KeyDescending,
}

///
/// MutationInsertQueryKeySource
///
/// Accepted authored-scalar source projected into an insert key.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationInsertQueryKeySource {
    /// Source primary key.
    Key,

    /// Source unsigned number.
    Number,
}

///
/// MutationWindow
///
/// Optional deterministic mutation selection window.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutationWindow {
    order: MutationOrder,
    limit: u32,
    offset: u32,
}

impl MutationWindow {
    /// Build a non-zero ordered mutation window.
    ///
    /// # Errors
    ///
    /// Returns a typed error when `limit` is zero.
    pub fn try_new(
        order: MutationOrder,
        limit: u32,
        offset: u32,
    ) -> Result<Self, SqlGeneratorError> {
        if limit == 0 {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "mutation window LIMIT must be non-zero",
            ));
        }
        Ok(Self {
            order,
            limit,
            offset,
        })
    }

    /// Return deterministic key order.
    #[must_use]
    pub const fn order(self) -> MutationOrder {
        self.order
    }

    /// Return the limit.
    #[must_use]
    pub const fn limit(self) -> u32 {
        self.limit
    }

    /// Return the offset.
    #[must_use]
    pub const fn offset(self) -> u32 {
        self.offset
    }
}

///
/// MutationReturning
///
/// Exact maintained RETURNING projection.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "fields", rename_all = "snake_case")]
pub enum MutationReturning {
    /// No returned rows.
    None,

    /// Every accepted field in field-ID order.
    AllFields,

    /// One exact non-empty role list.
    Fields(Vec<MutationFieldRole>),
}

impl MutationReturning {
    /// Resolve the accepted field roles in exact result-column order.
    ///
    /// # Errors
    ///
    /// Returns a typed error when a named projection is empty, duplicated, or
    /// absent from the accepted snapshot.
    pub fn field_roles(
        &self,
        snapshot: &MutationSnapshot,
    ) -> Result<Vec<MutationFieldRole>, SqlGeneratorError> {
        match self {
            Self::None => Ok(Vec::new()),
            Self::AllFields => Ok(snapshot.fields.iter().map(MutationField::role).collect()),
            Self::Fields(roles) => {
                if roles.is_empty()
                    || roles.iter().any(|role| snapshot.field(*role).is_none())
                    || roles.iter().copied().collect::<BTreeSet<_>>().len() != roles.len()
                {
                    return Err(SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        "mutation RETURNING fields must be non-empty, unique, and accepted",
                    ));
                }
                Ok(roles.clone())
            }
        }
    }

    /// Return whether the statement emits rows.
    #[must_use]
    pub const fn is_returning(&self) -> bool {
        !matches!(self, Self::None)
    }
}

///
/// MutationOperation
///
/// Typed insert/update/delete operation.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "verb", rename_all = "snake_case")]
pub enum MutationOperation {
    /// Delete selected rows.
    Delete {
        /// Predicate.
        predicate: MutationPredicate,
        /// Optional ordered window.
        window: Option<MutationWindow>,
    },

    /// Insert one or more intent-bearing rows atomically.
    Insert {
        /// Authored rows in statement order.
        rows: Vec<MutationInsertRow>,
    },

    /// Insert authored-scalar rows selected from current state.
    InsertFromQuery {
        /// Source predicate.
        predicate: MutationPredicate,
        /// Source projected into the target key.
        key_source: MutationInsertQueryKeySource,
    },

    /// Update selected rows.
    Update {
        /// Predicate.
        predicate: MutationPredicate,
        /// Assignment.
        assignment: MutationAssignment,
        /// Optional ordered window.
        window: Option<MutationWindow>,
    },
}

impl MutationOperation {
    fn validate(&self, snapshot: &MutationSnapshot) -> Result<(), SqlGeneratorError> {
        match self {
            Self::Insert { rows } => {
                if rows.is_empty() || rows.iter().any(|row| row.profile() != snapshot.profile) {
                    return Err(SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        "mutation INSERT rows must be non-empty and match the accepted profile",
                    ));
                }
                Ok(())
            }
            Self::InsertFromQuery { predicate, .. } => {
                if snapshot.profile != MutationSchemaProfile::AuthoredScalar {
                    return Err(SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        "INSERT FROM QUERY belongs only to the authored-scalar profile",
                    ));
                }
                predicate.validate()
            }
            Self::Update {
                predicate,
                assignment,
                ..
            } => {
                if assignment.profile() != snapshot.profile {
                    return Err(SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        "mutation UPDATE assignment disagrees with accepted profile",
                    ));
                }
                predicate.validate()
            }
            Self::Delete { predicate, .. } => predicate.validate(),
        }
    }

    /// Return whether this operation is INSERT.
    #[must_use]
    pub const fn is_insert(&self) -> bool {
        matches!(self, Self::Insert { .. } | Self::InsertFromQuery { .. })
    }

    /// Return whether this operation is UPDATE.
    #[must_use]
    pub const fn is_update(&self) -> bool {
        matches!(self, Self::Update { .. })
    }

    /// Return whether this operation is DELETE.
    #[must_use]
    pub const fn is_delete(&self) -> bool {
        matches!(self, Self::Delete { .. })
    }

    const fn window(&self) -> Option<MutationWindow> {
        match self {
            Self::Delete { window, .. } | Self::Update { window, .. } => *window,
            Self::Insert { .. } | Self::InsertFromQuery { .. } => None,
        }
    }

    fn intents(&self) -> Vec<MutationIntentKind> {
        match self {
            Self::Insert { rows } => rows.iter().flat_map(MutationInsertRow::intents).collect(),
            Self::Update { assignment, .. } => assignment.intents(),
            Self::Delete { .. } | Self::InsertFromQuery { .. } => Vec::new(),
        }
    }
}

///
/// MutationStatement
///
/// One typed DML statement with exact RETURNING and source intent.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutationStatement {
    operation: MutationOperation,
    returning: MutationReturning,
}

impl MutationStatement {
    /// Build one typed mutation statement.
    #[must_use]
    pub const fn new(operation: MutationOperation, returning: MutationReturning) -> Self {
        Self {
            operation,
            returning,
        }
    }

    /// Borrow the operation.
    #[must_use]
    pub const fn operation(&self) -> &MutationOperation {
        &self.operation
    }

    /// Borrow the exact RETURNING projection.
    #[must_use]
    pub const fn returning(&self) -> &MutationReturning {
        &self.returning
    }

    /// Return per-field write intent in authored row/field order.
    #[must_use]
    pub fn intent_provenance(&self) -> Vec<MutationIntentKind> {
        self.operation.intents()
    }

    /// Render current IcyDB SQL from accepted field names and typed intent.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the statement disagrees with its accepted snapshot.
    pub fn render(&self, snapshot: &MutationSnapshot) -> Result<String, SqlGeneratorError> {
        snapshot.validate()?;
        self.operation.validate(snapshot)?;
        let entity = snapshot.entity_name();
        let key = snapshot.required_field(MutationFieldRole::Key)?.name();
        let mut sql = match &self.operation {
            MutationOperation::Delete { predicate, .. } => {
                format!("DELETE FROM {entity} WHERE {}", predicate.render(snapshot)?)
            }
            MutationOperation::Insert { rows } => render_insert(snapshot, rows)?,
            MutationOperation::InsertFromQuery {
                predicate,
                key_source,
            } => {
                let text = snapshot.required_field(MutationFieldRole::Text)?.name();
                let number = snapshot.required_field(MutationFieldRole::Number)?.name();
                let source_key = match key_source {
                    MutationInsertQueryKeySource::Key => key,
                    MutationInsertQueryKeySource::Number => number,
                };
                format!(
                    "INSERT INTO {entity} ({key}, {text}, {number}) SELECT {source_key}, {text}, {number} FROM {entity} WHERE {} ORDER BY {key} ASC",
                    predicate.render(snapshot)?
                )
            }
            MutationOperation::Update {
                predicate,
                assignment,
                ..
            } => format!(
                "UPDATE {entity} SET {} WHERE {}",
                assignment.render(snapshot)?,
                predicate.render(snapshot)?
            ),
        };
        if let Some(window) = self.operation.window() {
            let direction = match window.order {
                MutationOrder::KeyAscending => "ASC",
                MutationOrder::KeyDescending => "DESC",
            };
            write!(
                &mut sql,
                " ORDER BY {key} {direction} LIMIT {}",
                window.limit
            )
            .map_err(|_| {
                SqlGeneratorError::new(
                    SqlGeneratorErrorKind::Rendering,
                    "failed to append the mutation window",
                )
            })?;
            if window.offset > 0 {
                write!(&mut sql, " OFFSET {}", window.offset).map_err(|_| {
                    SqlGeneratorError::new(
                        SqlGeneratorErrorKind::Rendering,
                        "failed to append the mutation offset",
                    )
                })?;
            }
        }
        let returning_roles = self.returning.field_roles(snapshot)?;
        if !returning_roles.is_empty() {
            let names = returning_roles
                .iter()
                .map(|role| snapshot.required_field(*role).map(MutationField::name))
                .collect::<Result<Vec<_>, _>>()?;
            write!(&mut sql, " RETURNING {}", names.join(", ")).map_err(|_| {
                SqlGeneratorError::new(
                    SqlGeneratorErrorKind::Rendering,
                    "failed to append mutation RETURNING",
                )
            })?;
        }
        Ok(sql)
    }
}

fn render_insert(
    snapshot: &MutationSnapshot,
    rows: &[MutationInsertRow],
) -> Result<String, SqlGeneratorError> {
    let roles = match snapshot.profile {
        MutationSchemaProfile::AuthoredScalar => vec![
            MutationFieldRole::Key,
            MutationFieldRole::Text,
            MutationFieldRole::Number,
        ],
        MutationSchemaProfile::AcceptedDefault => vec![
            MutationFieldRole::Key,
            MutationFieldRole::Name,
            MutationFieldRole::Tier,
            MutationFieldRole::Score,
            MutationFieldRole::Note,
        ],
    };
    let included = roles
        .iter()
        .copied()
        .filter(|role| rows.iter().any(|row| !insert_role_is_omitted(row, *role)))
        .collect::<Vec<_>>();
    let columns = included
        .iter()
        .map(|role| snapshot.required_field(*role).map(MutationField::name))
        .collect::<Result<Vec<_>, _>>()?;
    let values = rows
        .iter()
        .map(|row| {
            included
                .iter()
                .map(|role| render_insert_role(row, *role))
                .collect::<Result<Vec<_>, _>>()
                .map(|values| format!("({})", values.join(", ")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!(
        "INSERT INTO {} ({}) VALUES {}",
        snapshot.entity_name(),
        columns.join(", "),
        values.join(", "),
    ))
}

const fn insert_role_is_omitted(row: &MutationInsertRow, role: MutationFieldRole) -> bool {
    match (row, role) {
        (
            MutationInsertRow::AcceptedDefault {
                key,
                name,
                tier,
                score,
                note,
            },
            role,
        ) => match role {
            MutationFieldRole::Key => matches!(key, MutationWriteIntent::Omitted),
            MutationFieldRole::Name => matches!(name, MutationWriteIntent::Omitted),
            MutationFieldRole::Tier => matches!(tier, MutationWriteIntent::Omitted),
            MutationFieldRole::Score => matches!(score, MutationWriteIntent::Omitted),
            MutationFieldRole::Note => matches!(note, MutationWriteIntent::Omitted),
            MutationFieldRole::Text | MutationFieldRole::Number => false,
        },
        (MutationInsertRow::AuthoredScalar { .. }, _) => false,
    }
}

fn render_insert_role(
    row: &MutationInsertRow,
    role: MutationFieldRole,
) -> Result<String, SqlGeneratorError> {
    match (row, role) {
        (MutationInsertRow::AuthoredScalar { key, .. }, MutationFieldRole::Key) => {
            Ok(key.to_string())
        }
        (MutationInsertRow::AuthoredScalar { text, .. }, MutationFieldRole::Text) => {
            Ok(format!("'{}'", quote_text(text)))
        }
        (MutationInsertRow::AuthoredScalar { number, .. }, MutationFieldRole::Number) => {
            Ok(number.to_string())
        }
        (
            MutationInsertRow::AcceptedDefault {
                key,
                name,
                tier,
                score,
                note,
            },
            role,
        ) => match role {
            MutationFieldRole::Key => Ok(render_unsigned_write(key)),
            MutationFieldRole::Name => Ok(render_text_write(name)),
            MutationFieldRole::Tier => Ok(render_text_write(tier)),
            MutationFieldRole::Score => Ok(render_unsigned_write(score)),
            MutationFieldRole::Note => Ok(render_nullable_text_write(note)),
            MutationFieldRole::Text | MutationFieldRole::Number => Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "default-aware insert reached an authored-scalar field role",
            )),
        },
        _ => Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "insert row and accepted field role disagree",
        )),
    }
}

fn render_unsigned_write(intent: &MutationWriteIntent<u64>) -> String {
    match intent {
        MutationWriteIntent::Authored(value) => value.to_string(),
        MutationWriteIntent::Omitted | MutationWriteIntent::Default => "DEFAULT".to_string(),
    }
}

fn render_text_write(intent: &MutationWriteIntent<String>) -> String {
    match intent {
        MutationWriteIntent::Authored(value) => format!("'{}'", quote_text(value)),
        MutationWriteIntent::Omitted | MutationWriteIntent::Default => "DEFAULT".to_string(),
    }
}

fn render_nullable_text_write(intent: &MutationWriteIntent<Option<String>>) -> String {
    match intent {
        MutationWriteIntent::Authored(Some(value)) => format!("'{}'", quote_text(value)),
        MutationWriteIntent::Authored(None) => "NULL".to_string(),
        MutationWriteIntent::Omitted | MutationWriteIntent::Default => "DEFAULT".to_string(),
    }
}

///
/// MutationProjectedField
///
/// One role-bound value in a normalized RETURNING row.
///

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutationProjectedField {
    role: MutationFieldRole,
    value: MutationValue,
}

impl MutationProjectedField {
    /// Build one normalized role-bound value.
    #[must_use]
    pub const fn new(role: MutationFieldRole, value: MutationValue) -> Self {
        Self { role, value }
    }

    /// Return the projected accepted role.
    #[must_use]
    pub const fn role(&self) -> MutationFieldRole {
        self.role
    }

    /// Borrow the projected value.
    #[must_use]
    pub const fn value(&self) -> &MutationValue {
        &self.value
    }
}

///
/// MutationProjectedRow
///
/// One normalized row containing exactly the requested RETURNING roles.
///

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutationProjectedRow {
    fields: Vec<MutationProjectedField>,
}

impl MutationProjectedRow {
    /// Build one normalized row in exact result-column order.
    #[must_use]
    pub const fn new(fields: Vec<MutationProjectedField>) -> Self {
        Self { fields }
    }

    /// Borrow projected fields in declaration order.
    #[must_use]
    pub const fn fields(&self) -> &[MutationProjectedField] {
        self.fields.as_slice()
    }
}

///
/// MutationExpectedRejection
///
/// Stable independent-model rejection class.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationExpectedRejection {
    /// Explicit `DEFAULT` targeted a required field without an accepted default.
    DefaultUnavailable,

    /// Insert primary key collided with existing or same-batch state.
    DuplicatePrimaryKey,

    /// A required field was omitted.
    MissingRequiredField,
}

impl MutationExpectedRejection {
    /// Borrow the stable structural error identity.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::DefaultUnavailable => "default_unavailable",
            Self::DuplicatePrimaryKey => "duplicate_primary_key",
            Self::MissingRequiredField => "missing_required_field",
        }
    }
}

///
/// MutationStepOutcome
///
/// Independent atomic outcome including complete post-state.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum MutationStepOutcome {
    /// Statement committed completely.
    Accepted {
        /// Affected rows.
        affected_rows: u32,
        /// Exact normalized RETURNING projection.
        returned_rows: Vec<MutationProjectedRow>,
        /// Complete canonical state.
        state_after: Vec<MutationRow>,
        /// Complete logical secondary-index state derived independently.
        index_after: Vec<MutationIndexEntry>,
    },

    /// Statement rejected with unchanged state.
    Rejected {
        /// Stable typed rejection.
        rejection: MutationExpectedRejection,
        /// Complete unchanged state.
        state_after: Vec<MutationRow>,
        /// Complete unchanged logical secondary-index state.
        index_after: Vec<MutationIndexEntry>,
    },
}

impl MutationStepOutcome {
    /// Borrow the complete canonical post-state.
    #[must_use]
    pub fn state_after(&self) -> &[MutationRow] {
        match self {
            Self::Accepted { state_after, .. } | Self::Rejected { state_after, .. } => state_after,
        }
    }

    /// Return affected rows for an accepted statement.
    #[must_use]
    pub const fn affected_rows(&self) -> Option<u32> {
        match self {
            Self::Accepted { affected_rows, .. } => Some(*affected_rows),
            Self::Rejected { .. } => None,
        }
    }

    /// Borrow exact RETURNING rows for an accepted statement.
    #[must_use]
    pub fn returned_rows(&self) -> Option<&[MutationProjectedRow]> {
        match self {
            Self::Accepted { returned_rows, .. } => Some(returned_rows),
            Self::Rejected { .. } => None,
        }
    }

    /// Borrow the complete logical secondary-index post-state.
    #[must_use]
    pub fn index_after(&self) -> &[MutationIndexEntry] {
        match self {
            Self::Accepted { index_after, .. } | Self::Rejected { index_after, .. } => index_after,
        }
    }

    /// Return the stable rejection, when rejected.
    #[must_use]
    pub const fn rejection(&self) -> Option<MutationExpectedRejection> {
        match self {
            Self::Accepted { .. } => None,
            Self::Rejected { rejection, .. } => Some(*rejection),
        }
    }
}

///
/// MutationSqliteExclusion
///
/// Explicit reason a generated step does not belong to maintained SQLite overlap.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationSqliteExclusion {
    /// Typed policy rejection must be checked against IcyDB's accepted policy taxonomy.
    TypedPolicyRejection,

    /// SQLite does not accept IcyDB's ordered UPDATE/DELETE window grammar.
    WindowedMutation,
}

///
/// MutationSqliteEligibility
///
/// Per-step secondary-provider decision made before adapter execution.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", content = "reason", rename_all = "snake_case")]
pub enum MutationSqliteEligibility {
    /// Execute and compare against bundled SQLite.
    Eligible,

    /// Preserve one enumerated exclusion.
    Excluded(MutationSqliteExclusion),
}

///
/// GeneratedMutationIdentity
///
/// Stable current witness/repetition identity.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedMutationIdentity {
    id: String,
    generator_version: u32,
    witness_id: String,
    #[serde(with = "crate::model::tagged_u64")]
    root_seed: u64,
    #[serde(with = "crate::model::tagged_u64")]
    sub_seed: u64,
    #[serde(with = "crate::model::tagged_u64")]
    repetition: u64,
}

impl GeneratedMutationIdentity {
    pub(crate) const fn new(
        id: String,
        generator_version: u32,
        witness_id: String,
        root_seed: u64,
        sub_seed: u64,
        repetition: u64,
    ) -> Self {
        Self {
            id,
            generator_version,
            witness_id,
            root_seed,
            sub_seed,
            repetition,
        }
    }

    /// Borrow the stable generated scenario ID.
    #[must_use]
    pub const fn id(&self) -> &str {
        self.id.as_str()
    }

    /// Return the generator version.
    #[must_use]
    pub const fn generator_version(&self) -> u32 {
        self.generator_version
    }

    /// Borrow the stable scheduled witness ID.
    #[must_use]
    pub const fn witness_id(&self) -> &str {
        self.witness_id.as_str()
    }

    /// Return the root seed.
    #[must_use]
    pub const fn root_seed(&self) -> u64 {
        self.root_seed
    }

    /// Return the independently derived sub-seed.
    #[must_use]
    pub const fn sub_seed(&self) -> u64 {
        self.sub_seed
    }

    /// Return the witness-local repetition ordinal.
    #[must_use]
    pub const fn repetition(&self) -> u64 {
        self.repetition
    }

    fn validate(&self) -> Result<(), SqlGeneratorError> {
        crate::mutation::generator::validate_generated_mutation_identity(self)
    }
}

///
/// GeneratedMutationStep
///
/// Typed statement paired with independent pre-state, outcome, SQL, and provider eligibility.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedMutationStep {
    statement: MutationStatement,
    rendered_sql: String,
    sqlite_eligibility: MutationSqliteEligibility,
    state_before: Vec<MutationRow>,
    expected: MutationStepOutcome,
}

impl GeneratedMutationStep {
    /// Borrow the typed statement.
    #[must_use]
    pub const fn statement(&self) -> &MutationStatement {
        &self.statement
    }

    /// Borrow current IcyDB SQL.
    #[must_use]
    pub const fn rendered_sql(&self) -> &str {
        self.rendered_sql.as_str()
    }

    /// Return SQLite eligibility.
    #[must_use]
    pub const fn sqlite_eligibility(&self) -> MutationSqliteEligibility {
        self.sqlite_eligibility
    }

    /// Borrow complete canonical pre-state.
    #[must_use]
    pub const fn state_before(&self) -> &[MutationRow] {
        self.state_before.as_slice()
    }

    /// Borrow the independent expected outcome.
    #[must_use]
    pub const fn expected(&self) -> &MutationStepOutcome {
        &self.expected
    }
}

///
/// GeneratedMutationSequence
///
/// One catalog-bound scheduled mutation witness with complete independent state evidence.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedMutationSequence {
    identity: GeneratedMutationIdentity,
    structural_signature: StructuralSignature,
    ingress: MutationIngress,
    intent_class: MutationIntentClass,
    snapshot: MutationSnapshot,
    initial_rows: Vec<MutationRow>,
    steps: Vec<GeneratedMutationStep>,
    budgets: MutationBudgets,
}

impl GeneratedMutationSequence {
    /// Build one sequence and derive every atomic outcome independently.
    ///
    /// # Errors
    ///
    /// Returns a typed error for stale identity, signature, snapshot, fixture,
    /// statement, intent, or budget facts.
    #[expect(
        clippy::too_many_arguments,
        reason = "scheduled mutation construction keeps every frozen authority explicit"
    )]
    pub(crate) fn try_from_statements(
        identity: GeneratedMutationIdentity,
        structural_signature: StructuralSignature,
        ingress: MutationIngress,
        intent_class: MutationIntentClass,
        snapshot: MutationSnapshot,
        initial_rows: Vec<MutationRow>,
        statements: Vec<MutationStatement>,
        budgets: MutationBudgets,
    ) -> Result<Self, SqlGeneratorError> {
        identity.validate()?;
        structural_signature.validate()?;
        snapshot.validate()?;
        budgets.validate()?;
        validate_rows(&snapshot, &initial_rows)?;
        if initial_rows.len() > budgets.max_fixture_rows as usize
            || statements.is_empty()
            || statements.len() > budgets.max_statements as usize
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "mutation fixture or statement count violates its deterministic budget",
            ));
        }
        let mut state = canonical_rows(initial_rows.clone());
        let mut steps = Vec::with_capacity(statements.len());
        for statement in statements {
            statement.operation.validate(&snapshot)?;
            let state_before = state.clone();
            let expected = apply_statement(&snapshot, &state, &statement)?;
            state = expected.state_after().to_vec();
            let sqlite_eligibility = sqlite_eligibility(&statement, &expected);
            let rendered_sql = statement.render(&snapshot)?;
            steps.push(GeneratedMutationStep {
                statement,
                rendered_sql,
                sqlite_eligibility,
                state_before,
                expected,
            });
        }
        let sequence = Self {
            identity,
            structural_signature,
            ingress,
            intent_class,
            snapshot,
            initial_rows: canonical_rows(initial_rows),
            steps,
            budgets,
        };
        sequence.validate()?;
        Ok(sequence)
    }

    /// Borrow the deterministic witness identity.
    #[must_use]
    pub const fn identity(&self) -> &GeneratedMutationIdentity {
        &self.identity
    }

    /// Borrow the full observed structural signature.
    #[must_use]
    pub const fn structural_signature(&self) -> &StructuralSignature {
        &self.structural_signature
    }

    /// Return the frozen ingress requirement.
    #[must_use]
    pub const fn ingress(&self) -> MutationIngress {
        self.ingress
    }

    /// Return the primary frozen intent class.
    #[must_use]
    pub const fn intent_class(&self) -> MutationIntentClass {
        self.intent_class
    }

    /// Borrow accepted snapshot facts.
    #[must_use]
    pub const fn snapshot(&self) -> &MutationSnapshot {
        &self.snapshot
    }

    /// Borrow canonical initial rows.
    #[must_use]
    pub const fn initial_rows(&self) -> &[MutationRow] {
        self.initial_rows.as_slice()
    }

    /// Borrow generated steps.
    #[must_use]
    pub const fn steps(&self) -> &[GeneratedMutationStep] {
        self.steps.as_slice()
    }

    /// Return deterministic budgets.
    #[must_use]
    pub const fn budgets(&self) -> MutationBudgets {
        self.budgets
    }

    /// Borrow final canonical modeled state.
    #[must_use]
    pub fn final_state(&self) -> &[MutationRow] {
        self.steps
            .last()
            .map_or_else(|| self.initial_rows(), |step| step.expected.state_after())
    }

    /// Compute a canonical sequence fingerprint.
    ///
    /// # Errors
    ///
    /// Returns a typed serialization error when canonical encoding fails.
    pub fn fingerprint(&self) -> Result<String, SqlGeneratorError> {
        let bytes = crate::replay::canonical_json_bytes(self)?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }

    /// Return exact per-field intent counts across the witness.
    #[must_use]
    pub fn intent_counts(&self) -> BTreeMap<MutationIntentKind, u32> {
        let mut counts = BTreeMap::new();
        for intent in self
            .steps
            .iter()
            .flat_map(|step| step.statement.intent_provenance())
        {
            *counts.entry(intent).or_insert(0) += 1;
        }
        counts
    }

    /// Revalidate all identity, signature, rendering, state, intent, and budget facts.
    ///
    /// # Errors
    ///
    /// Returns a typed error at the first stale embedded fact.
    pub fn validate(&self) -> Result<(), SqlGeneratorError> {
        self.identity.validate()?;
        self.structural_signature.validate()?;
        self.snapshot.validate()?;
        self.budgets.validate()?;
        validate_rows(&self.snapshot, &self.initial_rows)?;
        if self.initial_rows.len() > self.budgets.max_fixture_rows as usize
            || self.steps.is_empty()
            || self.steps.len() > self.budgets.max_statements as usize
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "mutation sequence exceeds fixture or statement budget",
            ));
        }
        let mut state = self.initial_rows.clone();
        for step in &self.steps {
            step.statement.operation.validate(&self.snapshot)?;
            let expected = apply_statement(&self.snapshot, &state, &step.statement)?;
            if step.state_before != state
                || step.rendered_sql != step.statement.render(&self.snapshot)?
                || step.sqlite_eligibility != sqlite_eligibility(&step.statement, &expected)
                || step.expected != expected
            {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidCase,
                    "mutation step embeds stale pre-state, SQL, eligibility, or model outcome",
                ));
            }
            state = expected.state_after().to_vec();
        }
        validate_intent_class(self.intent_class, &self.intent_counts())?;
        Ok(())
    }

    pub(crate) fn statements(&self) -> Vec<MutationStatement> {
        self.steps
            .iter()
            .map(|step| step.statement.clone())
            .collect()
    }

    pub(crate) fn rebuilt(
        &self,
        initial_rows: Vec<MutationRow>,
        statements: Vec<MutationStatement>,
    ) -> Result<Self, SqlGeneratorError> {
        Self::try_from_statements(
            self.identity.clone(),
            self.structural_signature.clone(),
            self.ingress,
            self.intent_class,
            self.snapshot.clone(),
            initial_rows,
            statements,
            self.budgets,
        )
    }
}

const fn sqlite_eligibility(
    statement: &MutationStatement,
    expected: &MutationStepOutcome,
) -> MutationSqliteEligibility {
    if statement.operation.window().is_some() {
        return MutationSqliteEligibility::Excluded(MutationSqliteExclusion::WindowedMutation);
    }
    if matches!(
        expected.rejection(),
        Some(
            MutationExpectedRejection::DefaultUnavailable
                | MutationExpectedRejection::MissingRequiredField
        )
    ) {
        return MutationSqliteEligibility::Excluded(MutationSqliteExclusion::TypedPolicyRejection);
    }
    MutationSqliteEligibility::Eligible
}

fn validate_intent_class(
    class: MutationIntentClass,
    counts: &BTreeMap<MutationIntentKind, u32>,
) -> Result<(), SqlGeneratorError> {
    let contains = |kind| counts.get(&kind).copied().unwrap_or_default() > 0;
    let valid = match class {
        MutationIntentClass::Authored => {
            !contains(MutationIntentKind::Omitted)
                && !contains(MutationIntentKind::InsertDefault)
                && !contains(MutationIntentKind::UpdateDefault)
        }
        MutationIntentClass::ExplicitDefault => {
            contains(MutationIntentKind::InsertDefault)
                || contains(MutationIntentKind::UpdateDefault)
        }
        MutationIntentClass::MixedBatch => {
            contains(MutationIntentKind::Authored)
                && contains(MutationIntentKind::Omitted)
                && contains(MutationIntentKind::InsertDefault)
        }
        MutationIntentClass::Omitted => contains(MutationIntentKind::Omitted),
        MutationIntentClass::Preserve => {
            contains(MutationIntentKind::Preserve) && contains(MutationIntentKind::Authored)
        }
    };
    if !valid {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "mutation witness primary intent disagrees with per-field provenance",
        ));
    }
    Ok(())
}

fn apply_statement(
    snapshot: &MutationSnapshot,
    state_before: &[MutationRow],
    statement: &MutationStatement,
) -> Result<MutationStepOutcome, SqlGeneratorError> {
    let mut state_after = state_before.to_vec();
    match &statement.operation {
        MutationOperation::Insert { rows } => {
            let mut resolved = Vec::with_capacity(rows.len());
            for row in rows {
                match resolve_insert(snapshot, row) {
                    Ok(row) => resolved.push(row),
                    Err(rejection) => {
                        return Ok(MutationStepOutcome::Rejected {
                            rejection,
                            state_after: state_before.to_vec(),
                            index_after: snapshot.secondary_index_entries(state_before)?,
                        });
                    }
                }
            }
            apply_insert_rows(snapshot, state_before, resolved, &statement.returning)
        }
        MutationOperation::InsertFromQuery {
            predicate,
            key_source,
        } => {
            let rows = state_before
                .iter()
                .filter(|row| predicate.matches(row))
                .map(|row| {
                    let key = match key_source {
                        MutationInsertQueryKeySource::Key => row.key,
                        MutationInsertQueryKeySource::Number => row.predicate_number(),
                    };
                    MutationRow::authored_scalar(key, row.predicate_text(), row.predicate_number())
                })
                .collect::<Vec<_>>();
            apply_insert_rows(snapshot, state_before, rows, &statement.returning)
        }
        MutationOperation::Update {
            predicate,
            assignment,
            window,
        } => {
            let selected = selected_indices(state_before, predicate, *window);
            let mut returned = Vec::with_capacity(selected.len());
            for index in selected.iter().copied() {
                assignment.apply(snapshot, &mut state_after[index])?;
                returned.push(state_after[index].clone());
            }
            state_after = canonical_rows(state_after);
            Ok(MutationStepOutcome::Accepted {
                affected_rows: row_count(selected.len())?,
                returned_rows: project_rows(snapshot, &returned, &statement.returning)?,
                index_after: snapshot.secondary_index_entries(&state_after)?,
                state_after,
            })
        }
        MutationOperation::Delete { predicate, window } => {
            let selected = selected_indices(state_before, predicate, *window);
            let selected_rows = selected
                .iter()
                .map(|index| state_before[*index].clone())
                .collect::<Vec<_>>();
            let keys = selected_rows
                .iter()
                .map(MutationRow::key)
                .collect::<BTreeSet<_>>();
            state_after.retain(|row| !keys.contains(&row.key));
            Ok(MutationStepOutcome::Accepted {
                affected_rows: row_count(selected.len())?,
                returned_rows: project_rows(snapshot, &selected_rows, &statement.returning)?,
                index_after: snapshot.secondary_index_entries(&state_after)?,
                state_after,
            })
        }
    }
}

fn resolve_insert(
    snapshot: &MutationSnapshot,
    input: &MutationInsertRow,
) -> Result<MutationRow, MutationExpectedRejection> {
    match (snapshot.profile, input) {
        (
            MutationSchemaProfile::AuthoredScalar,
            MutationInsertRow::AuthoredScalar { key, text, number },
        ) => Ok(MutationRow::authored_scalar(*key, text, *number)),
        (
            MutationSchemaProfile::AcceptedDefault,
            MutationInsertRow::AcceptedDefault {
                key,
                name,
                tier,
                score,
                note,
            },
        ) => {
            let key = resolve_required_unsigned(key)?;
            let name = resolve_required_text(name)?;
            let tier = resolve_default_text(
                tier,
                snapshot
                    .field(MutationFieldRole::Tier)
                    .and_then(MutationField::default),
            )?;
            let score = resolve_default_unsigned(
                score,
                snapshot
                    .field(MutationFieldRole::Score)
                    .and_then(MutationField::default),
            )?;
            let note = resolve_default_nullable_text(
                note,
                snapshot
                    .field(MutationFieldRole::Note)
                    .and_then(MutationField::default),
            )?;
            Ok(MutationRow::accepted_default(key, name, tier, score, note))
        }
        _ => Err(MutationExpectedRejection::MissingRequiredField),
    }
}

const fn resolve_required_unsigned(
    intent: &MutationWriteIntent<u64>,
) -> Result<u64, MutationExpectedRejection> {
    match intent {
        MutationWriteIntent::Authored(value) => Ok(*value),
        MutationWriteIntent::Omitted => Err(MutationExpectedRejection::MissingRequiredField),
        MutationWriteIntent::Default => Err(MutationExpectedRejection::DefaultUnavailable),
    }
}

fn resolve_required_text(
    intent: &MutationWriteIntent<String>,
) -> Result<String, MutationExpectedRejection> {
    match intent {
        MutationWriteIntent::Authored(value) => Ok(value.clone()),
        MutationWriteIntent::Omitted => Err(MutationExpectedRejection::MissingRequiredField),
        MutationWriteIntent::Default => Err(MutationExpectedRejection::DefaultUnavailable),
    }
}

fn resolve_default_text(
    intent: &MutationWriteIntent<String>,
    default: Option<&MutationDefaultValue>,
) -> Result<String, MutationExpectedRejection> {
    match intent {
        MutationWriteIntent::Authored(value) => Ok(value.clone()),
        MutationWriteIntent::Omitted | MutationWriteIntent::Default => match default {
            Some(MutationDefaultValue::Text(value)) => Ok(value.clone()),
            _ => Err(MutationExpectedRejection::DefaultUnavailable),
        },
    }
}

const fn resolve_default_unsigned(
    intent: &MutationWriteIntent<u64>,
    default: Option<&MutationDefaultValue>,
) -> Result<u64, MutationExpectedRejection> {
    match intent {
        MutationWriteIntent::Authored(value) => Ok(*value),
        MutationWriteIntent::Omitted | MutationWriteIntent::Default => match default {
            Some(MutationDefaultValue::UnsignedInteger(value)) => Ok(*value),
            _ => Err(MutationExpectedRejection::DefaultUnavailable),
        },
    }
}

fn resolve_default_nullable_text(
    intent: &MutationWriteIntent<Option<String>>,
    default: Option<&MutationDefaultValue>,
) -> Result<Option<String>, MutationExpectedRejection> {
    match intent {
        MutationWriteIntent::Authored(value) => Ok(value.clone()),
        MutationWriteIntent::Omitted | MutationWriteIntent::Default => match default {
            Some(MutationDefaultValue::NullText) => Ok(None),
            Some(MutationDefaultValue::Text(value)) => Ok(Some(value.clone())),
            _ => Err(MutationExpectedRejection::DefaultUnavailable),
        },
    }
}

fn apply_insert_rows(
    snapshot: &MutationSnapshot,
    state_before: &[MutationRow],
    rows: Vec<MutationRow>,
    returning: &MutationReturning,
) -> Result<MutationStepOutcome, SqlGeneratorError> {
    let existing = state_before
        .iter()
        .map(MutationRow::key)
        .collect::<BTreeSet<_>>();
    let mut inserted = BTreeSet::new();
    if rows
        .iter()
        .any(|row| existing.contains(&row.key) || !inserted.insert(row.key))
    {
        return Ok(MutationStepOutcome::Rejected {
            rejection: MutationExpectedRejection::DuplicatePrimaryKey,
            state_after: state_before.to_vec(),
            index_after: snapshot.secondary_index_entries(state_before)?,
        });
    }
    let mut state_after = state_before.to_vec();
    state_after.extend(rows.iter().cloned());
    state_after = canonical_rows(state_after);
    Ok(MutationStepOutcome::Accepted {
        affected_rows: row_count(rows.len())?,
        returned_rows: project_rows(snapshot, &rows, returning)?,
        index_after: snapshot.secondary_index_entries(&state_after)?,
        state_after,
    })
}

fn project_rows(
    snapshot: &MutationSnapshot,
    rows: &[MutationRow],
    returning: &MutationReturning,
) -> Result<Vec<MutationProjectedRow>, SqlGeneratorError> {
    let roles = returning.field_roles(snapshot)?;
    if roles.is_empty() {
        return Ok(Vec::new());
    }
    let mut ordered = rows.to_vec();
    ordered.sort_by_key(MutationRow::key);
    ordered
        .iter()
        .map(|row| {
            let fields = roles
                .iter()
                .map(|role| {
                    row.value(*role)
                        .map(|value| MutationProjectedField { role: *role, value })
                        .ok_or_else(|| {
                            SqlGeneratorError::new(
                                SqlGeneratorErrorKind::InvalidCase,
                                "RETURNING role is absent from the modeled row",
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(MutationProjectedRow { fields })
        })
        .collect()
}

fn selected_indices(
    rows: &[MutationRow],
    predicate: &MutationPredicate,
    window: Option<MutationWindow>,
) -> Vec<usize> {
    let mut selected = rows
        .iter()
        .enumerate()
        .filter(|(_, row)| predicate.matches(row))
        .map(|(index, row)| (index, row.key))
        .collect::<Vec<_>>();
    if let Some(window) = window {
        selected.sort_by_key(|(_, key)| *key);
        if window.order == MutationOrder::KeyDescending {
            selected.reverse();
        }
        selected = selected
            .into_iter()
            .skip(window.offset as usize)
            .take(window.limit as usize)
            .collect();
    }
    selected.into_iter().map(|(index, _)| index).collect()
}

fn apply_text_update(
    current: &mut String,
    intent: &MutationUpdateIntent<String>,
    field: &MutationField,
) -> Result<(), SqlGeneratorError> {
    match intent {
        MutationUpdateIntent::Authored(value) => current.clone_from(value),
        MutationUpdateIntent::Default => match field.default() {
            Some(MutationDefaultValue::Text(value)) => current.clone_from(value),
            _ => {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidCase,
                    "typed update DEFAULT lacks an accepted text default",
                ));
            }
        },
        MutationUpdateIntent::Preserve => {}
    }
    Ok(())
}

fn apply_unsigned_update(
    current: &mut u64,
    intent: &MutationUpdateIntent<u64>,
    field: &MutationField,
) -> Result<(), SqlGeneratorError> {
    match intent {
        MutationUpdateIntent::Authored(value) => *current = *value,
        MutationUpdateIntent::Default => match field.default() {
            Some(MutationDefaultValue::UnsignedInteger(value)) => *current = *value,
            _ => {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidCase,
                    "typed update DEFAULT lacks an accepted unsigned default",
                ));
            }
        },
        MutationUpdateIntent::Preserve => {}
    }
    Ok(())
}

fn apply_nullable_text_update(
    current: &mut Option<String>,
    intent: &MutationUpdateIntent<Option<String>>,
    field: &MutationField,
) -> Result<(), SqlGeneratorError> {
    match intent {
        MutationUpdateIntent::Authored(value) => current.clone_from(value),
        MutationUpdateIntent::Default => match field.default() {
            Some(MutationDefaultValue::NullText) => *current = None,
            Some(MutationDefaultValue::Text(value)) => *current = Some(value.clone()),
            _ => {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidCase,
                    "typed update DEFAULT lacks an accepted nullable-text default",
                ));
            }
        },
        MutationUpdateIntent::Preserve => {}
    }
    Ok(())
}

fn render_text_update(
    field: &MutationField,
    intent: &MutationUpdateIntent<String>,
) -> Option<String> {
    match intent {
        MutationUpdateIntent::Authored(value) => {
            Some(format!("{} = '{}'", field.name(), quote_text(value)))
        }
        MutationUpdateIntent::Default => Some(format!("{} = DEFAULT", field.name())),
        MutationUpdateIntent::Preserve => None,
    }
}

fn render_unsigned_update(
    field: &MutationField,
    intent: &MutationUpdateIntent<u64>,
) -> Option<String> {
    match intent {
        MutationUpdateIntent::Authored(value) => Some(format!("{} = {value}", field.name())),
        MutationUpdateIntent::Default => Some(format!("{} = DEFAULT", field.name())),
        MutationUpdateIntent::Preserve => None,
    }
}

fn render_nullable_text_update(
    field: &MutationField,
    intent: &MutationUpdateIntent<Option<String>>,
) -> Option<String> {
    match intent {
        MutationUpdateIntent::Authored(Some(value)) => {
            Some(format!("{} = '{}'", field.name(), quote_text(value)))
        }
        MutationUpdateIntent::Authored(None) => Some(format!("{} = NULL", field.name())),
        MutationUpdateIntent::Default => Some(format!("{} = DEFAULT", field.name())),
        MutationUpdateIntent::Preserve => None,
    }
}

fn validate_rows(
    snapshot: &MutationSnapshot,
    rows: &[MutationRow],
) -> Result<(), SqlGeneratorError> {
    let mut keys = BTreeSet::new();
    for row in rows {
        row.validate(snapshot.profile)?;
        if !keys.insert(row.key) {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "complete modeled mutation state contains duplicate keys",
            ));
        }
    }
    Ok(())
}

fn canonical_rows(mut rows: Vec<MutationRow>) -> Vec<MutationRow> {
    rows.sort_by_key(MutationRow::key);
    rows
}

fn row_count(count: usize) -> Result<u32, SqlGeneratorError> {
    u32::try_from(count).map_err(|_| {
        SqlGeneratorError::new(
            SqlGeneratorErrorKind::Budget,
            "mutation affected-row count exceeds u32",
        )
    })
}

fn quote_text(value: &str) -> String {
    value.replace('\'', "''")
}
