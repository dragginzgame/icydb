//! Module: sql_generator::structure
//! Responsibility: one lossless structural signature and the frozen 0.215 SELECT obligations.
//! Does not own: SQL rendering, execution verdicts, or product route selection.
//! Boundary: reads the reviewed obligation catalog and exposes only current generated witnesses.

use crate::{
    SqlGeneratorError, SqlGeneratorErrorKind, model::SelectSchemaProfile,
    replay::canonical_json_bytes,
};

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const CATALOG_ARTIFACT: &str = include_str!(
    "../../../docs/design/0.215-sql-structural-coverage-and-range-remediation/0.215-coverage-obligations.json"
);
const CATALOG_FORMAT_VERSION: u32 = 1;
const MAX_CATALOG_BYTES: usize = 262_144;
const MAX_SIGNATURE_BYTES: usize = 4_096;
const GENERATED_SELECT_PROVIDER_PREFIX: &str = "generated.select.";
const GENERATED_MUTATION_PROVIDER_PREFIX: &str = "generated.mutation.";

///
/// StructuralSignature
///
/// Lossless semantic identity derived for one generated statement. Literal payloads,
/// root seeds, and repetition ordinals are deliberately excluded.
///

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StructuralSignature {
    declaration_kind: String,
    schema_profile: String,
    statement_family: String,
    result_shape: String,
    projection_shape: String,
    predicate_shape: String,
    grouping_shape: String,
    having_shape: String,
    order_shape: String,
    window_shape: String,
    field_roles: String,
    semantic_value_class: String,
    fixture_class: String,
    required_access: String,
    required_covering: String,
    expected_violation: String,
}

impl StructuralSignature {
    #[expect(
        clippy::too_many_arguments,
        reason = "the lossless signature keeps every closed semantic dimension explicit"
    )]
    pub(crate) fn select(
        declaration_kind: &str,
        profile: SelectSchemaProfile,
        result_shape: &str,
        projection_shape: &str,
        predicate_shape: &str,
        grouping_shape: &str,
        having_shape: &str,
        order_shape: &str,
        window_shape: &str,
        field_roles: &str,
        semantic_value_class: &str,
        fixture_class: &str,
        required_access: &str,
        required_covering: &str,
        expected_violation: &str,
    ) -> Self {
        Self {
            declaration_kind: declaration_kind.to_string(),
            schema_profile: profile.id().to_string(),
            statement_family: "select".to_string(),
            result_shape: result_shape.to_string(),
            projection_shape: projection_shape.to_string(),
            predicate_shape: predicate_shape.to_string(),
            grouping_shape: grouping_shape.to_string(),
            having_shape: having_shape.to_string(),
            order_shape: order_shape.to_string(),
            window_shape: window_shape.to_string(),
            field_roles: field_roles.to_string(),
            semantic_value_class: semantic_value_class.to_string(),
            fixture_class: fixture_class.to_string(),
            required_access: required_access.to_string(),
            required_covering: required_covering.to_string(),
            expected_violation: expected_violation.to_string(),
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the lossless signature keeps every closed mutation dimension explicit"
    )]
    pub(crate) fn mutation(
        declaration_kind: &str,
        profile: &str,
        statement_family: &str,
        result_shape: &str,
        projection_shape: &str,
        predicate_shape: &str,
        order_shape: &str,
        field_roles: &str,
        semantic_value_class: &str,
        fixture_class: &str,
        expected_violation: &str,
    ) -> Self {
        Self {
            declaration_kind: declaration_kind.to_string(),
            schema_profile: profile.to_string(),
            statement_family: statement_family.to_string(),
            result_shape: result_shape.to_string(),
            projection_shape: projection_shape.to_string(),
            predicate_shape: predicate_shape.to_string(),
            grouping_shape: "none".to_string(),
            having_shape: "none".to_string(),
            order_shape: order_shape.to_string(),
            window_shape: "none".to_string(),
            field_roles: field_roles.to_string(),
            semantic_value_class: semantic_value_class.to_string(),
            fixture_class: fixture_class.to_string(),
            required_access: "mutation_selection".to_string(),
            required_covering: "not_applicable".to_string(),
            expected_violation: expected_violation.to_string(),
        }
    }

    pub(crate) fn invalid_select(profile: &str, violation: &str) -> Self {
        Self {
            declaration_kind: "singly_invalid".to_string(),
            schema_profile: profile.to_string(),
            statement_family: "select".to_string(),
            result_shape: "typed_error".to_string(),
            projection_shape: "valid_base".to_string(),
            predicate_shape: "none".to_string(),
            grouping_shape: "none".to_string(),
            having_shape: "none".to_string(),
            order_shape: "valid_base".to_string(),
            window_shape: "limit".to_string(),
            field_roles: "stored_scalar".to_string(),
            semantic_value_class: "ordinary".to_string(),
            fixture_class: "valid_base".to_string(),
            required_access: "not_applicable".to_string(),
            required_covering: "not_applicable".to_string(),
            expected_violation: violation.to_string(),
        }
    }

    /// Borrow the accepted schema profile that owns field and index facts.
    #[must_use]
    pub const fn schema_profile(&self) -> &str {
        self.schema_profile.as_str()
    }

    /// Borrow the statement-family identity.
    #[must_use]
    pub const fn statement_family(&self) -> &str {
        self.statement_family.as_str()
    }

    /// Borrow the semantic result shape.
    #[must_use]
    pub const fn result_shape(&self) -> &str {
        self.result_shape.as_str()
    }

    /// Borrow the grouping shape.
    #[must_use]
    pub const fn grouping_shape(&self) -> &str {
        self.grouping_shape.as_str()
    }

    /// Borrow the post-aggregate predicate shape.
    #[must_use]
    pub const fn having_shape(&self) -> &str {
        self.having_shape.as_str()
    }

    /// Borrow the limit/offset shape.
    #[must_use]
    pub const fn window_shape(&self) -> &str {
        self.window_shape.as_str()
    }

    /// Borrow the exact fixture-package identity.
    #[must_use]
    pub const fn fixture_class(&self) -> &str {
        self.fixture_class.as_str()
    }

    /// Borrow the expected typed violation, or `none` for an accepted case.
    #[must_use]
    pub const fn expected_violation(&self) -> &str {
        self.expected_violation.as_str()
    }

    /// Return whether this signature describes a singly-invalid statement.
    #[must_use]
    pub fn is_singly_invalid(&self) -> bool {
        self.declaration_kind == "singly_invalid" && self.expected_violation != "none"
    }

    /// Return a stable BLAKE3 digest of the full canonical signature.
    ///
    /// # Errors
    ///
    /// Returns a typed serialization error if canonical encoding fails.
    pub fn digest(&self) -> Result<String, SqlGeneratorError> {
        self.validate()?;
        Ok(blake3::hash(&canonical_json_bytes(self)?)
            .to_hex()
            .to_string())
    }

    /// Encode the sole current bounded canonical signature format.
    ///
    /// # Errors
    ///
    /// Returns a typed error for an invalid signature or oversized encoding.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, SqlGeneratorError> {
        self.validate()?;
        let bytes = canonical_json_bytes(self)?;
        if bytes.len() > MAX_SIGNATURE_BYTES {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "structural signature exceeds its current byte bound",
            ));
        }
        Ok(bytes)
    }

    /// Decode the sole current bounded canonical signature format.
    ///
    /// # Errors
    ///
    /// Returns a typed error for oversized, malformed, non-canonical, or invalid input.
    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, SqlGeneratorError> {
        if bytes.len() > MAX_SIGNATURE_BYTES {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "structural signature exceeds its current byte bound",
            ));
        }
        let signature = serde_json::from_slice::<Self>(bytes).map_err(|source| {
            SqlGeneratorError::with_json_source(
                SqlGeneratorErrorKind::Serialization,
                "failed to decode structural signature",
                source,
            )
        })?;
        signature.validate()?;
        if signature.to_canonical_json()? != bytes {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Serialization,
                "structural signature input is not canonical JSON",
            ));
        }
        Ok(signature)
    }

    pub(crate) fn validate(&self) -> Result<(), SqlGeneratorError> {
        for value in [
            self.declaration_kind.as_str(),
            self.schema_profile.as_str(),
            self.statement_family.as_str(),
            self.result_shape.as_str(),
            self.projection_shape.as_str(),
            self.predicate_shape.as_str(),
            self.grouping_shape.as_str(),
            self.having_shape.as_str(),
            self.order_shape.as_str(),
            self.window_shape.as_str(),
            self.field_roles.as_str(),
            self.semantic_value_class.as_str(),
            self.fixture_class.as_str(),
            self.required_access.as_str(),
            self.required_covering.as_str(),
            self.expected_violation.as_str(),
        ] {
            if value.is_empty()
                || value.len() > 128
                || !value.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'_' | b'|' | b'-')
                })
            {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidCase,
                    "structural signature contains an invalid closed-vocabulary value",
                ));
            }
        }
        let invalid = self.declaration_kind == "singly_invalid";
        if invalid == (self.expected_violation == "none")
            || (!invalid && self.declaration_kind != "accepted")
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "structural signature acceptance and violation facts disagree",
            ));
        }
        Ok(())
    }
}

///
/// ScheduledSelectWitness
///
/// One generated SELECT obligation read directly from the frozen reviewed catalog.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledSelectWitness {
    requirement_id: String,
    provider_id: String,
    witness_id: String,
    signature: StructuralSignature,
}

///
/// ScheduledMutationWitness
///
/// One generated mutation obligation read directly from the frozen reviewed catalog.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledMutationWitness {
    requirement_id: String,
    provider_id: String,
    witness_id: String,
    signature: StructuralSignature,
}

impl ScheduledMutationWitness {
    /// Borrow the exact reviewed requirement identity.
    #[must_use]
    pub const fn requirement_id(&self) -> &str {
        self.requirement_id.as_str()
    }

    /// Borrow the generated provider identity.
    #[must_use]
    pub const fn provider_id(&self) -> &str {
        self.provider_id.as_str()
    }

    /// Borrow the stable scheduled witness identity.
    #[must_use]
    pub const fn witness_id(&self) -> &str {
        self.witness_id.as_str()
    }

    /// Borrow the full required structural signature.
    #[must_use]
    pub const fn signature(&self) -> &StructuralSignature {
        &self.signature
    }
}

impl ScheduledSelectWitness {
    /// Borrow the exact reviewed requirement identity.
    #[must_use]
    pub const fn requirement_id(&self) -> &str {
        self.requirement_id.as_str()
    }

    /// Borrow the generated provider identity.
    #[must_use]
    pub const fn provider_id(&self) -> &str {
        self.provider_id.as_str()
    }

    /// Borrow the stable scheduled witness identity.
    #[must_use]
    pub const fn witness_id(&self) -> &str {
        self.witness_id.as_str()
    }

    /// Borrow the full required structural signature.
    #[must_use]
    pub const fn signature(&self) -> &StructuralSignature {
        &self.signature
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogArtifact {
    format_version: u32,
    catalog_hash: String,
    catalog: CatalogBody,
}

#[derive(Deserialize)]
struct CatalogBody {
    required_structural_obligations: Vec<CatalogRequirement>,
}

#[derive(Deserialize)]
struct CatalogRequirement {
    id: String,
    expected_structural_signature: StructuralSignature,
    provider_id: String,
    witness_id: String,
}

/// Return the reviewed catalog hash carried by every current scheduled receipt.
///
/// # Errors
///
/// Returns a typed error when the checked-in artifact is malformed or stale.
pub fn structural_obligation_catalog_hash() -> Result<String, SqlGeneratorError> {
    Ok(read_catalog()?.catalog_hash)
}

/// Return the exact stable-order generated SELECT witness set from the frozen catalog.
///
/// # Errors
///
/// Returns a typed error when the catalog is malformed, duplicated, or contains
/// an unsupported generated SELECT profile.
pub fn scheduled_select_witnesses() -> Result<Vec<ScheduledSelectWitness>, SqlGeneratorError> {
    let artifact = read_catalog()?;
    let mut witnesses = artifact
        .catalog
        .required_structural_obligations
        .into_iter()
        .filter(|requirement| {
            requirement
                .provider_id
                .starts_with(GENERATED_SELECT_PROVIDER_PREFIX)
        })
        .map(|requirement| {
            requirement.expected_structural_signature.validate()?;
            if requirement.expected_structural_signature.statement_family() != "select"
                || !matches!(
                    requirement.expected_structural_signature.schema_profile(),
                    "reference_scalar" | "indexed_nullable_reference"
                )
            {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidCase,
                    "generated SELECT obligation names an unsupported statement or schema profile",
                ));
            }
            Ok(ScheduledSelectWitness {
                requirement_id: requirement.id,
                provider_id: requirement.provider_id,
                witness_id: requirement.witness_id,
                signature: requirement.expected_structural_signature,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    witnesses.sort_by(|left, right| left.witness_id.cmp(&right.witness_id));
    let unique = witnesses
        .iter()
        .map(|witness| witness.witness_id.as_str())
        .collect::<BTreeSet<_>>();
    if witnesses.is_empty() || unique.len() != witnesses.len() {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "generated SELECT witness catalog is empty or duplicated",
        ));
    }
    Ok(witnesses)
}

/// Return the exact stable-order generated mutation witness set from the frozen catalog.
///
/// # Errors
///
/// Returns a typed error when the catalog is malformed, duplicated, or contains
/// an unsupported generated mutation profile.
pub fn scheduled_mutation_witnesses() -> Result<Vec<ScheduledMutationWitness>, SqlGeneratorError> {
    let artifact = read_catalog()?;
    let mut witnesses = artifact
        .catalog
        .required_structural_obligations
        .into_iter()
        .filter(|requirement| {
            requirement
                .provider_id
                .starts_with(GENERATED_MUTATION_PROVIDER_PREFIX)
        })
        .map(|requirement| {
            requirement.expected_structural_signature.validate()?;
            if !matches!(
                (
                    requirement.expected_structural_signature.statement_family(),
                    requirement.expected_structural_signature.schema_profile(),
                ),
                (
                    "insert"
                        | "insert_from_query"
                        | "update"
                        | "delete"
                        | "update_delete_no_match"
                        | "update_delete_window",
                    "authored_scalar" | "accepted_default",
                )
            ) {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidCase,
                    "generated mutation obligation names an unsupported statement or schema profile",
                ));
            }
            Ok(ScheduledMutationWitness {
                requirement_id: requirement.id,
                provider_id: requirement.provider_id,
                witness_id: requirement.witness_id,
                signature: requirement.expected_structural_signature,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    witnesses.sort_by(|left, right| left.witness_id.cmp(&right.witness_id));
    let unique = witnesses
        .iter()
        .map(|witness| witness.witness_id.as_str())
        .collect::<BTreeSet<_>>();
    if witnesses.is_empty() || unique.len() != witnesses.len() {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "generated mutation witness catalog is empty or duplicated",
        ));
    }
    Ok(witnesses)
}

fn read_catalog() -> Result<CatalogArtifact, SqlGeneratorError> {
    if CATALOG_ARTIFACT.len() > MAX_CATALOG_BYTES {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::Budget,
            "SQL obligation catalog exceeds its current byte bound",
        ));
    }
    let artifact = serde_json::from_str::<CatalogArtifact>(CATALOG_ARTIFACT).map_err(|source| {
        SqlGeneratorError::with_json_source(
            SqlGeneratorErrorKind::Serialization,
            "failed to decode the reviewed SQL obligation catalog",
            source,
        )
    })?;
    if artifact.format_version != CATALOG_FORMAT_VERSION
        || artifact.catalog_hash.len() != 64
        || !artifact
            .catalog_hash
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "SQL obligation catalog does not use the current reviewed identity",
        ));
    }
    Ok(artifact)
}
