//! Module: sql_generator::structure
//! Responsibility: one relationship-preserving structural signature and frozen obligations.
//! Does not own: SQL rendering, execution verdicts, or product route selection.
//! Boundary: reads the reviewed obligation catalog and exposes only current generated witnesses.

use crate::{SqlGeneratorError, SqlGeneratorErrorKind, replay::canonical_json_bytes};

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const CATALOG_ARTIFACT: &str = include_str!(
    "../../../docs/design/0.215-sql-structural-coverage-and-range-remediation/0.215-coverage-obligations.json"
);
const CATALOG_FORMAT_VERSION: u32 = 3;
const MAX_CATALOG_BYTES: usize = 262_144;
const MAX_SIGNATURE_BYTES: usize = 65_536;
const GENERATED_SELECT_PROVIDER_PREFIX: &str = "generated.select.";
const GENERATED_MUTATION_PROVIDER_PREFIX: &str = "generated.mutation.";

///
/// ExecutionAccess
///
/// Closed plan-access fact compared independently from structural identity.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionAccess {
    /// A compatible composite index supplies a constrained key prefix.
    CompositePrefix,

    /// A maintained expression-range route is required by deterministic evidence.
    ExpressionRange,

    /// The accepted entity store is scanned without a selected secondary route.
    FullScan,

    /// Mutation selection flows through the ordinary admitted mutation executor.
    MutationSelection,

    /// No product execution is expected for a typed rejection.
    NotApplicable,

    /// One exact primary-key lookup is required.
    PrimaryExact,

    /// One single-field secondary-index range is selected.
    SecondaryRange,
}

///
/// ExecutionCovering
///
/// Closed row-materialization fact compared independently from structural identity.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionCovering {
    /// Some result cells come from an index while other cells require row materialization.
    Hybrid,

    /// The selected route requires ordinary row materialization.
    NonCovering,

    /// Covering does not apply to this scenario.
    NotApplicable,

    /// Every required result cell is supplied by the selected index route.
    Pure,
}

///
/// RequiredExecutionFacts
///
/// Reviewed planner and materialization requirement carried by one scheduled witness.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RequiredExecutionFacts {
    access: ExecutionAccess,
    covering: ExecutionCovering,
}

impl RequiredExecutionFacts {
    /// Build one reviewed execution requirement.
    #[must_use]
    pub const fn new(access: ExecutionAccess, covering: ExecutionCovering) -> Self {
        Self { access, covering }
    }

    /// Return the required selected-access family.
    #[must_use]
    pub const fn access(self) -> ExecutionAccess {
        self.access
    }

    /// Return the required materialization family.
    #[must_use]
    pub const fn covering(self) -> ExecutionCovering {
        self.covering
    }
}

///
/// ObservedExecutionFacts
///
/// Product-derived access and materialization facts recorded after IcyDB execution.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObservedExecutionFacts {
    access: ExecutionAccess,
    covering: ExecutionCovering,
}

impl ObservedExecutionFacts {
    /// Build facts observed from product compilation and execution.
    #[must_use]
    pub const fn new(access: ExecutionAccess, covering: ExecutionCovering) -> Self {
        Self { access, covering }
    }

    /// Return the observed selected-access family.
    #[must_use]
    pub const fn access(self) -> ExecutionAccess {
        self.access
    }

    /// Return the observed materialization family.
    #[must_use]
    pub const fn covering(self) -> ExecutionCovering {
        self.covering
    }
}

impl From<RequiredExecutionFacts> for ObservedExecutionFacts {
    fn from(required: RequiredExecutionFacts) -> Self {
        Self::new(required.access(), required.covering())
    }
}

///
/// StructuralSignature
///
/// Relationship-preserving identity derived from one validated typed SQL tree.
/// Fixture policy, planner expectations, literal payloads, roots, and repetitions
/// are excluded.
///

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StructuralSignature {
    declaration_kind: String,
    schema_profile: String,
    statement_family: String,
    expected_violation: String,
    canonical_structure: String,
}

impl StructuralSignature {
    pub(crate) fn derived(
        declaration_kind: &str,
        profile: &str,
        statement_family: &str,
        expected_violation: &str,
        canonical_structure: String,
    ) -> Self {
        Self {
            declaration_kind: declaration_kind.to_string(),
            schema_profile: profile.to_string(),
            statement_family: statement_family.to_string(),
            expected_violation: expected_violation.to_string(),
            canonical_structure,
        }
    }

    /// Build one code-owned deterministic-provider requirement.
    ///
    /// Generated SELECT and mutation signatures must use their typed-tree
    /// derivation paths instead. This constructor exists for exact maintained
    /// providers whose canonical declaration is outside the generated AST.
    ///
    /// # Errors
    ///
    /// Returns a typed error when any closed-vocabulary or canonical-structure
    /// fact is invalid.
    pub fn try_new_deterministic_requirement(
        profile: &str,
        statement_family: &str,
        canonical_structure: String,
    ) -> Result<Self, SqlGeneratorError> {
        let signature = Self::derived(
            "accepted",
            profile,
            statement_family,
            "none",
            canonical_structure,
        );
        signature.validate()?;
        Ok(signature)
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
        if self.canonical_structure.is_empty()
            || self.canonical_structure.len() > MAX_SIGNATURE_BYTES
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "structural signature has an empty or oversized canonical typed structure",
            ));
        }
        let structure = serde_json::from_str::<serde_json::Value>(&self.canonical_structure)
            .map_err(|source| {
                SqlGeneratorError::with_json_source(
                    SqlGeneratorErrorKind::Serialization,
                    "structural signature contains malformed canonical typed structure",
                    source,
                )
            })?;
        if canonical_json_bytes(&structure)? != self.canonical_structure.as_bytes() {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Serialization,
                "structural signature typed structure is not canonical JSON",
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
    required_execution_facts: RequiredExecutionFacts,
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
    required_execution_facts: RequiredExecutionFacts,
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

    /// Borrow the full frozen required structural signature.
    #[must_use]
    pub const fn signature(&self) -> &StructuralSignature {
        &self.signature
    }

    /// Return the reviewed execution requirement.
    #[must_use]
    pub const fn required_execution_facts(&self) -> RequiredExecutionFacts {
        self.required_execution_facts
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

    /// Borrow the full frozen required structural signature.
    #[must_use]
    pub const fn signature(&self) -> &StructuralSignature {
        &self.signature
    }

    /// Return the reviewed execution requirement.
    #[must_use]
    pub const fn required_execution_facts(&self) -> RequiredExecutionFacts {
        self.required_execution_facts
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
    required_execution_facts: RequiredExecutionFacts,
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
            Ok(ScheduledSelectWitness {
                requirement_id: requirement.id,
                provider_id: requirement.provider_id,
                witness_id: requirement.witness_id,
                signature: requirement.expected_structural_signature,
                required_execution_facts: requirement.required_execution_facts,
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
            Ok(ScheduledMutationWitness {
                requirement_id: requirement.id,
                provider_id: requirement.provider_id,
                witness_id: requirement.witness_id,
                signature: requirement.expected_structural_signature,
                required_execution_facts: requirement.required_execution_facts,
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
    for requirement in &artifact.catalog.required_structural_obligations {
        requirement.expected_structural_signature.validate()?;
    }
    Ok(artifact)
}
