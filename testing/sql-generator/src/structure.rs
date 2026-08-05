//! Module: sql_generator::structure
//! Responsibility: one relationship-preserving structural signature and code-owned witness schedule.
//! Does not own: SQL rendering, execution verdicts, or product route selection.
//! Boundary: exposes the reviewed current generated witnesses without reading historical artifacts.

use crate::{SqlGeneratorError, SqlGeneratorErrorKind, replay::canonical_json_bytes};

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const MAX_WITNESS_SCHEDULE_BYTES: usize = 262_144;
const MAX_SIGNATURE_BYTES: usize = 65_536;
const WITNESS_SCHEDULE_HASH_DOMAIN: &[u8] = b"icydb-sql-structural-witness-schedule/v1";

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

#[derive(Clone, Copy)]
enum ScheduledWitnessKind {
    Mutation,
    Select,
}

#[derive(Clone, Copy)]
struct ScheduledWitnessDeclaration {
    kind: ScheduledWitnessKind,
    requirement_id: &'static str,
    provider_id: &'static str,
    witness_id: &'static str,
    required_execution_facts: RequiredExecutionFacts,
}

const FULL_SCAN_NON_COVERING: RequiredExecutionFacts =
    RequiredExecutionFacts::new(ExecutionAccess::FullScan, ExecutionCovering::NonCovering);
const SECONDARY_RANGE_NON_COVERING: RequiredExecutionFacts = RequiredExecutionFacts::new(
    ExecutionAccess::SecondaryRange,
    ExecutionCovering::NonCovering,
);
const COMPOSITE_PREFIX_NON_COVERING: RequiredExecutionFacts = RequiredExecutionFacts::new(
    ExecutionAccess::CompositePrefix,
    ExecutionCovering::NonCovering,
);
const MUTATION_SELECTION: RequiredExecutionFacts = RequiredExecutionFacts::new(
    ExecutionAccess::MutationSelection,
    ExecutionCovering::NotApplicable,
);
const EXECUTION_NOT_APPLICABLE: RequiredExecutionFacts = RequiredExecutionFacts::new(
    ExecutionAccess::NotApplicable,
    ExecutionCovering::NotApplicable,
);

const fn scheduled_witness(
    kind: ScheduledWitnessKind,
    requirement_id: &'static str,
    provider_id: &'static str,
    witness_id: &'static str,
    required_execution_facts: RequiredExecutionFacts,
) -> ScheduledWitnessDeclaration {
    ScheduledWitnessDeclaration {
        kind,
        requirement_id,
        provider_id,
        witness_id,
        required_execution_facts,
    }
}

const SCHEDULED_WITNESS_DECLARATIONS: &[ScheduledWitnessDeclaration] = &[
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.cache.cold_sql_fluent",
        "generated.select.reference_scalar",
        "tier_c.cache.cold_sql_fluent",
        FULL_SCAN_NON_COVERING,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.global.empty_filter",
        "generated.select.reference_scalar",
        "tier_c.global.empty_filter",
        FULL_SCAN_NON_COVERING,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.global.nonempty_filter",
        "generated.select.reference_scalar",
        "tier_c.global.nonempty_filter",
        FULL_SCAN_NON_COVERING,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.global.nonempty_multiple_projection",
        "generated.select.reference_scalar",
        "tier_c.global.nonempty_multiple_projection",
        FULL_SCAN_NON_COVERING,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.grouped.hash_bounded",
        "generated.select.reference_scalar",
        "tier_c.grouped.hash_bounded",
        FULL_SCAN_NON_COVERING,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.grouped.ordered_bounded",
        "generated.select.indexed_nullable_reference",
        "tier_c.grouped.ordered_bounded",
        SECONDARY_RANGE_NON_COVERING,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.indexed.composite_prefix_non_covering",
        "generated.select.indexed_nullable_reference",
        "tier_c.indexed.composite_prefix_non_covering",
        COMPOSITE_PREFIX_NON_COVERING,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.indexed.secondary_range_direct_compatible",
        "generated.select.indexed_nullable_reference",
        "tier_c.indexed.secondary_range_direct_compatible",
        SECONDARY_RANGE_NON_COVERING,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.indexed.secondary_range_non_covering_incompatible",
        "generated.select.indexed_nullable_reference",
        "tier_c.indexed.secondary_range_non_covering_incompatible",
        SECONDARY_RANGE_NON_COVERING,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Mutation,
        "required.mutation.authored_insert",
        "generated.mutation.authored_scalar",
        "tier_c.mutation.authored_insert",
        MUTATION_SELECTION,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Mutation,
        "required.mutation.authored_insert_from_query",
        "generated.mutation.authored_scalar",
        "tier_c.mutation.authored_insert_from_query",
        MUTATION_SELECTION,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Mutation,
        "required.mutation.authored_windowed",
        "generated.mutation.authored_scalar",
        "tier_c.mutation.authored_windowed",
        MUTATION_SELECTION,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Mutation,
        "required.mutation.default_delete_returning",
        "generated.mutation.accepted_default",
        "tier_c.mutation.default_delete_returning",
        MUTATION_SELECTION,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Mutation,
        "required.mutation.default_insert_authored",
        "generated.mutation.accepted_default",
        "tier_c.mutation.default_insert_authored",
        MUTATION_SELECTION,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Mutation,
        "required.mutation.default_insert_explicit",
        "generated.mutation.accepted_default",
        "tier_c.mutation.default_insert_explicit",
        MUTATION_SELECTION,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Mutation,
        "required.mutation.default_insert_mixed_batch",
        "generated.mutation.accepted_default",
        "tier_c.mutation.default_insert_mixed_batch",
        MUTATION_SELECTION,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Mutation,
        "required.mutation.default_insert_omitted",
        "generated.mutation.accepted_default",
        "tier_c.mutation.default_insert_omitted",
        MUTATION_SELECTION,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Mutation,
        "required.mutation.default_no_match",
        "generated.mutation.accepted_default",
        "tier_c.mutation.default_no_match",
        MUTATION_SELECTION,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Mutation,
        "required.mutation.default_reject_duplicate",
        "generated.mutation.accepted_default",
        "tier_c.mutation.default_reject_duplicate",
        MUTATION_SELECTION,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Mutation,
        "required.mutation.default_reject_pk_default",
        "generated.mutation.accepted_default",
        "tier_c.mutation.default_reject_pk_default",
        MUTATION_SELECTION,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Mutation,
        "required.mutation.default_reject_required",
        "generated.mutation.accepted_default",
        "tier_c.mutation.default_reject_required",
        MUTATION_SELECTION,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Mutation,
        "required.mutation.default_update_authored",
        "generated.mutation.accepted_default",
        "tier_c.mutation.default_update_authored",
        MUTATION_SELECTION,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Mutation,
        "required.mutation.default_update_default",
        "generated.mutation.accepted_default",
        "tier_c.mutation.default_update_default",
        MUTATION_SELECTION,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Mutation,
        "required.mutation.default_update_preserve",
        "generated.mutation.accepted_default",
        "tier_c.mutation.default_update_preserve",
        MUTATION_SELECTION,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.null.computed_aggregate",
        "generated.select.reference_scalar",
        "tier_c.null.computed_aggregate",
        FULL_SCAN_NON_COVERING,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.null.computed_distinct",
        "generated.select.reference_scalar",
        "tier_c.null.computed_distinct",
        FULL_SCAN_NON_COVERING,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.null.computed_ordering",
        "generated.select.reference_scalar",
        "tier_c.null.computed_ordering",
        FULL_SCAN_NON_COVERING,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.null.stored_comparison_membership",
        "generated.select.indexed_nullable_reference",
        "tier_c.null.stored_comparison_membership",
        FULL_SCAN_NON_COVERING,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.null.stored_ordering",
        "generated.select.indexed_nullable_reference",
        "tier_c.null.stored_ordering",
        FULL_SCAN_NON_COVERING,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.scalar.indexed_computed_distinct_window",
        "generated.select.indexed_nullable_reference",
        "tier_c.scalar.indexed_computed_distinct_window",
        SECONDARY_RANGE_NON_COVERING,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.scalar.reference_full_window",
        "generated.select.reference_scalar",
        "tier_c.scalar.reference_full_window",
        FULL_SCAN_NON_COVERING,
    ),
    scheduled_witness(
        ScheduledWitnessKind::Select,
        "required.scalar.reference_unknown_alias_order",
        "generated.select.reference_scalar",
        "tier_c.scalar.reference_unknown_alias_order",
        EXECUTION_NOT_APPLICABLE,
    ),
];

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
/// One generated SELECT obligation derived from the code-owned witness schedule.
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
/// One generated mutation obligation derived from the code-owned witness schedule.
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

#[derive(Serialize)]
struct ScheduledWitnessHashEntry<'a> {
    kind: &'static str,
    requirement_id: &'a str,
    provider_id: &'a str,
    witness_id: &'a str,
    expected_structural_signature: &'a StructuralSignature,
    required_execution_facts: RequiredExecutionFacts,
}

/// Return the reviewed schedule hash carried by every current scheduled receipt.
///
/// # Errors
///
/// Returns a typed error when a scheduled declaration or derived signature is invalid.
pub fn structural_witness_schedule_hash() -> Result<String, SqlGeneratorError> {
    let select = scheduled_select_witnesses()?;
    let mutation = scheduled_mutation_witnesses()?;
    let mut entries = select
        .iter()
        .map(|witness| ScheduledWitnessHashEntry {
            kind: "select",
            requirement_id: witness.requirement_id(),
            provider_id: witness.provider_id(),
            witness_id: witness.witness_id(),
            expected_structural_signature: witness.signature(),
            required_execution_facts: witness.required_execution_facts(),
        })
        .chain(mutation.iter().map(|witness| ScheduledWitnessHashEntry {
            kind: "mutation",
            requirement_id: witness.requirement_id(),
            provider_id: witness.provider_id(),
            witness_id: witness.witness_id(),
            expected_structural_signature: witness.signature(),
            required_execution_facts: witness.required_execution_facts(),
        }))
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.witness_id.cmp(right.witness_id));
    let bytes = canonical_json_bytes(&entries)?;
    if bytes.len() > MAX_WITNESS_SCHEDULE_BYTES {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::Budget,
            "SQL witness schedule exceeds its current byte bound",
        ));
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(WITNESS_SCHEDULE_HASH_DOMAIN);
    hasher.update(&bytes);

    Ok(hasher.finalize().to_hex().to_string())
}

/// Return the exact stable-order generated SELECT witness set from code-owned declarations.
///
/// # Errors
///
/// Returns a typed error when a declaration is duplicated or its typed recipe is invalid.
pub fn scheduled_select_witnesses() -> Result<Vec<ScheduledSelectWitness>, SqlGeneratorError> {
    let mut witnesses = SCHEDULED_WITNESS_DECLARATIONS
        .iter()
        .filter(|declaration| matches!(declaration.kind, ScheduledWitnessKind::Select))
        .map(|declaration| {
            Ok(ScheduledSelectWitness {
                requirement_id: declaration.requirement_id.to_string(),
                provider_id: declaration.provider_id.to_string(),
                witness_id: declaration.witness_id.to_string(),
                signature: crate::structural_signature_for_scheduled_select_witness(
                    declaration.witness_id,
                )?,
                required_execution_facts: declaration.required_execution_facts,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    witnesses.sort_by(|left, right| left.witness_id.cmp(&right.witness_id));
    let unique_witnesses = witnesses
        .iter()
        .map(|witness| witness.witness_id.as_str())
        .collect::<BTreeSet<_>>();
    let unique_requirements = witnesses
        .iter()
        .map(|witness| witness.requirement_id.as_str())
        .collect::<BTreeSet<_>>();
    if witnesses.is_empty()
        || unique_witnesses.len() != witnesses.len()
        || unique_requirements.len() != witnesses.len()
    {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "generated SELECT witness schedule is empty or duplicated",
        ));
    }
    Ok(witnesses)
}

/// Return the exact stable-order generated mutation witness set from code-owned declarations.
///
/// # Errors
///
/// Returns a typed error when a declaration is duplicated or its typed recipe is invalid.
pub fn scheduled_mutation_witnesses() -> Result<Vec<ScheduledMutationWitness>, SqlGeneratorError> {
    let mut witnesses = SCHEDULED_WITNESS_DECLARATIONS
        .iter()
        .filter(|declaration| matches!(declaration.kind, ScheduledWitnessKind::Mutation))
        .map(|declaration| {
            Ok(ScheduledMutationWitness {
                requirement_id: declaration.requirement_id.to_string(),
                provider_id: declaration.provider_id.to_string(),
                witness_id: declaration.witness_id.to_string(),
                signature: crate::structural_signature_for_scheduled_mutation_witness(
                    declaration.witness_id,
                )?,
                required_execution_facts: declaration.required_execution_facts,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    witnesses.sort_by(|left, right| left.witness_id.cmp(&right.witness_id));
    let unique_witnesses = witnesses
        .iter()
        .map(|witness| witness.witness_id.as_str())
        .collect::<BTreeSet<_>>();
    let unique_requirements = witnesses
        .iter()
        .map(|witness| witness.requirement_id.as_str())
        .collect::<BTreeSet<_>>();
    if witnesses.is_empty()
        || unique_witnesses.len() != witnesses.len()
        || unique_requirements.len() != witnesses.len()
    {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "generated mutation witness schedule is empty or duplicated",
        ));
    }
    Ok(witnesses)
}
