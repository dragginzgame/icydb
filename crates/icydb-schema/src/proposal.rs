//! Canonical database-scoped proposal envelope.

use std::collections::{BTreeMap, BTreeSet};

use candid::CandidType;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    ConstraintSourceKey, EntityFragment, EntitySourceKey, FieldFragment, FieldSourceKey, FieldType,
    IndexSourceKey, MAX_SCHEMA_ASSIGNMENTS, MAX_SCHEMA_CAPABILITIES, MAX_SCHEMA_PROPOSAL_FRAGMENTS,
    MAX_SCHEMA_REMOVALS, MAX_SCHEMA_TYPE_DEPTH, NamedTypeFragment, RelationSourceKey,
    ScalarLiteral, SchemaContractError, SchemaFragment, SchemaProposalDigest, SchemaSubmissionKey,
    SourceCheckExpr, SourceCheckInstruction, TargetDatabaseIdentity, TargetStoreIdentity,
    TypeSourceKey, check_len, encode_schema_fragment, encode_schema_proposal,
};

/// Sole maintained proposal contract version.
#[derive(
    CandidType, Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct ProposalContractVersion(u16);

impl ProposalContractVersion {
    /// Current pre-1.0 hard-cut proposal contract version.
    pub const CURRENT: Self = Self(1);

    /// Construct a version token for decoding and incompatibility tests.
    #[must_use]
    pub const fn from_raw(value: u16) -> Self {
        Self(value)
    }

    /// Return the raw version value.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// Feature required by one proposal.
#[derive(
    CandidType, Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct SchemaCapability(u16);

impl SchemaCapability {
    /// Exact composite record and enum contracts.
    pub const EXACT_COMPOSITE_TYPES: Self = Self(1);
    /// Accepted check constraints.
    pub const ACCEPTED_CHECKS: Self = Self(2);
    /// Secondary indexes.
    pub const SECONDARY_INDEXES: Self = Self(3);
    /// Restrictive relations.
    pub const RESTRICTIVE_RELATIONS: Self = Self(4);
    /// Accepted database defaults.
    pub const INSERT_DEFAULTS: Self = Self(5);
    /// Generated values.
    pub const GENERATED_VALUES: Self = Self(6);
    /// Managed created/updated timestamps.
    pub const MANAGED_TIMESTAMPS: Self = Self(7);

    /// Construct a raw token for incompatibility testing and transport.
    #[must_use]
    pub const fn from_raw(value: u16) -> Self {
        Self(value)
    }

    /// Return the raw capability number.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }

    const fn is_supported(self) -> bool {
        matches!(self.0, 1..=7)
    }
}

/// Expected accepted-schema head used for optimistic application.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ExpectedAcceptedHead {
    /// The target database has no accepted schema.
    Empty,
    /// The target must match this exact accepted head.
    Exact {
        /// Nonzero accepted-schema revision.
        revision: u64,
        /// Opaque accepted-schema fingerprint.
        fingerprint: crate::ExpectedSchemaFingerprint,
    },
}

impl ExpectedAcceptedHead {
    const fn validate(&self) -> Result<(), SchemaContractError> {
        match self {
            Self::Exact { revision: 0, .. } => Err(SchemaContractError::InvalidReferenceList),
            Self::Empty | Self::Exact { .. } => Ok(()),
        }
    }
}

/// Explicit entity-to-store routing in the target database.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EntityStoreAssignment {
    entity: EntitySourceKey,
    store: TargetStoreIdentity,
}

impl EntityStoreAssignment {
    /// Construct one opaque routing assignment.
    #[must_use]
    pub const fn new(entity: EntitySourceKey, store: TargetStoreIdentity) -> Self {
        Self { entity, store }
    }

    /// Borrow the routed entity source key.
    #[must_use]
    pub const fn entity(&self) -> &EntitySourceKey {
        &self.entity
    }

    /// Return the opaque target-store identity.
    #[must_use]
    pub const fn store(&self) -> TargetStoreIdentity {
        self.store
    }
}

/// Explicit hard-cut removal operation.
#[derive(
    CandidType, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub enum SchemaRemoval {
    /// Remove an entity.
    Entity(EntitySourceKey),
    /// Remove one field from an entity.
    Field {
        /// Owning entity.
        entity: EntitySourceKey,
        /// Field identity.
        field: FieldSourceKey,
    },
    /// Remove a named type.
    Type(TypeSourceKey),
    /// Remove one accepted constraint.
    Constraint {
        /// Owning entity.
        entity: EntitySourceKey,
        /// Constraint identity.
        constraint: ConstraintSourceKey,
    },
    /// Remove one index.
    Index {
        /// Owning entity.
        entity: EntitySourceKey,
        /// Index identity.
        index: IndexSourceKey,
    },
    /// Remove one relation.
    Relation {
        /// Owning entity.
        entity: EntitySourceKey,
        /// Relation identity.
        relation: RelationSourceKey,
    },
}

/// Canonical current-form database-scoped proposal.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SchemaProposal {
    version: ProposalContractVersion,
    capabilities: Vec<SchemaCapability>,
    target_database: TargetDatabaseIdentity,
    submission_key: SchemaSubmissionKey,
    expected_head: ExpectedAcceptedHead,
    fragments: Vec<SchemaFragment>,
    assignments: Vec<EntityStoreAssignment>,
    removals: Vec<SchemaRemoval>,
}

impl SchemaProposal {
    /// Compose the public transport form from already selected fragments.
    ///
    /// This validates contract-local closure only. IcyDB still treats the
    /// result as untrusted and resolves target ownership, accepted references,
    /// capabilities, and catalog-native mutation semantics during application.
    ///
    /// # Errors
    ///
    /// Returns a typed contract error for bounds, duplicate definitions,
    /// ambiguous routing, removal conflicts, or malformed nested data.
    #[expect(
        clippy::too_many_lines,
        reason = "composition validates and canonicalizes one atomic public envelope"
    )]
    pub fn try_compose(
        mut capabilities: Vec<SchemaCapability>,
        target_database: TargetDatabaseIdentity,
        submission_key: SchemaSubmissionKey,
        expected_head: ExpectedAcceptedHead,
        mut fragments: Vec<SchemaFragment>,
        mut assignments: Vec<EntityStoreAssignment>,
        mut removals: Vec<SchemaRemoval>,
    ) -> Result<Self, SchemaContractError> {
        check_len(
            "proposal capabilities",
            capabilities.len(),
            MAX_SCHEMA_CAPABILITIES,
        )?;
        check_len(
            "proposal fragments",
            fragments.len(),
            MAX_SCHEMA_PROPOSAL_FRAGMENTS,
        )?;
        check_len(
            "proposal assignments",
            assignments.len(),
            MAX_SCHEMA_ASSIGNMENTS,
        )?;
        check_len("proposal removals", removals.len(), MAX_SCHEMA_REMOVALS)?;
        expected_head.validate()?;
        capabilities.sort_unstable();
        ensure_no_adjacent_duplicates(&capabilities)?;
        if capabilities
            .iter()
            .any(|capability| !capability.is_supported())
        {
            return Err(SchemaContractError::UnsupportedCapability);
        }
        for fragment in &fragments {
            fragment.validate()?;
        }
        let mut keyed_fragments = fragments
            .into_iter()
            .map(|fragment| encode_schema_fragment(&fragment).map(|bytes| (bytes, fragment)))
            .collect::<Result<Vec<_>, _>>()?;
        keyed_fragments.sort_by(|left, right| left.0.cmp(&right.0));
        fragments = keyed_fragments
            .into_iter()
            .map(|(_, fragment)| fragment)
            .collect();
        assignments.sort_by(|left, right| left.entity.cmp(&right.entity));
        ensure_no_adjacent_duplicates_by(&assignments, |assignment| &assignment.entity)?;
        removals.sort();
        ensure_no_adjacent_duplicates(&removals)?;

        let mut entity_definitions = BTreeMap::new();
        let mut type_definitions = BTreeMap::new();
        let mut field_definitions = BTreeSet::new();
        let mut constraint_definitions = BTreeSet::new();
        let mut index_definitions = BTreeSet::new();
        let mut relation_definitions = BTreeSet::new();
        let mut entity_names = BTreeSet::new();
        let mut type_names = BTreeSet::new();
        for fragment in &fragments {
            for entity in fragment.entities() {
                if entity_definitions
                    .insert(entity.source_key().clone(), entity)
                    .is_some()
                {
                    return Err(SchemaContractError::DuplicateSourceKey);
                }
                if !entity_names.insert(entity.name()) {
                    return Err(SchemaContractError::DuplicateEditableName);
                }
                for field in entity.fields() {
                    field_definitions
                        .insert((entity.source_key().clone(), field.source_key().clone()));
                }
                for constraint in entity.constraints() {
                    constraint_definitions
                        .insert((entity.source_key().clone(), constraint.source_key().clone()));
                }
                for index in entity.indexes() {
                    index_definitions
                        .insert((entity.source_key().clone(), index.source_key().clone()));
                }
                for relation in entity.relations() {
                    relation_definitions
                        .insert((entity.source_key().clone(), relation.source_key().clone()));
                }
            }
            for r#type in fragment.types() {
                if type_definitions
                    .insert(r#type.source_key().clone(), r#type)
                    .is_some()
                {
                    return Err(SchemaContractError::DuplicateSourceKey);
                }
                if !type_names.insert(r#type.name()) {
                    return Err(SchemaContractError::DuplicateEditableName);
                }
            }
        }
        for assignment in &assignments {
            if !entity_definitions.contains_key(assignment.entity()) {
                return Err(SchemaContractError::InvalidReferenceList);
            }
        }
        if assignments.len() != entity_definitions.len() {
            return Err(SchemaContractError::MissingEntityStoreAssignment);
        }
        for removal in &removals {
            let collides = match removal {
                SchemaRemoval::Entity(entity) => entity_definitions.contains_key(entity),
                SchemaRemoval::Field { entity, field } => {
                    field_definitions.contains(&(entity.clone(), field.clone()))
                }
                SchemaRemoval::Type(r#type) => type_definitions.contains_key(r#type),
                SchemaRemoval::Constraint { entity, constraint } => {
                    constraint_definitions.contains(&(entity.clone(), constraint.clone()))
                }
                SchemaRemoval::Index { entity, index } => {
                    index_definitions.contains(&(entity.clone(), index.clone()))
                }
                SchemaRemoval::Relation { entity, relation } => {
                    relation_definitions.contains(&(entity.clone(), relation.clone()))
                }
            };
            if collides {
                return Err(SchemaContractError::DefinitionRemovalConflict);
            }
        }
        validate_proposal_closure(
            &expected_head,
            &entity_definitions,
            &type_definitions,
            &removals,
        )?;

        Ok(Self {
            version: ProposalContractVersion::CURRENT,
            capabilities,
            target_database,
            submission_key,
            expected_head,
            fragments,
            assignments,
            removals,
        })
    }

    /// Return the contract version.
    #[must_use]
    pub const fn version(&self) -> ProposalContractVersion {
        self.version
    }

    /// Borrow required capabilities in canonical order.
    #[must_use]
    pub fn capabilities(&self) -> &[SchemaCapability] {
        &self.capabilities
    }

    /// Return the target database identity.
    #[must_use]
    pub const fn target_database(&self) -> TargetDatabaseIdentity {
        self.target_database
    }

    /// Borrow the submission key.
    #[must_use]
    pub const fn submission_key(&self) -> &SchemaSubmissionKey {
        &self.submission_key
    }

    /// Borrow the optimistic accepted-head condition.
    #[must_use]
    pub const fn expected_head(&self) -> &ExpectedAcceptedHead {
        &self.expected_head
    }

    /// Borrow reusable fragments.
    #[must_use]
    pub fn fragments(&self) -> &[SchemaFragment] {
        &self.fragments
    }

    /// Borrow canonical entity-to-store assignments.
    #[must_use]
    pub fn assignments(&self) -> &[EntityStoreAssignment] {
        &self.assignments
    }

    /// Borrow explicit removals.
    #[must_use]
    pub fn removals(&self) -> &[SchemaRemoval] {
        &self.removals
    }

    /// Compute the canonical proposal digest.
    ///
    /// # Errors
    ///
    /// Returns a typed encoding error if the proposal no longer satisfies the
    /// current bounded contract.
    pub fn digest(&self) -> Result<SchemaProposalDigest, SchemaContractError> {
        let bytes = encode_schema_proposal(self)?;
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        Ok(SchemaProposalDigest::from_bytes(digest))
    }

    pub(crate) fn validate_current(&self) -> Result<(), SchemaContractError> {
        if self.version != ProposalContractVersion::CURRENT {
            return Err(SchemaContractError::UnsupportedVersion {
                found: self.version.get(),
                supported: ProposalContractVersion::CURRENT.get(),
            });
        }
        let rebuilt = Self::try_compose(
            self.capabilities.clone(),
            self.target_database,
            self.submission_key.clone(),
            self.expected_head.clone(),
            self.fragments.clone(),
            self.assignments.clone(),
            self.removals.clone(),
        )?;
        if rebuilt != *self {
            return Err(SchemaContractError::NonCanonical);
        }
        Ok(())
    }
}

#[derive(Default)]
struct ProposalReferences {
    types: BTreeSet<TypeSourceKey>,
    relation_entities: BTreeSet<EntitySourceKey>,
    relation_fields: BTreeSet<(EntitySourceKey, FieldSourceKey)>,
}

fn validate_proposal_closure(
    expected_head: &ExpectedAcceptedHead,
    entities: &BTreeMap<EntitySourceKey, &EntityFragment>,
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    removals: &[SchemaRemoval],
) -> Result<(), SchemaContractError> {
    validate_local_type_graph(types)?;
    let mut references = ProposalReferences::default();
    for entity in entities.values() {
        collect_entity_references(entity, types, &mut references)?;
        validate_local_relation_targets(entity, entities)?;
    }
    for r#type in types.values() {
        collect_named_type_references(r#type, &mut references);
    }
    for removal in removals {
        let removes_reference = match removal {
            SchemaRemoval::Entity(entity) => references.relation_entities.contains(entity),
            SchemaRemoval::Field { entity, field } => references
                .relation_fields
                .contains(&(entity.clone(), field.clone())),
            SchemaRemoval::Type(r#type) => references.types.contains(r#type),
            SchemaRemoval::Constraint { .. }
            | SchemaRemoval::Index { .. }
            | SchemaRemoval::Relation { .. } => false,
        };
        if removes_reference {
            return Err(SchemaContractError::RemovedReference);
        }
    }
    if matches!(expected_head, ExpectedAcceptedHead::Empty)
        && (references
            .types
            .iter()
            .any(|reference| !types.contains_key(reference))
            || references
                .relation_entities
                .iter()
                .any(|reference| !entities.contains_key(reference)))
    {
        return Err(SchemaContractError::InvalidLocalReference);
    }
    Ok(())
}

fn collect_entity_references(
    entity: &EntityFragment,
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    references: &mut ProposalReferences,
) -> Result<(), SchemaContractError> {
    for field in entity.fields() {
        collect_field_references(field, types, references)?;
    }
    for relation in entity.relations() {
        references
            .relation_entities
            .insert(relation.target_entity().clone());
        references.relation_fields.extend(
            relation
                .target_fields()
                .iter()
                .cloned()
                .map(|field| (relation.target_entity().clone(), field)),
        );
    }
    for index in entity.indexes() {
        if let Some(predicate) = index.predicate() {
            collect_expression_enum_references(predicate, types, references)?;
        }
    }
    for constraint in entity.constraints() {
        collect_expression_enum_references(constraint.expression(), types, references)?;
    }
    Ok(())
}

fn collect_named_type_references(r#type: &NamedTypeFragment, references: &mut ProposalReferences) {
    match r#type {
        NamedTypeFragment::Record(record) => {
            for field in record.fields() {
                collect_field_type_reference(field.field_type(), references);
            }
        }
        NamedTypeFragment::Enum(_) => {}
        NamedTypeFragment::Newtype { inner, .. }
        | NamedTypeFragment::List { item: inner, .. }
        | NamedTypeFragment::Set { item: inner, .. } => {
            collect_field_type_reference(inner, references);
        }
        NamedTypeFragment::Map { key, value, .. } => {
            collect_field_type_reference(key, references);
            collect_field_type_reference(value, references);
        }
        NamedTypeFragment::Tuple { members, .. } => {
            for member in members {
                collect_field_type_reference(member, references);
            }
        }
    }
}

fn collect_field_references(
    field: &FieldFragment,
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    references: &mut ProposalReferences,
) -> Result<(), SchemaContractError> {
    collect_field_type_reference(field.field_type(), references);
    if let crate::FieldInsertPolicy::Default(ScalarLiteral::EnumUnit { enum_type, variant }) =
        field.insert_policy()
    {
        let FieldType::Named(field_type) = field.field_type() else {
            return Err(SchemaContractError::LiteralTypeMismatch);
        };
        if field_type != enum_type {
            return Err(SchemaContractError::LiteralTypeMismatch);
        }
        collect_enum_literal_reference(enum_type, variant, types, references)?;
    }
    Ok(())
}

fn collect_field_type_reference(field_type: &FieldType, references: &mut ProposalReferences) {
    if let FieldType::Named(reference) = field_type {
        references.types.insert(reference.clone());
    }
}

fn collect_expression_enum_references(
    expression: &SourceCheckExpr,
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    references: &mut ProposalReferences,
) -> Result<(), SchemaContractError> {
    for instruction in expression.instructions() {
        if let SourceCheckInstruction::Literal(ScalarLiteral::EnumUnit { enum_type, variant }) =
            instruction
        {
            collect_enum_literal_reference(enum_type, variant, types, references)?;
        }
    }
    Ok(())
}

fn collect_enum_literal_reference(
    enum_type: &TypeSourceKey,
    variant: &TypeSourceKey,
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    references: &mut ProposalReferences,
) -> Result<(), SchemaContractError> {
    references.types.insert(enum_type.clone());
    let Some(local) = types.get(enum_type) else {
        return Ok(());
    };
    let NamedTypeFragment::Enum(local) = local else {
        return Err(SchemaContractError::InvalidEnumLiteral);
    };
    if local
        .variants()
        .iter()
        .all(|candidate| candidate.source_key() != variant)
    {
        return Err(SchemaContractError::InvalidEnumLiteral);
    }
    Ok(())
}

fn validate_local_relation_targets(
    source: &EntityFragment,
    entities: &BTreeMap<EntitySourceKey, &EntityFragment>,
) -> Result<(), SchemaContractError> {
    for relation in source.relations() {
        let Some(target) = entities.get(relation.target_entity()) else {
            continue;
        };
        for (source_key, target_key) in relation.local_fields().iter().zip(relation.target_fields())
        {
            let source_field = source
                .fields()
                .iter()
                .find(|field| field.source_key() == source_key)
                .ok_or(SchemaContractError::InvalidLocalReference)?;
            let target_field = target
                .fields()
                .iter()
                .find(|field| field.source_key() == target_key)
                .ok_or(SchemaContractError::InvalidLocalReference)?;
            if source_field.field_type() != target_field.field_type() {
                return Err(SchemaContractError::RelationTypeMismatch);
            }
        }
    }
    Ok(())
}

fn validate_local_type_graph(
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
) -> Result<(), SchemaContractError> {
    let mut active = BTreeSet::new();
    let mut depths = BTreeMap::new();
    for source_key in types.keys() {
        type_depth(source_key, types, &mut active, &mut depths)?;
    }
    Ok(())
}

fn type_depth(
    source_key: &TypeSourceKey,
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    active: &mut BTreeSet<TypeSourceKey>,
    depths: &mut BTreeMap<TypeSourceKey, usize>,
) -> Result<usize, SchemaContractError> {
    if let Some(depth) = depths.get(source_key) {
        return Ok(*depth);
    }
    if !active.insert(source_key.clone()) || active.len() > MAX_SCHEMA_TYPE_DEPTH {
        return Err(SchemaContractError::InvalidNamedTypeGraph);
    }
    let Some(r#type) = types.get(source_key) else {
        active.remove(source_key);
        return Ok(0);
    };
    let mut max_child_depth = 0usize;
    for reference in direct_named_type_references(r#type) {
        if types.contains_key(reference) {
            max_child_depth = max_child_depth.max(type_depth(reference, types, active, depths)?);
        }
    }
    active.remove(source_key);
    let depth = max_child_depth
        .checked_add(1)
        .ok_or(SchemaContractError::InvalidNamedTypeGraph)?;
    if depth > MAX_SCHEMA_TYPE_DEPTH {
        return Err(SchemaContractError::InvalidNamedTypeGraph);
    }
    depths.insert(source_key.clone(), depth);
    Ok(depth)
}

fn direct_named_type_references(r#type: &NamedTypeFragment) -> Vec<&TypeSourceKey> {
    let mut references = Vec::new();
    match r#type {
        NamedTypeFragment::Record(record) => {
            references.extend(record.fields().iter().filter_map(|field| {
                let FieldType::Named(reference) = field.field_type() else {
                    return None;
                };
                Some(reference)
            }));
        }
        NamedTypeFragment::Enum(_) => {}
        NamedTypeFragment::Newtype { inner, .. }
        | NamedTypeFragment::List { item: inner, .. }
        | NamedTypeFragment::Set { item: inner, .. } => {
            if let FieldType::Named(reference) = inner {
                references.push(reference);
            }
        }
        NamedTypeFragment::Map { key, value, .. } => {
            if let FieldType::Named(reference) = key {
                references.push(reference);
            }
            if let FieldType::Named(reference) = value {
                references.push(reference);
            }
        }
        NamedTypeFragment::Tuple { members, .. } => {
            references.extend(members.iter().filter_map(|member| {
                let FieldType::Named(reference) = member else {
                    return None;
                };
                Some(reference)
            }));
        }
    }
    references
}

fn ensure_no_adjacent_duplicates<T>(values: &[T]) -> Result<(), SchemaContractError>
where
    T: Eq,
{
    if values.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(SchemaContractError::DuplicateSourceKey);
    }
    Ok(())
}

fn ensure_no_adjacent_duplicates_by<T, K>(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode_schema_proposal;

    fn empty_proposal() -> SchemaProposal {
        SchemaProposal::try_compose(
            Vec::new(),
            TargetDatabaseIdentity::from_bytes([1; 32]),
            SchemaSubmissionKey::try_new("proposal-version-test")
                .expect("submission key should admit"),
            ExpectedAcceptedHead::Empty,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("empty proposal should compose")
    }

    #[test]
    fn decoded_future_contract_version_fails_typed() {
        let mut proposal = empty_proposal();
        proposal.version = ProposalContractVersion::from_raw(2);
        let bytes = candid::encode_one(proposal).expect("raw future proposal should encode");

        assert_eq!(
            decode_schema_proposal(&bytes),
            Err(SchemaContractError::UnsupportedVersion {
                found: 2,
                supported: 1,
            }),
        );
    }

    #[test]
    fn decoded_unknown_capability_fails_typed() {
        let mut proposal = empty_proposal();
        proposal.capabilities = vec![SchemaCapability::from_raw(u16::MAX)];
        let bytes = candid::encode_one(proposal).expect("raw proposal should encode");

        assert_eq!(
            decode_schema_proposal(&bytes),
            Err(SchemaContractError::UnsupportedCapability),
        );
    }
}
