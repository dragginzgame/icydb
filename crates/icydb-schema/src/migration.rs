//! Canonical source-declared entity-version migration vocabulary.

use std::collections::BTreeSet;

use candid::CandidType;
use serde::{Deserialize, Deserializer, Serialize, de::Error as DeError};
use sha2::{Digest, Sha256};

use crate::{
    ConstraintSourceKey, EntitySourceKey, FieldSourceKey, MAX_SCHEMA_MIGRATION_ENTITIES,
    MAX_SCHEMA_MIGRATION_PLAN_BYTES, MAX_SCHEMA_MIGRATION_RENAMES, MAX_SCHEMA_MIGRATION_TRANSFORMS,
    RelationSourceKey, RuleSourceKey, ScalarLiteral, ScalarType, SchemaContractError,
    SchemaMigrationPlanDigest, TypeSourceKey, check_len,
};

const MIGRATION_PROGRAM_VERSION_CURRENT: u16 = 1;
const MIGRATION_PLAN_DIGEST_PROFILE: &[u8] = b"icydb.schema-migration-plan.v1";

/// Positive application-declared source version for one current entity.
#[derive(CandidType, Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct DeclaredEntityVersion(u32);

impl DeclaredEntityVersion {
    /// Construct one positive declared entity version.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaContractError::InvalidEntityVersion`] for zero.
    pub const fn try_new(value: u32) -> Result<Self, SchemaContractError> {
        if value == 0 {
            return Err(SchemaContractError::InvalidEntityVersion);
        }
        Ok(Self(value))
    }

    /// Return the authored version number.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl<'de> Deserialize<'de> for DeclaredEntityVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::try_new(u32::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

/// One exact source-name correspondence applied simultaneously with its plan.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SchemaMigrationRename {
    /// Rename one entity-local field.
    Field {
        /// Accepted-before field name.
        from: FieldSourceKey,
        /// Current target field name.
        to: FieldSourceKey,
    },
    /// Rename one named record, enum, newtype, or collection.
    NamedType {
        /// Accepted-before named-type name.
        from: TypeSourceKey,
        /// Current target named-type name.
        to: TypeSourceKey,
    },
    /// Rename one unit or payload enum variant below an accepted-before enum.
    EnumVariant {
        /// Accepted-before owning enum name.
        named_type: TypeSourceKey,
        /// Accepted-before variant name.
        from: TypeSourceKey,
        /// Current target variant name.
        to: TypeSourceKey,
    },
    /// Rename one field below an accepted-before named record.
    RecordField {
        /// Accepted-before owning record name.
        named_type: TypeSourceKey,
        /// Accepted-before record-field name.
        from: FieldSourceKey,
        /// Current target record-field name.
        to: FieldSourceKey,
    },
    /// Rename one entity-local relation.
    Relation {
        /// Accepted-before relation name.
        from: RelationSourceKey,
        /// Current target relation name.
        to: RelationSourceKey,
    },
    /// Rename one entity-local accepted constraint.
    Constraint {
        /// Accepted-before constraint name.
        from: ConstraintSourceKey,
        /// Current target constraint name.
        to: ConstraintSourceKey,
    },
    /// Rename one durable rule below an accepted-before named type.
    Rule {
        /// Accepted-before owning named type.
        named_type: TypeSourceKey,
        /// Accepted-before local rule name.
        from: RuleSourceKey,
        /// Current target local rule name.
        to: RuleSourceKey,
    },
}

impl SchemaMigrationRename {
    const fn sort_key(&self) -> (u8, &str, &str, &str) {
        match self {
            Self::Field { from, to } => (0, "", from.as_str(), to.as_str()),
            Self::NamedType { from, to } => (1, "", from.as_str(), to.as_str()),
            Self::EnumVariant {
                named_type,
                from,
                to,
            } => (2, named_type.as_str(), from.as_str(), to.as_str()),
            Self::RecordField {
                named_type,
                from,
                to,
            } => (3, named_type.as_str(), from.as_str(), to.as_str()),
            Self::Relation { from, to } => (4, "", from.as_str(), to.as_str()),
            Self::Constraint { from, to } => (5, "", from.as_str(), to.as_str()),
            Self::Rule {
                named_type,
                from,
                to,
            } => (6, named_type.as_str(), from.as_str(), to.as_str()),
        }
    }

    fn validate(&self) -> Result<(), SchemaContractError> {
        let (_, _, from, to) = self.sort_key();
        if from == to {
            return Err(SchemaContractError::InvalidMigrationPlan);
        }
        Ok(())
    }
}

/// One closed deterministic historical-row transform declaration.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SchemaMigrationTransform {
    /// Fill one current target field with one exact typed literal.
    Fill {
        /// Current target field.
        to: FieldSourceKey,
        /// Exact target literal.
        literal: ScalarLiteral,
    },
    /// Copy one accepted-before field into one distinct current target field.
    Copy {
        /// Accepted-before source field.
        from: FieldSourceKey,
        /// Current target field.
        to: FieldSourceKey,
    },
    /// Convert one scalar through the closed exact checked-cast matrix.
    CheckedCast {
        /// Accepted-before source field.
        from: FieldSourceKey,
        /// Current target field.
        to: FieldSourceKey,
        /// Exact current target scalar contract.
        target: ScalarType,
    },
    /// Preserve a non-null predecessor value or use one exact fallback.
    Coalesce {
        /// Accepted-before optional source field.
        from: FieldSourceKey,
        /// Current required target field.
        to: FieldSourceKey,
        /// Exact fallback literal.
        literal: ScalarLiteral,
    },
}

impl SchemaMigrationTransform {
    /// Borrow the current target field.
    #[must_use]
    pub const fn target(&self) -> &FieldSourceKey {
        match self {
            Self::Fill { to, .. }
            | Self::Copy { to, .. }
            | Self::CheckedCast { to, .. }
            | Self::Coalesce { to, .. } => to,
        }
    }

    const fn sort_key(&self) -> (&str, u8, &str) {
        match self {
            Self::Fill { to, .. } => (to.as_str(), 0, ""),
            Self::Copy { from, to } => (to.as_str(), 1, from.as_str()),
            Self::CheckedCast { from, to, .. } => (to.as_str(), 2, from.as_str()),
            Self::Coalesce { from, to, .. } => (to.as_str(), 3, from.as_str()),
        }
    }

    fn validate(&self) -> Result<(), SchemaContractError> {
        match self {
            Self::Fill { literal, .. } | Self::Coalesce { literal, .. } => literal.validate(),
            Self::Copy { from, to } if from == to => {
                Err(SchemaContractError::InvalidMigrationTransform)
            }
            Self::CheckedCast { target, .. } if !is_v1_cast_target(*target) => {
                Err(SchemaContractError::InvalidMigrationTransform)
            }
            Self::Copy { .. } | Self::CheckedCast { .. } => Ok(()),
        }
    }
}

/// One immediate-predecessor transition for a current entity declaration.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EntityMigration {
    entity: EntitySourceKey,
    from: DeclaredEntityVersion,
    from_name: Option<EntitySourceKey>,
    renames: Vec<SchemaMigrationRename>,
    transforms: Vec<SchemaMigrationTransform>,
}

impl EntityMigration {
    /// Construct and canonicalize one entity transition.
    ///
    /// # Errors
    ///
    /// Returns a typed migration error for an empty transition, a no-op
    /// entity rename, duplicate rename ownership, duplicate transform target,
    /// or invalid transform.
    pub fn try_new(
        entity: EntitySourceKey,
        from: DeclaredEntityVersion,
        from_name: Option<EntitySourceKey>,
        mut renames: Vec<SchemaMigrationRename>,
        mut transforms: Vec<SchemaMigrationTransform>,
    ) -> Result<Self, SchemaContractError> {
        check_len(
            "migration renames",
            renames.len(),
            MAX_SCHEMA_MIGRATION_RENAMES,
        )?;
        check_len(
            "migration transforms",
            transforms.len(),
            MAX_SCHEMA_MIGRATION_TRANSFORMS,
        )?;
        if from_name.as_ref() == Some(&entity) {
            return Err(SchemaContractError::InvalidMigrationPlan);
        }
        if from_name.is_none() && renames.is_empty() && transforms.is_empty() {
            return Err(SchemaContractError::InvalidMigrationPlan);
        }
        for rename in &renames {
            rename.validate()?;
        }
        for transform in &transforms {
            transform.validate()?;
        }
        ensure_distinct_rename_ownership(&renames)?;
        ensure_distinct_transform_targets(&transforms)?;
        renames.sort_unstable_by(|left, right| left.sort_key().cmp(&right.sort_key()));
        transforms.sort_unstable_by(|left, right| left.sort_key().cmp(&right.sort_key()));
        Ok(Self {
            entity,
            from,
            from_name,
            renames,
            transforms,
        })
    }

    /// Borrow the current target entity key.
    #[must_use]
    pub const fn entity(&self) -> &EntitySourceKey {
        &self.entity
    }

    /// Return the accepted-before declared entity version.
    #[must_use]
    pub const fn from(&self) -> DeclaredEntityVersion {
        self.from
    }

    /// Borrow an optional accepted-before entity name.
    #[must_use]
    pub const fn from_name(&self) -> Option<&EntitySourceKey> {
        self.from_name.as_ref()
    }

    /// Borrow canonical rename operations.
    #[must_use]
    pub fn renames(&self) -> &[SchemaMigrationRename] {
        &self.renames
    }

    /// Borrow canonical historical transforms.
    #[must_use]
    pub fn transforms(&self) -> &[SchemaMigrationTransform] {
        &self.transforms
    }
}

/// One canonical database-scoped coordinated migration plan.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SchemaMigrationPlan {
    program_version: u16,
    transitions: Vec<EntityMigration>,
    digest: SchemaMigrationPlanDigest,
}

impl SchemaMigrationPlan {
    /// Construct, canonicalize, bound, and digest one coordinated plan.
    ///
    /// # Errors
    ///
    /// Returns a typed migration or encoded-size error for an empty,
    /// duplicate, ambiguous, or oversized plan.
    pub fn try_new(mut transitions: Vec<EntityMigration>) -> Result<Self, SchemaContractError> {
        check_len(
            "migration entity transitions",
            transitions.len(),
            MAX_SCHEMA_MIGRATION_ENTITIES,
        )?;
        if transitions.is_empty() {
            return Err(SchemaContractError::InvalidMigrationPlan);
        }
        transitions.sort_unstable_by(|left, right| left.entity.cmp(&right.entity));
        if transitions
            .windows(2)
            .any(|pair| pair[0].entity == pair[1].entity)
        {
            return Err(SchemaContractError::DuplicateMigrationTarget);
        }
        let mut predecessor_entities = BTreeSet::new();
        for transition in &transitions {
            let predecessor = transition.from_name.as_ref().unwrap_or(&transition.entity);
            if !predecessor_entities.insert(predecessor.clone()) {
                return Err(SchemaContractError::DuplicateMigrationSource);
            }
        }
        let transition_bytes =
            candid::encode_one(&transitions).map_err(|_| SchemaContractError::Encode)?;
        let digest = digest_plan_transitions(&transition_bytes);
        let plan = Self {
            program_version: MIGRATION_PROGRAM_VERSION_CURRENT,
            transitions,
            digest,
        };
        ensure_plan_bound(&plan)?;
        Ok(plan)
    }

    /// Return the closed migration-program version.
    #[must_use]
    pub const fn program_version(&self) -> u16 {
        self.program_version
    }

    /// Borrow canonical entity transitions.
    #[must_use]
    pub fn transitions(&self) -> &[EntityMigration] {
        &self.transitions
    }

    /// Return the exact canonical plan digest.
    #[must_use]
    pub const fn digest(&self) -> SchemaMigrationPlanDigest {
        self.digest
    }

    pub(crate) fn validate(&self) -> Result<(), SchemaContractError> {
        if self.program_version != MIGRATION_PROGRAM_VERSION_CURRENT {
            return Err(SchemaContractError::UnsupportedMigrationProgramVersion {
                found: self.program_version,
                supported: MIGRATION_PROGRAM_VERSION_CURRENT,
            });
        }
        let rebuilt = Self::try_new(self.transitions.clone())?;
        if rebuilt != *self {
            return Err(SchemaContractError::NonCanonical);
        }
        Ok(())
    }
}

/// Encode one bounded canonical migration plan.
///
/// # Errors
///
/// Returns a typed validation, encoding, or size error.
pub fn encode_schema_migration_plan(
    plan: &SchemaMigrationPlan,
) -> Result<Vec<u8>, SchemaContractError> {
    plan.validate()?;
    let bytes = candid::encode_one(plan).map_err(|_| SchemaContractError::Encode)?;
    if bytes.len() > MAX_SCHEMA_MIGRATION_PLAN_BYTES {
        return Err(SchemaContractError::EncodedTooLarge {
            len: bytes.len(),
            max: MAX_SCHEMA_MIGRATION_PLAN_BYTES,
        });
    }
    Ok(bytes)
}

/// Decode one bounded canonical migration plan.
///
/// # Errors
///
/// Returns a typed size, decoding, version, validation, or canonicalization
/// error. Obsolete forms are never translated.
pub fn decode_schema_migration_plan(
    bytes: &[u8],
) -> Result<SchemaMigrationPlan, SchemaContractError> {
    if bytes.len() > MAX_SCHEMA_MIGRATION_PLAN_BYTES {
        return Err(SchemaContractError::EncodedTooLarge {
            len: bytes.len(),
            max: MAX_SCHEMA_MIGRATION_PLAN_BYTES,
        });
    }
    let plan = candid::decode_one::<SchemaMigrationPlan>(bytes)
        .map_err(|_| SchemaContractError::Decode)?;
    plan.validate()?;
    if encode_schema_migration_plan(&plan)? != bytes {
        return Err(SchemaContractError::NonCanonical);
    }
    Ok(plan)
}

fn ensure_plan_bound(plan: &SchemaMigrationPlan) -> Result<(), SchemaContractError> {
    let bytes = candid::encode_one(plan).map_err(|_| SchemaContractError::Encode)?;
    if bytes.len() > MAX_SCHEMA_MIGRATION_PLAN_BYTES {
        return Err(SchemaContractError::EncodedTooLarge {
            len: bytes.len(),
            max: MAX_SCHEMA_MIGRATION_PLAN_BYTES,
        });
    }
    Ok(())
}

fn digest_plan_transitions(bytes: &[u8]) -> SchemaMigrationPlanDigest {
    let mut hasher = Sha256::new();
    hasher.update(MIGRATION_PLAN_DIGEST_PROFILE);
    hasher.update(MIGRATION_PROGRAM_VERSION_CURRENT.to_be_bytes());
    hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
    SchemaMigrationPlanDigest::from_bytes(hasher.finalize().into())
}

fn ensure_distinct_rename_ownership(
    renames: &[SchemaMigrationRename],
) -> Result<(), SchemaContractError> {
    let mut sources = BTreeSet::new();
    let mut targets = BTreeSet::new();
    for rename in renames {
        let (kind, owner, from, to) = rename.sort_key();
        if !sources.insert((kind, owner, from)) {
            return Err(SchemaContractError::DuplicateMigrationSource);
        }
        if !targets.insert((kind, owner, to)) {
            return Err(SchemaContractError::DuplicateMigrationTarget);
        }
    }
    Ok(())
}

fn ensure_distinct_transform_targets(
    transforms: &[SchemaMigrationTransform],
) -> Result<(), SchemaContractError> {
    let mut targets = BTreeSet::new();
    for transform in transforms {
        if !targets.insert(transform.target()) {
            return Err(SchemaContractError::DuplicateMigrationTarget);
        }
    }
    Ok(())
}

const fn is_v1_cast_target(target: ScalarType) -> bool {
    matches!(
        target,
        ScalarType::Int8
            | ScalarType::Int16
            | ScalarType::Int32
            | ScalarType::Int64
            | ScalarType::Int128
            | ScalarType::Nat8
            | ScalarType::Nat16
            | ScalarType::Nat32
            | ScalarType::Nat64
            | ScalarType::Nat128
            | ScalarType::Decimal { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entity(value: &str) -> EntitySourceKey {
        EntitySourceKey::try_new(value).expect("entity source should admit")
    }

    fn field(value: &str) -> FieldSourceKey {
        FieldSourceKey::try_new(value).expect("field source should admit")
    }

    fn transition(entity_name: &str, from_name: &str) -> EntityMigration {
        EntityMigration::try_new(
            entity(entity_name),
            DeclaredEntityVersion::try_new(1).expect("version should admit"),
            Some(entity(from_name)),
            vec![SchemaMigrationRename::Field {
                from: field("old_value"),
                to: field("value"),
            }],
            Vec::new(),
        )
        .expect("transition should admit")
    }

    #[test]
    fn declared_entity_version_is_strictly_positive() {
        assert_eq!(
            DeclaredEntityVersion::try_new(0),
            Err(SchemaContractError::InvalidEntityVersion),
        );
        assert_eq!(
            DeclaredEntityVersion::try_new(1)
                .expect("version one should admit")
                .get(),
            1,
        );
    }

    #[test]
    fn plan_canonicalization_is_declaration_order_independent() {
        let account = transition("Account", "User");
        let article = transition("Article", "Post");
        let first = SchemaMigrationPlan::try_new(vec![account.clone(), article.clone()])
            .expect("plan should admit");
        let reverse = SchemaMigrationPlan::try_new(vec![article, account])
            .expect("reverse plan should admit");

        assert_eq!(first, reverse);
        assert_eq!(first.digest(), reverse.digest());
        let encoded = encode_schema_migration_plan(&first).expect("plan should encode");
        assert_eq!(
            decode_schema_migration_plan(&encoded).expect("plan should decode"),
            first,
        );
    }

    #[test]
    fn plan_digest_has_one_fixed_current_vector() {
        let plan = SchemaMigrationPlan::try_new(vec![transition("Account", "User")])
            .expect("plan should admit");
        assert_eq!(
            plan.digest().to_bytes(),
            [
                112, 32, 202, 185, 208, 245, 26, 207, 64, 171, 3, 52, 250, 32, 61, 253, 141, 53,
                112, 54, 108, 123, 104, 157, 229, 10, 70, 161, 138, 164, 77, 16,
            ],
        );
    }

    #[test]
    fn duplicate_targets_predecessors_and_transform_targets_reject() {
        let account = transition("Account", "User");
        assert_eq!(
            SchemaMigrationPlan::try_new(vec![account.clone(), account]),
            Err(SchemaContractError::DuplicateMigrationTarget),
        );
        assert_eq!(
            SchemaMigrationPlan::try_new(vec![
                transition("Account", "User"),
                transition("Profile", "User"),
            ]),
            Err(SchemaContractError::DuplicateMigrationSource),
        );
        assert_eq!(
            EntityMigration::try_new(
                entity("Account"),
                DeclaredEntityVersion::try_new(1).expect("version should admit"),
                None,
                Vec::new(),
                vec![
                    SchemaMigrationTransform::Fill {
                        to: field("value"),
                        literal: ScalarLiteral::Nat(1),
                    },
                    SchemaMigrationTransform::Copy {
                        from: field("old_value"),
                        to: field("value"),
                    },
                ],
            ),
            Err(SchemaContractError::DuplicateMigrationTarget),
        );
    }

    #[test]
    fn obsolete_programs_and_oversized_transport_fail_closed() {
        let mut plan = SchemaMigrationPlan::try_new(vec![transition("Account", "User")])
            .expect("plan should admit");
        plan.program_version = 2;
        let bytes = candid::encode_one(plan).expect("raw future plan should encode");
        assert_eq!(
            decode_schema_migration_plan(&bytes),
            Err(SchemaContractError::UnsupportedMigrationProgramVersion {
                found: 2,
                supported: 1,
            }),
        );
        assert!(matches!(
            decode_schema_migration_plan(&vec![0; MAX_SCHEMA_MIGRATION_PLAN_BYTES + 1]),
            Err(SchemaContractError::EncodedTooLarge { .. }),
        ));
    }
}
