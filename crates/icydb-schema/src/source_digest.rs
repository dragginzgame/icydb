//! Canonical per-entity generated-source meaning.

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use crate::{
    DeclaredEntityVersion, EntityFragment, EntitySourceDigest, EntitySourceKey, FieldFragment,
    FieldSourceKey, FieldType, NamedTypeFragment, RelationFragment, SchemaContractError,
    SchemaMigrationPlan, SchemaProposal, TypeSourceKey,
};

const ENTITY_SOURCE_DIGEST_PROFILE: &[u8] = b"icydb.entity-source-meaning.v1";

impl SchemaProposal {
    /// Compute the canonical generated-owned meaning for one current entity.
    ///
    /// The digest excludes the declared entity version and migration plan. It
    /// includes the complete entity contract, its reachable named-type
    /// closure, and the exact target-field contracts referenced by relations.
    ///
    /// # Errors
    ///
    /// Returns a typed reference or encoding error when the requested entity
    /// or one of its current relation/type dependencies is absent.
    pub fn entity_source_digest(
        &self,
        source: &EntitySourceKey,
    ) -> Result<EntitySourceDigest, SchemaContractError> {
        self.entity_source_digest_with_targets(source, &BTreeMap::new())
    }

    /// Compute source meaning with relation targets mapped to the explicit
    /// predecessor entity names in this proposal's migration plan.
    ///
    /// This comparison proof changes only target names, not target fields or
    /// reachable type contracts. The planner compares it to accepted lineage to
    /// admit a dependency-only transition; it is not runtime schema authority.
    ///
    /// # Errors
    ///
    /// Returns a typed reference or encoding error for invalid dependencies or
    /// colliding predecessor target names.
    pub fn entity_source_digest_before_entity_renames(
        &self,
        source: &EntitySourceKey,
    ) -> Result<EntitySourceDigest, SchemaContractError> {
        let targets = self
            .migration()
            .into_iter()
            .flat_map(SchemaMigrationPlan::transitions)
            .filter_map(|transition| {
                transition
                    .from_name()
                    .map(|from| (transition.entity(), from))
            })
            .collect();
        self.entity_source_digest_with_targets(source, &targets)
    }

    // Share the exact canonical encoding with ordinary lineage digests. Only
    // explicit relation target correspondences may differ in the proof.
    fn entity_source_digest_with_targets(
        &self,
        source: &EntitySourceKey,
        target_names: &BTreeMap<&EntitySourceKey, &EntitySourceKey>,
    ) -> Result<EntitySourceDigest, SchemaContractError> {
        let mut entities = BTreeMap::new();
        let mut types = BTreeMap::new();
        for fragment in self.fragments() {
            for entity in fragment.entities() {
                entities.insert(entity.source_key().clone(), entity);
            }
            for definition in fragment.types() {
                types.insert(definition.source_key().clone(), definition);
            }
        }
        let entity = entities
            .get(source)
            .copied()
            .ok_or(SchemaContractError::InvalidMigrationReference)?;
        let normalized = normalized_entity(entity, target_names)?;

        let mut pending_types = Vec::new();
        for field in entity.fields() {
            collect_field_type_sources(field.field_type(), &mut pending_types);
        }
        let mut relation_targets = relation_target_meanings(entity, &entities, &mut pending_types)?;
        if !target_names.is_empty() {
            for (target, _) in &mut relation_targets {
                if let Some(predecessor) = target_names.get(target) {
                    target.clone_from(predecessor);
                }
            }
            // Renames can reverse lexical order. Preserve canonical ordering
            // without merging ambiguous predecessor targets.
            crate::compact_sort_unstable_by(&mut relation_targets, |left, right| {
                left.0.cmp(&right.0)
            });
            if relation_targets
                .windows(2)
                .any(|pair| pair[0].0 == pair[1].0)
            {
                return Err(SchemaContractError::InvalidMigrationReference);
            }
        }
        let reachable_types = reachable_type_meanings(&types, pending_types)?;
        let encoded = crate::codec::encode_entity_source_meaning(
            &normalized,
            &relation_targets,
            &reachable_types,
        )?;

        let mut hasher = Sha256::new();
        hasher.update(ENTITY_SOURCE_DIGEST_PROFILE);
        hasher.update(
            u64::try_from(encoded.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        hasher.update(encoded);
        Ok(EntitySourceDigest::from_bytes(hasher.finalize().into()))
    }
}

fn normalized_entity(
    entity: &EntityFragment,
    target_names: &BTreeMap<&EntitySourceKey, &EntitySourceKey>,
) -> Result<EntityFragment, SchemaContractError> {
    let relations = entity
        .relations()
        .iter()
        .map(|relation| {
            let Some(predecessor) = target_names.get(relation.target_entity()) else {
                return Ok(relation.clone());
            };
            RelationFragment::try_new(
                relation.name().clone(),
                relation.source().clone(),
                (*predecessor).clone(),
                relation.target_fields().to_vec(),
                relation.on_delete(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    EntityFragment::try_new(
        entity.name().clone(),
        DeclaredEntityVersion::try_new(1)?,
        entity.fields().to_vec(),
        entity.primary_key().to_vec(),
        entity.indexes().to_vec(),
        relations,
        entity.constraints().to_vec(),
    )
}

type RelationTargetMeaning = (EntitySourceKey, Vec<(FieldSourceKey, FieldFragment)>);

fn relation_target_meanings(
    entity: &EntityFragment,
    entities: &BTreeMap<EntitySourceKey, &EntityFragment>,
    pending_types: &mut Vec<TypeSourceKey>,
) -> Result<Vec<RelationTargetMeaning>, SchemaContractError> {
    let mut targets = BTreeMap::<EntitySourceKey, BTreeSet<FieldSourceKey>>::new();
    for relation in entity.relations() {
        targets
            .entry(relation.target_entity().clone())
            .or_default()
            .extend(relation.target_fields().iter().cloned());
    }
    targets
        .into_iter()
        .map(|(target_source, field_sources)| {
            let target = entities
                .get(&target_source)
                .copied()
                .ok_or(SchemaContractError::InvalidMigrationReference)?;
            let fields = field_sources
                .into_iter()
                .map(|field_source| {
                    let field = target
                        .fields()
                        .iter()
                        .find(|field| field.source_key() == &field_source)
                        .cloned()
                        .ok_or(SchemaContractError::InvalidMigrationReference)?;
                    collect_field_type_sources(field.field_type(), pending_types);
                    Ok((field_source, field))
                })
                .collect::<Result<Vec<_>, SchemaContractError>>()?;
            Ok((target_source, fields))
        })
        .collect()
}

fn reachable_type_meanings(
    types: &BTreeMap<TypeSourceKey, &NamedTypeFragment>,
    mut pending: Vec<TypeSourceKey>,
) -> Result<Vec<NamedTypeFragment>, SchemaContractError> {
    let mut reachable = BTreeSet::new();
    while let Some(source) = pending.pop() {
        if !reachable.insert(source.clone()) {
            continue;
        }
        let definition = types
            .get(&source)
            .copied()
            .ok_or(SchemaContractError::InvalidMigrationReference)?;
        collect_named_type_sources(definition, &mut pending);
    }
    reachable
        .into_iter()
        .map(|source| {
            types
                .get(&source)
                .copied()
                .cloned()
                .ok_or(SchemaContractError::InvalidMigrationReference)
        })
        .collect()
}

fn collect_named_type_sources(definition: &NamedTypeFragment, pending: &mut Vec<TypeSourceKey>) {
    match definition {
        NamedTypeFragment::Record(record) => {
            for field in record.fields() {
                collect_field_type_sources(field.field_type(), pending);
            }
        }
        NamedTypeFragment::Enum(r#enum) => {
            for variant in r#enum.variants() {
                if let Some(payload) = variant.payload() {
                    collect_field_type_sources(payload, pending);
                }
            }
        }
        NamedTypeFragment::Newtype { inner, .. }
        | NamedTypeFragment::List { item: inner, .. }
        | NamedTypeFragment::Set { item: inner, .. } => {
            collect_field_type_sources(inner, pending);
        }
        NamedTypeFragment::Map { key, value, .. } => {
            collect_field_type_sources(key, pending);
            collect_field_type_sources(value, pending);
        }
        NamedTypeFragment::Tuple { members, .. } => {
            for member in members {
                collect_field_type_sources(member.field_type(), pending);
            }
        }
    }
}

fn collect_field_type_sources(field_type: &FieldType, pending: &mut Vec<TypeSourceKey>) {
    match field_type {
        FieldType::List(inner) => collect_field_type_sources(inner, pending),
        FieldType::Named(source) => pending.push(source.clone()),
        FieldType::Scalar(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        DeclaredEntityVersion, EntityFragment, EntityMigration, EntitySourceKey,
        EntityStoreAssignment, ExpectedAcceptedHead, FieldFragment, FieldInsertPolicy,
        FieldSourceKey, FieldType, NamedTypeFragment, RelationDeleteAction, RelationFragment,
        RelationSourceFragment, ScalarType, SchemaCapability, SchemaFragment, SchemaMigrationPlan,
        SchemaName, SchemaProposal, SchemaSubmissionKey, TargetDatabaseIdentity,
        TargetStoreIdentity, TypeSourceKey,
    };

    fn proposal(version: u32, field_name: &str) -> SchemaProposal {
        let id = FieldFragment::new(
            SchemaName::try_new(field_name).expect("field name should admit"),
            FieldType::Scalar(crate::ScalarType::Nat64),
            false,
            FieldInsertPolicy::Required,
            None,
        );
        let entity = EntityFragment::try_new(
            SchemaName::try_new("User").expect("entity name should admit"),
            DeclaredEntityVersion::try_new(version).expect("version should admit"),
            vec![id],
            vec![FieldSourceKey::try_new(field_name).expect("field key should admit")],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("entity should admit");
        SchemaProposal::try_compose(
            Vec::new(),
            TargetDatabaseIdentity::from_bytes([1; 32]),
            SchemaSubmissionKey::try_new("source-digest").expect("submission should admit"),
            ExpectedAcceptedHead::Empty,
            vec![SchemaFragment::try_new(vec![entity], Vec::new()).expect("fragment should admit")],
            vec![EntityStoreAssignment::new(
                EntitySourceKey::try_new("User").expect("entity key should admit"),
                TargetStoreIdentity::from_bytes([2; 32]),
            )],
            Vec::new(),
            None,
        )
        .expect("proposal should admit")
    }

    #[test]
    fn source_digest_ignores_declared_version_but_not_entity_meaning() {
        let source = EntitySourceKey::try_new("User").expect("source should admit");
        assert_eq!(
            proposal(1, "id")
                .entity_source_digest(&source)
                .expect("digest should derive"),
            proposal(7, "id")
                .entity_source_digest(&source)
                .expect("digest should derive"),
        );
        assert_ne!(
            proposal(1, "id")
                .entity_source_digest(&source)
                .expect("digest should derive"),
            proposal(1, "other")
                .entity_source_digest(&source)
                .expect("digest should derive"),
        );
    }

    // Zebra -> Alpha crosses Middle in canonical target order. Payload keeps a
    // reachable named-type contract in the owner's source meaning.
    #[expect(
        clippy::too_many_lines,
        reason = "one predecessor/successor fixture keeps the complete dependency contract visible"
    )]
    fn dependency_proposal(
        renamed: bool,
        payload: ScalarType,
        target: FieldInsertPolicy,
    ) -> SchemaProposal {
        let name = |value| SchemaName::try_new(value).unwrap();
        let key = |value| EntitySourceKey::try_new(value).unwrap();
        let field = |value| FieldSourceKey::try_new(value).unwrap();
        let id = |policy| {
            FieldFragment::new(
                name("id"),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                policy,
                None,
            )
        };
        let target_name = if renamed { "Alpha" } else { "Zebra" };
        let mut entities = Vec::new();
        for entity in [target_name, "Middle", "Holder"] {
            let mut fields = vec![id(if entity == target_name {
                target.clone()
            } else {
                FieldInsertPolicy::Required
            })];
            let relations = if entity == "Holder" {
                fields.push(FieldFragment::new(
                    name("payload"),
                    FieldType::Named(TypeSourceKey::try_new("Payload").unwrap()),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ));
                [target_name, "Middle"]
                    .into_iter()
                    .enumerate()
                    .map(|(ordinal, target)| {
                        RelationFragment::try_new(
                            name(if ordinal == 0 { "first" } else { "second" }),
                            RelationSourceFragment::direct(vec![field("id")]),
                            key(target),
                            vec![field("id")],
                            RelationDeleteAction::Restrict,
                        )
                        .unwrap()
                    })
                    .collect()
            } else {
                Vec::new()
            };
            entities.push(
                EntityFragment::try_new(
                    name(entity),
                    DeclaredEntityVersion::try_new(if renamed && entity != "Middle" {
                        2
                    } else {
                        1
                    })
                    .unwrap(),
                    fields,
                    vec![field("id")],
                    Vec::new(),
                    relations,
                    Vec::new(),
                )
                .unwrap(),
            );
        }
        let migration = renamed.then(|| {
            SchemaMigrationPlan::try_new(vec![
                EntityMigration::try_new(
                    key("Alpha"),
                    DeclaredEntityVersion::try_new(1).unwrap(),
                    Some(key("Zebra")),
                    Vec::new(),
                    Vec::new(),
                )
                .unwrap(),
                EntityMigration::try_new(
                    key("Holder"),
                    DeclaredEntityVersion::try_new(1).unwrap(),
                    None,
                    Vec::new(),
                    Vec::new(),
                )
                .unwrap(),
            ])
            .unwrap()
        });
        let assignments = entities
            .iter()
            .map(|entity| {
                EntityStoreAssignment::new(
                    entity.source_key().clone(),
                    TargetStoreIdentity::from_bytes([2; 32]),
                )
            })
            .collect();
        let mut capabilities = vec![SchemaCapability::RESTRICTIVE_RELATIONS];
        if renamed {
            capabilities.push(SchemaCapability::VERSIONED_MIGRATIONS);
        }
        SchemaProposal::try_compose(
            capabilities,
            TargetDatabaseIdentity::from_bytes([1; 32]),
            SchemaSubmissionKey::try_new("dependency").unwrap(),
            ExpectedAcceptedHead::Empty,
            vec![
                SchemaFragment::try_new(
                    entities,
                    vec![NamedTypeFragment::newtype(
                        name("Payload"),
                        FieldType::Scalar(payload),
                    )],
                )
                .unwrap(),
            ],
            assignments,
            Vec::new(),
            migration,
        )
        .unwrap()
    }

    #[test]
    fn entity_rename_dependency_proof_preserves_canonical_order_and_complete_meaning() {
        let holder = EntitySourceKey::try_new("Holder").unwrap();
        let before = dependency_proposal(false, ScalarType::Nat64, FieldInsertPolicy::Required)
            .entity_source_digest(&holder)
            .unwrap();
        let renamed = dependency_proposal(true, ScalarType::Nat64, FieldInsertPolicy::Required);
        assert_ne!(before, renamed.entity_source_digest(&holder).unwrap());
        assert_eq!(
            before,
            renamed
                .entity_source_digest_before_entity_renames(&holder)
                .unwrap()
        );
        for (payload, target) in [
            (ScalarType::Nat32, FieldInsertPolicy::Required),
            (
                ScalarType::Nat64,
                FieldInsertPolicy::Default(crate::ScalarLiteral::Nat(1)),
            ),
        ] {
            assert_ne!(
                before,
                dependency_proposal(true, payload, target)
                    .entity_source_digest_before_entity_renames(&holder)
                    .unwrap()
            );
        }
    }
}
