//! Canonical per-entity generated-source meaning.

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use crate::{
    DeclaredEntityVersion, EntityFragment, EntitySourceDigest, EntitySourceKey, FieldFragment,
    FieldSourceKey, NamedTypeFragment, RelationFragment, SchemaContractError, SchemaMigrationPlan,
    SchemaProposal, TypeSourceKey,
};

const ENTITY_SOURCE_DIGEST_PROFILE: &[u8] = b"icydb.entity-source-meaning.wire.v1";

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
            field
                .field_type()
                .append_named_type_dependencies(&mut pending_types);
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
                    field
                        .field_type()
                        .append_named_type_dependencies(pending_types);
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
        definition.append_named_type_dependencies(&mut pending);
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

#[cfg(test)]
mod tests;
