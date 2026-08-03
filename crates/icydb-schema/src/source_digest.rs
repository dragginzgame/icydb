//! Canonical per-entity generated-source meaning.

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use crate::{
    DeclaredEntityVersion, EntityFragment, EntitySourceDigest, EntitySourceKey, FieldFragment,
    FieldSourceKey, FieldType, NamedTypeFragment, SchemaContractError, SchemaProposal,
    TypeSourceKey,
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
        let normalized = normalized_entity(entity)?;

        let mut pending_types = Vec::new();
        for field in entity.fields() {
            collect_field_type_sources(field.field_type(), &mut pending_types);
        }
        let relation_targets = relation_target_meanings(entity, &entities, &mut pending_types)?;
        let reachable_types = reachable_type_meanings(&types, pending_types)?;
        let encoded = candid::encode_args((&normalized, &relation_targets, &reachable_types))
            .map_err(|_| SchemaContractError::Encode)?;

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

fn normalized_entity(entity: &EntityFragment) -> Result<EntityFragment, SchemaContractError> {
    EntityFragment::try_new(
        entity.name().clone(),
        DeclaredEntityVersion::try_new(1)?,
        entity.fields().to_vec(),
        entity.primary_key().to_vec(),
        entity.indexes().to_vec(),
        entity.relations().to_vec(),
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
        DeclaredEntityVersion, EntityFragment, EntitySourceKey, EntityStoreAssignment,
        ExpectedAcceptedHead, FieldFragment, FieldInsertPolicy, FieldSourceKey, FieldType,
        SchemaFragment, SchemaName, SchemaProposal, SchemaSubmissionKey, TargetDatabaseIdentity,
        TargetStoreIdentity,
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
}
