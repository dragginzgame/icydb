//! Module: diagnostic artifact.
//! Responsibility: export and validate bounded accepted-schema identity for host diagnostics.
//! Does not own: runtime schema authority, mutation admission, or canister persistence.
//! Boundary: an artifact can label an exact fingerprint only; it cannot authorize runtime work.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};

const DIAGNOSTIC_ARTIFACT_FORMAT: &str = "icydb-diagnostic-schema";
const DIAGNOSTIC_ARTIFACT_VERSION: u8 = 1;
const MAX_DIAGNOSTIC_ARTIFACT_BYTES: usize = icydb_schema::MAX_SCHEMA_PROPOSAL_BYTES;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DiagnosticSchemaArtifact {
    format: String,
    version: u8,
    provenance: DiagnosticArtifactProvenance,
    entities: Vec<DiagnosticArtifactEntity>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DiagnosticArtifactProvenance {
    environment: String,
    canister: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DiagnosticArtifactEntity {
    fingerprint_method: u8,
    fingerprint: [u8; 16],
    entity_tag: u64,
    entity_name: String,
    entity_path: String,
    fields: Vec<DiagnosticArtifactIdentity>,
    constraints: Vec<DiagnosticArtifactConstraint>,
    indexes: Vec<DiagnosticArtifactIdentity>,
    relations: Vec<DiagnosticArtifactIdentity>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DiagnosticArtifactIdentity {
    id: u32,
    name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DiagnosticArtifactConstraint {
    id: u32,
    name: String,
    kind: String,
    field_ids: Vec<u32>,
    index_id: Option<u32>,
    relation_id: Option<u32>,
}

pub(crate) struct ResolvedDiagnosticEntity<'a> {
    entity: &'a DiagnosticArtifactEntity,
    constraint: Option<&'a DiagnosticArtifactConstraint>,
}

impl DiagnosticSchemaArtifact {
    pub(crate) fn from_report(
        environment: &str,
        canister: &str,
        report: &[icydb::db::EntitySchemaDescription],
    ) -> Result<Self, String> {
        let mut entities = report
            .iter()
            .map(DiagnosticArtifactEntity::from_description)
            .collect::<Result<Vec<_>, _>>()?;
        entities.sort_by_key(|entity| {
            (
                entity.fingerprint_method,
                entity.fingerprint,
                entity.entity_tag,
            )
        });

        let artifact = Self {
            format: DIAGNOSTIC_ARTIFACT_FORMAT.to_string(),
            version: DIAGNOSTIC_ARTIFACT_VERSION,
            provenance: DiagnosticArtifactProvenance {
                environment: environment.to_string(),
                canister: canister.to_string(),
            },
            entities,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub(crate) fn read(path: &Path) -> Result<Self, String> {
        let file = File::open(path).map_err(|err| {
            format!(
                "failed to open diagnostic artifact '{}': {err}",
                path.display()
            )
        })?;
        let metadata = file.metadata().map_err(|err| {
            format!(
                "failed to inspect diagnostic artifact '{}': {err}",
                path.display()
            )
        })?;
        let byte_len = usize::try_from(metadata.len())
            .map_err(|_| "diagnostic artifact length does not fit this host".to_string())?;
        if byte_len > MAX_DIAGNOSTIC_ARTIFACT_BYTES {
            return Err(format!(
                "diagnostic artifact is {byte_len} bytes; maximum is {MAX_DIAGNOSTIC_ARTIFACT_BYTES}"
            ));
        }

        let mut bytes = Vec::with_capacity(byte_len);
        file.take((MAX_DIAGNOSTIC_ARTIFACT_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|err| {
                format!(
                    "failed to read diagnostic artifact '{}': {err}",
                    path.display()
                )
            })?;
        if bytes.len() > MAX_DIAGNOSTIC_ARTIFACT_BYTES {
            return Err(format!(
                "diagnostic artifact exceeds {MAX_DIAGNOSTIC_ARTIFACT_BYTES} bytes"
            ));
        }
        let artifact = serde_json::from_slice::<Self>(bytes.as_slice()).map_err(|err| {
            format!(
                "failed to decode current diagnostic artifact '{}': {err}",
                path.display()
            )
        })?;
        artifact.validate()?;
        Ok(artifact)
    }

    pub(crate) fn write_new(&self, path: &Path) -> Result<(), String> {
        self.validate()?;
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|err| format!("failed to encode diagnostic artifact: {err}"))?;
        if bytes.len() > MAX_DIAGNOSTIC_ARTIFACT_BYTES {
            return Err(format!(
                "encoded diagnostic artifact is {} bytes; maximum is {MAX_DIAGNOSTIC_ARTIFACT_BYTES}",
                bytes.len()
            ));
        }
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|err| {
                format!(
                    "failed to create diagnostic artifact '{}': {err}",
                    path.display()
                )
            })?;
        output.write_all(bytes.as_slice()).map_err(|err| {
            format!(
                "failed to write diagnostic artifact '{}': {err}",
                path.display()
            )
        })
    }

    pub(crate) fn provenance_matches(&self, environment: &str, canister: &str) -> bool {
        self.provenance.environment == environment && self.provenance.canister == canister
    }

    pub(crate) fn resolve(
        &self,
        fingerprint_method: u8,
        fingerprint: [u8; 16],
        entity_tag: u64,
        constraint_id: Option<u32>,
    ) -> Option<ResolvedDiagnosticEntity<'_>> {
        let entity = self.entities.iter().find(|entity| {
            entity.fingerprint_method == fingerprint_method
                && entity.fingerprint == fingerprint
                && entity.entity_tag == entity_tag
        })?;
        let constraint = constraint_id.and_then(|constraint_id| entity.constraint(constraint_id));
        Some(ResolvedDiagnosticEntity { entity, constraint })
    }

    fn validate(&self) -> Result<(), String> {
        if self.format != DIAGNOSTIC_ARTIFACT_FORMAT || self.version != DIAGNOSTIC_ARTIFACT_VERSION
        {
            return Err(format!(
                "unsupported diagnostic artifact format/version '{}'/{}; expected '{}'/{}",
                self.format, self.version, DIAGNOSTIC_ARTIFACT_FORMAT, DIAGNOSTIC_ARTIFACT_VERSION
            ));
        }
        validate_text("environment", self.provenance.environment.as_str())?;
        validate_text("canister", self.provenance.canister.as_str())?;
        validate_count(
            "entities",
            self.entities.len(),
            icydb_schema::MAX_FRAGMENT_ENTITIES,
        )?;

        let mut entity_keys = BTreeSet::new();
        let mut entity_tags = BTreeSet::new();
        for entity in &self.entities {
            entity.validate()?;
            if !entity_keys.insert((
                entity.fingerprint_method,
                entity.fingerprint,
                entity.entity_tag,
            )) {
                return Err("diagnostic artifact contains duplicate entity identity".to_string());
            }
            if !entity_tags.insert(entity.entity_tag) {
                return Err("diagnostic artifact contains a duplicate entity tag".to_string());
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn test_fixture() -> Self {
        Self {
            format: DIAGNOSTIC_ARTIFACT_FORMAT.to_string(),
            version: DIAGNOSTIC_ARTIFACT_VERSION,
            provenance: DiagnosticArtifactProvenance {
                environment: "demo".to_string(),
                canister: "app".to_string(),
            },
            entities: vec![DiagnosticArtifactEntity {
                fingerprint_method: 1,
                fingerprint: [7; 16],
                entity_tag: 42,
                entity_name: "Account".to_string(),
                entity_path: "schema::Account".to_string(),
                fields: vec![],
                constraints: vec![DiagnosticArtifactConstraint {
                    id: 3,
                    name: "account_name_unique".to_string(),
                    kind: "unique".to_string(),
                    field_ids: vec![],
                    index_id: None,
                    relation_id: None,
                }],
                indexes: vec![],
                relations: vec![],
            }],
        }
    }
}

impl DiagnosticArtifactEntity {
    fn from_description(entity: &icydb::db::EntitySchemaDescription) -> Result<Self, String> {
        let mut fields = BTreeMap::<u32, String>::new();
        let mut indexes = BTreeMap::<u32, String>::new();
        let mut relations = BTreeMap::<u32, String>::new();
        let mut constraints = Vec::with_capacity(entity.constraints().len());

        for constraint in entity.constraints() {
            let mut field_ids = Vec::new();
            if let Some(field_id) = constraint.field_id() {
                let field_name = constraint.fields().first().ok_or_else(|| {
                    format!(
                        "constraint {} exposes field ID {field_id} without a field name",
                        constraint.id()
                    )
                })?;
                insert_exact_name(&mut fields, field_id, field_name, "field")?;
                field_ids.push(field_id);
            }
            if let (Some(index_id), Some(index_name)) = (constraint.index_id(), constraint.index())
            {
                insert_exact_name(&mut indexes, index_id, index_name, "index")?;
            }
            if let (Some(relation_id), Some(relation_name)) =
                (constraint.relation_id(), constraint.relation())
            {
                insert_exact_name(&mut relations, relation_id, relation_name, "relation")?;
            }
            constraints.push(DiagnosticArtifactConstraint {
                id: constraint.id(),
                name: constraint.name().to_string(),
                kind: constraint.kind().to_string(),
                field_ids,
                index_id: constraint.index_id(),
                relation_id: constraint.relation_id(),
            });
        }
        constraints.sort_by_key(|constraint| constraint.id);

        let entity = Self {
            fingerprint_method: entity.accepted_schema_fingerprint_method(),
            fingerprint: entity.accepted_schema_fingerprint(),
            entity_tag: entity.entity_tag(),
            entity_name: entity.entity_name().to_string(),
            entity_path: entity.entity_path().to_string(),
            fields: identities(fields),
            constraints,
            indexes: identities(indexes),
            relations: identities(relations),
        };
        entity.validate()?;
        Ok(entity)
    }

    fn constraint(&self, id: u32) -> Option<&DiagnosticArtifactConstraint> {
        self.constraints
            .binary_search_by_key(&id, |constraint| constraint.id)
            .ok()
            .map(|index| &self.constraints[index])
    }

    fn validate(&self) -> Result<(), String> {
        if self.fingerprint_method == 0 {
            return Err("diagnostic artifact fingerprint method must be non-zero".to_string());
        }
        validate_text("entity name", self.entity_name.as_str())?;
        validate_text("entity path", self.entity_path.as_str())?;
        validate_identities(
            "fields",
            self.fields.as_slice(),
            icydb_schema::MAX_FRAGMENT_FIELDS,
        )?;
        validate_identities(
            "indexes",
            self.indexes.as_slice(),
            icydb_schema::MAX_FRAGMENT_INDEXES,
        )?;
        validate_identities(
            "relations",
            self.relations.as_slice(),
            icydb_schema::MAX_FRAGMENT_RELATIONS,
        )?;
        validate_count(
            "constraints",
            self.constraints.len(),
            icydb_schema::MAX_FRAGMENT_CONSTRAINTS,
        )?;

        let mut constraint_ids = BTreeSet::new();
        for constraint in &self.constraints {
            if constraint.id == 0 || !constraint_ids.insert(constraint.id) {
                return Err("diagnostic artifact contains an invalid constraint ID".to_string());
            }
            validate_text("constraint name", constraint.name.as_str())?;
            validate_text("constraint kind", constraint.kind.as_str())?;
            validate_count(
                "constraint field IDs",
                constraint.field_ids.len(),
                icydb_schema::MAX_FRAGMENT_FIELDS,
            )?;
            if constraint
                .field_ids
                .iter()
                .any(|id| !self.fields.iter().any(|field| field.id == *id))
            {
                return Err(
                    "diagnostic artifact constraint references an unknown field ID".to_string(),
                );
            }
            if constraint
                .index_id
                .is_some_and(|id| !self.indexes.iter().any(|index| index.id == id))
            {
                return Err(
                    "diagnostic artifact constraint references an unknown index ID".to_string(),
                );
            }
            if constraint
                .relation_id
                .is_some_and(|id| !self.relations.iter().any(|relation| relation.id == id))
            {
                return Err(
                    "diagnostic artifact constraint references an unknown relation ID".to_string(),
                );
            }
        }
        Ok(())
    }
}

impl ResolvedDiagnosticEntity<'_> {
    pub(crate) const fn entity_name(&self) -> &str {
        self.entity.entity_name.as_str()
    }

    pub(crate) const fn entity_path(&self) -> &str {
        self.entity.entity_path.as_str()
    }

    pub(crate) fn constraint_name(&self) -> Option<&str> {
        self.constraint.map(|constraint| constraint.name.as_str())
    }

    pub(crate) fn constraint_kind(&self) -> Option<&str> {
        self.constraint.map(|constraint| constraint.kind.as_str())
    }

    pub(crate) fn field_name(&self, id: u32) -> Option<&str> {
        identity_name(self.entity.fields.as_slice(), id)
    }

    pub(crate) fn index_name(&self, id: u32) -> Option<&str> {
        identity_name(self.entity.indexes.as_slice(), id)
    }

    pub(crate) fn relation_name(&self, id: u32) -> Option<&str> {
        identity_name(self.entity.relations.as_slice(), id)
    }
}

fn identities(names: BTreeMap<u32, String>) -> Vec<DiagnosticArtifactIdentity> {
    names
        .into_iter()
        .map(|(id, name)| DiagnosticArtifactIdentity { id, name })
        .collect()
}

fn identity_name(identities: &[DiagnosticArtifactIdentity], id: u32) -> Option<&str> {
    identities
        .binary_search_by_key(&id, |identity| identity.id)
        .ok()
        .map(|index| identities[index].name.as_str())
}

fn insert_exact_name(
    identities: &mut BTreeMap<u32, String>,
    id: u32,
    name: &str,
    label: &str,
) -> Result<(), String> {
    if let Some(existing) = identities.get(&id) {
        if existing != name {
            return Err(format!(
                "diagnostic artifact {label} ID {id} has conflicting names"
            ));
        }
        return Ok(());
    }
    identities.insert(id, name.to_string());
    Ok(())
}

fn validate_identities(
    label: &str,
    identities: &[DiagnosticArtifactIdentity],
    maximum: usize,
) -> Result<(), String> {
    validate_count(label, identities.len(), maximum)?;
    let mut ids = BTreeSet::new();
    for identity in identities {
        if !ids.insert(identity.id) {
            return Err(format!("diagnostic artifact contains duplicate {label} ID"));
        }
        validate_text(label, identity.name.as_str())?;
    }
    Ok(())
}

fn validate_count(label: &str, actual: usize, maximum: usize) -> Result<(), String> {
    if actual > maximum {
        return Err(format!(
            "diagnostic artifact {label} count {actual} exceeds {maximum}"
        ));
    }
    Ok(())
}

fn validate_text(label: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > icydb_schema::MAX_SOURCE_KEY_BYTES {
        return Err(format!(
            "diagnostic artifact {label} must contain 1..={} bytes",
            icydb_schema::MAX_SOURCE_KEY_BYTES
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrong_version_and_unknown_fields_fail_closed() {
        let wrong_version = br#"{
            "format":"icydb-diagnostic-schema",
            "version":2,
            "provenance":{"environment":"demo","canister":"app"},
            "entities":[]
        }"#;
        let artifact: DiagnosticSchemaArtifact =
            serde_json::from_slice(wrong_version).expect("shape should decode");
        assert!(artifact.validate().is_err());

        let unknown_field = br#"{
            "format":"icydb-diagnostic-schema",
            "version":1,
            "provenance":{"environment":"demo","canister":"app"},
            "entities":[],
            "legacy":true
        }"#;
        assert!(serde_json::from_slice::<DiagnosticSchemaArtifact>(unknown_field).is_err());
    }

    #[test]
    fn exact_fingerprint_and_entity_tag_are_required_for_resolution() {
        let artifact = DiagnosticSchemaArtifact::test_fixture();

        assert!(artifact.resolve(1, [7; 16], 42, Some(3)).is_some());
        assert!(artifact.resolve(2, [7; 16], 42, Some(3)).is_none());
        assert!(artifact.resolve(1, [8; 16], 42, Some(3)).is_none());
        assert!(artifact.resolve(1, [7; 16], 43, Some(3)).is_none());
    }

    #[test]
    fn artifact_identity_is_projected_from_the_accepted_description() {
        let report = [icydb::db::EntitySchemaDescription::new(
            "schema::Account".to_string(),
            "Account".to_string(),
            42,
            1,
            [7; 16],
            "id".to_string(),
            vec!["id".to_string()],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            1,
            1,
        )];
        let artifact = DiagnosticSchemaArtifact::from_report("demo", "app", &report)
            .expect("accepted description should export");

        assert!(artifact.provenance_matches("demo", "app"));
        let resolved = artifact
            .resolve(1, [7; 16], 42, None)
            .expect("exact accepted identity should resolve");
        assert_eq!(resolved.entity_name(), "Account");
        assert_eq!(resolved.entity_path(), "schema::Account");
    }

    #[test]
    fn current_artifact_roundtrips_without_overwriting_existing_output() {
        let artifact = DiagnosticSchemaArtifact::test_fixture();
        let path = std::env::temp_dir().join(format!(
            "icydb-diagnostic-artifact-{}.json",
            std::process::id()
        ));
        if path.exists() {
            std::fs::remove_file(path.as_path()).expect("stale test artifact should be removable");
        }

        artifact
            .write_new(path.as_path())
            .expect("current artifact should write");
        assert_eq!(
            DiagnosticSchemaArtifact::read(path.as_path()).expect("current artifact should read"),
            artifact
        );
        assert!(artifact.write_new(path.as_path()).is_err());

        std::fs::remove_file(path).expect("test artifact should be removable");
    }
}
