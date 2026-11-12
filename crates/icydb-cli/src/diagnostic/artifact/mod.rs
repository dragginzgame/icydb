//! Module: diagnostic artifact.
//! Responsibility: export and validate bounded accepted-schema identity for host diagnostics.
//! Does not own: runtime schema authority, mutation admission, or canister persistence.
//! Boundary: an artifact can label an exact fingerprint only; it cannot authorize runtime work.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    path::Path,
};

use ic_host_fs::read::{open_regular_file, read_opened_file};
use ic_host_text::escape::terminal_text;
use serde::{Deserialize, Serialize};

const DIAGNOSTIC_ARTIFACT_FORMAT: &str = "icydb-diagnostic-schema";
const DIAGNOSTIC_ARTIFACT_VERSION: u8 = 1;
const MAX_DIAGNOSTIC_ARTIFACT_BYTES: usize = icydb_schema::MAX_SCHEMA_PROPOSAL_BYTES;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DiagnosticSchemaArtifact {
    format: String,
    version: u8,
    provenance: DiagnosticArtifactOwner,
    entities: Vec<DiagnosticArtifactEntity>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
enum DiagnosticArtifactOwner {
    Deployment {
        environment: String,
        canister: String,
    },
    Source {
        source: String,
    },
}

/// Identity lists are bounded and unique; their JSON order carries no authority.
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
            provenance: DiagnosticArtifactOwner::Deployment {
                environment: environment.to_string(),
                canister: canister.to_string(),
            },
            entities,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub(crate) fn read_deployment(path: &Path) -> Result<Self, String> {
        let artifact = Self::read(path)?;
        if !matches!(
            &artifact.provenance,
            DiagnosticArtifactOwner::Deployment { .. }
        ) {
            return Err(format!(
                "diagnostic artifact '{}' is source metadata, not a deployment artifact",
                terminal_text(&path.display().to_string())
            ));
        }
        Ok(artifact)
    }

    pub(crate) fn read_source_metadata(path: &Path) -> Result<Self, String> {
        let artifact = Self::read(path)?;
        if !matches!(&artifact.provenance, DiagnosticArtifactOwner::Source { .. }) {
            return Err(format!(
                "diagnostic source metadata '{}' is a deployment artifact",
                terminal_text(&path.display().to_string())
            ));
        }
        Ok(artifact)
    }

    pub(crate) fn bind_to_source(mut self, source: &str) -> Result<Self, String> {
        validate_text("source", source)?;
        self.provenance = DiagnosticArtifactOwner::Source {
            source: source.to_string(),
        };
        self.validate()?;
        Ok(self)
    }

    fn read(path: &Path) -> Result<Self, String> {
        // Admit the selected descriptor before reading; a FIFO must not wait
        // for a writer during open. Regular-file symlinks remain supported.
        let file = open_regular_file(path).map_err(|err| {
            format!(
                "failed to open diagnostic artifact '{}': {err}",
                terminal_text(&path.display().to_string()),
                err = terminal_text(&err.to_string()),
            )
        })?;
        // Keep the selected descriptor and existing symlink policy; the shared
        // owner enforces regular-file admission and the independent stream bound.
        let bytes = read_opened_file(file, MAX_DIAGNOSTIC_ARTIFACT_BYTES).map_err(|err| {
            format!(
                "failed to read diagnostic artifact '{}': {err}",
                terminal_text(&path.display().to_string()),
                err = terminal_text(&err.to_string()),
            )
        })?;
        let artifact = serde_json::from_slice::<Self>(bytes.as_slice()).map_err(|err| {
            format!(
                "failed to decode current diagnostic artifact '{}': {err}",
                terminal_text(&path.display().to_string()),
                err = terminal_text(&err.to_string()),
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
        #[cfg(unix)]
        {
            use ic_host_fs::durable::{
                PublicationMode, WriteOptions, write_existing_parent_validated_with,
            };

            // Host admits the target and retains its existing parent descriptor;
            // missing parents are refused and create-only publication never follows
            // or replaces an occupied final entry.
            write_existing_parent_validated_with(
                path,
                WriteOptions {
                    mode: PublicationMode::CreateNew,
                    permissions: 0o666,
                },
                |file| file.write_all(&bytes),
                |()| Ok(()),
            )
            .map_err(|err| {
                // Host's display preserves whether the complete output is visible.
                // This terminal CLI error never authorizes overwrite or retry.
                format!(
                    "failed to publish diagnostic artifact '{}': {err}",
                    terminal_text(&path.display().to_string()),
                    err = terminal_text(&err.to_string()),
                )
            })
        }
        #[cfg(not(unix))]
        {
            let _ = bytes;
            Err("durable diagnostic artifact publication is unsupported on this host".to_string())
        }
    }

    pub(crate) fn provenance_matches(&self, environment: &str, canister: &str) -> bool {
        matches!(
            &self.provenance,
            DiagnosticArtifactOwner::Deployment {
                environment: artifact_environment,
                canister: artifact_canister,
            } if artifact_environment == environment && artifact_canister == canister
        )
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
        let constraint = match constraint_id {
            Some(constraint_id) => Some(entity.constraint(constraint_id)?),
            None => None,
        };
        Some(ResolvedDiagnosticEntity { entity, constraint })
    }

    fn validate(&self) -> Result<(), String> {
        if self.format != DIAGNOSTIC_ARTIFACT_FORMAT || self.version != DIAGNOSTIC_ARTIFACT_VERSION
        {
            return Err(format!(
                "unsupported diagnostic artifact format/version '{}'/{}; expected '{}'/{}",
                terminal_text(&self.format),
                self.version,
                DIAGNOSTIC_ARTIFACT_FORMAT,
                DIAGNOSTIC_ARTIFACT_VERSION
            ));
        }
        match &self.provenance {
            DiagnosticArtifactOwner::Deployment {
                environment,
                canister,
            } => {
                validate_text("environment", environment.as_str())?;
                validate_text("canister", canister.as_str())?;
            }
            DiagnosticArtifactOwner::Source { source } => {
                validate_text("source", source.as_str())?;
            }
        }
        validate_count(
            "entities",
            self.entities.len(),
            icydb_schema::MAX_FRAGMENT_ENTITIES,
        )?;

        // Unique tags also make each (method, fingerprint, tag) identity unique.
        let mut entity_tags = BTreeSet::new();
        for entity in &self.entities {
            entity.validate()?;
            if !entity_tags.insert(entity.entity_tag) {
                return Err("diagnostic artifact contains a duplicate entity tag".to_string());
            }
        }
        Ok(())
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
            .iter()
            .find(|constraint| constraint.id == id)
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
        .iter()
        .find(|identity| identity.id == id)
        .map(|identity| identity.name.as_str())
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
mod tests;

#[cfg(test)]
mod helper_tests;
