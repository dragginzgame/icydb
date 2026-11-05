//! Test-only constructors and adapters for the parent module.

use super::*;

impl DiagnosticSchemaArtifact {
    pub(in crate::diagnostic) fn test_fixture() -> Self {
        Self {
            format: DIAGNOSTIC_ARTIFACT_FORMAT.to_string(),
            version: DIAGNOSTIC_ARTIFACT_VERSION,
            provenance: DiagnosticArtifactOwner::Deployment {
                environment: "demo".to_string(),
                canister: "app".to_string(),
            },
            entities: vec![DiagnosticArtifactEntity {
                fingerprint_method: 1,
                fingerprint: [7; 16],
                entity_tag: 42,
                entity_name: "Account".to_string(),
                entity_path: "schema::Account".to_string(),
                fields: vec![DiagnosticArtifactIdentity {
                    id: 1,
                    name: "id".to_string(),
                }],
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

    pub(in crate::diagnostic) fn test_source_fixture() -> Self {
        let mut artifact = Self::test_fixture();
        artifact.provenance = DiagnosticArtifactOwner::Source {
            source: "schema/account.rs".to_string(),
        };
        artifact.entities[0].entity_name = "SourceAccount".to_string();
        artifact.entities[0].entity_path = "schema::source::Account".to_string();
        artifact.entities[0].constraints[0].name = "source_account_name_unique".to_string();
        artifact
    }
}
