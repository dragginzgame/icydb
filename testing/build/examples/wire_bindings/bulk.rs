//! Consumer decisions for Wire's host-only bulk-field advice.

use std::collections::BTreeMap;

use ic_wire::typescript::{BulkLintReport, GenerationError, lint_bulk_roots};
use serde_json::Value;

/// Review every endpoint request/reply use site through the canonical Wire lint.
pub(crate) fn review(
    types: &BTreeMap<String, Value>,
    methods: &[Value],
) -> Result<BulkLintReport, GenerationError> {
    let mut roots = BTreeMap::new();
    for method in methods {
        let name = method["name"].as_str().ok_or(GenerationError::Schema)?;
        for direction in ["request", "response"] {
            // Endpoint identities must be unique before reviewing their payloads.
            let root = format!("@method:{name}:{direction}");
            if roots.insert(root, method[direction].clone()).is_some() {
                return Err(GenerationError::Schema);
            }
        }
    }

    let mut acknowledgements = BTreeMap::new();
    for method in ["icydb_query", "icydb_ddl", "icydb_update"] {
        acknowledgements.insert(
            format!("/@method:{method}:response/ok/variants/Ddl/fields/constraint_validation/item/fields/findings/item/fields/field_ids/list/u32"),
            "Diagnostic field IDs retain compact varints; no measured bulk benefit justifies changing payload bytes and the client array type.".to_owned(),
        );
    }

    lint_bulk_roots(&roots, types, &acknowledgements)
}

#[cfg(test)]
mod tests {
    use super::review;
    use ic_wire::typescript::GenerationError;
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn inline_requests_and_error_replies_cannot_bypass_bulk_review() {
        let methods = [json!({
            "name": "example",
            "request": {"k":"list","item":{"k":"u","bits":8}},
            "response": {"k":"result","ok":{"k":"unit"},
                "err":{"k":"list","item":{"k":"u","bits":32}}}
        })];
        let report = review(&BTreeMap::new(), &methods).expect("valid method roots");
        let paths: Vec<_> = report
            .warnings
            .iter()
            .map(|warning| warning.path.as_str())
            .collect();
        assert_eq!(
            paths,
            [
                "/@method:example:request/list/u8",
                "/@method:example:response/err/list/u32"
            ]
        );
        assert!(!report.is_clean());
    }

    #[test]
    fn removed_contract_fields_leave_stale_acknowledgements() {
        let report = review(&BTreeMap::new(), &[]).expect("empty registry");
        assert!(report.warnings.is_empty());
        assert!(
            report
                .stale_acknowledgements
                .iter()
                .any(|path| { path == "/@method:icydb_query:response/ok/variants/Ddl/fields/constraint_validation/item/fields/findings/item/fields/field_ids/list/u32" })
        );
        assert!(!report.is_clean());
    }

    #[test]
    fn field_id_acknowledgement_does_not_hide_other_fields() {
        let mut types = crate::metadata::registry().unwrap().finish().unwrap();
        let methods: Vec<_> = ["icydb_query", "icydb_ddl", "icydb_update"]
            .into_iter()
            .map(|name| {
                json!({"name":name,"request":{"k":"text"},
                "response":{"k":"result","ok":{"k":"ref","name":"SqlQueryResult"},
                    "err":{"k":"unit"}}})
            })
            .collect();
        assert!(review(&types, &methods).unwrap().is_clean());
        let fields = types.get_mut("ConstraintValidationFindingOutput").unwrap()["fields"]
            .as_array_mut()
            .unwrap();
        let mut other = fields
            .iter()
            .find(|f| f["name"] == "field_ids")
            .unwrap()
            .clone();
        other["name"] = json!("other_ids");
        fields.push(other);
        let report = review(&types, &methods).unwrap();
        assert_eq!(report.warnings.len(), 3);
        assert!(
            report
                .warnings
                .iter()
                .all(|w| w.path.ends_with("/fields/other_ids/list/u32"))
        );
        assert!(report.stale_acknowledgements.is_empty());

        // The shared container surviving elsewhere cannot keep this decision alive.
        types.get_mut("ConstraintValidationFindingOutput").unwrap()["fields"]
            .as_array_mut()
            .unwrap()
            .retain(|f| f["name"] != "field_ids");
        let report = review(&types, &methods).unwrap();
        assert_eq!(report.warnings.len(), 3);
        assert_eq!(report.stale_acknowledgements.len(), 3);

        types.get_mut("ConstraintValidationFindingOutput").unwrap()["fields"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name":"field_ids","value":{"k":"bytes"}}));
        let report = review(&types, &methods).unwrap();
        assert_eq!(report.warnings.len(), 3);
        assert_eq!(report.stale_acknowledgements.len(), 3);
        assert!(!report.is_clean());
    }

    #[test]
    fn registry_bulk_review_preserves_field_scope_and_registration_order() {
        use ic_wire::typescript::{SchemaRegistry, lint_bulk_roots};

        #[derive(serde::Deserialize, serde::Serialize, ic_wire::Wire)]
        struct Reply {
            field_ids: Vec<u32>,
            other_ids: Vec<u32>,
        }

        // Synthetic registry names must not become consumer policy identities.
        for register_unrelated_first in [false, true] {
            let mut registry = SchemaRegistry::new();
            if register_unrelated_first {
                registry.register::<String>("Unrelated").unwrap();
            }
            let root = registry.register::<Reply>("Reply").unwrap();
            let types = registry.finish().unwrap();
            let roots = BTreeMap::from([("Reply".to_owned(), root)]);
            let acknowledgements = BTreeMap::from([(
                "/Reply/fields/field_ids/list/u32".to_owned(),
                "Retain diagnostic field IDs as compact varints".to_owned(),
            )]);
            let report = lint_bulk_roots(&roots, &types, &acknowledgements).unwrap();
            assert_eq!(report.warnings.len(), 1);
            assert_eq!(report.warnings[0].path, "/Reply/fields/other_ids/list/u32");
            assert!(report.stale_acknowledgements.is_empty());
            assert!(!report.is_clean());
        }
    }

    #[test]
    fn duplicate_method_roots_are_rejected() {
        let method = json!({"name":"example","request":{"k":"unit"},"response":{"k":"unit"}});
        assert!(matches!(
            review(&BTreeMap::new(), &[method.clone(), method]),
            Err(GenerationError::Schema)
        ));
    }
}
