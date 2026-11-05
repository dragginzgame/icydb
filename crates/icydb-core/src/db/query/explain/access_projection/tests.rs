use super::*;
use crate::{
    db::query::{explain::writer::render_logical, plan::write_explain_access_strategy_label},
    value::Value,
};

#[test]
fn nested_access_json_reports_one_ordered_metadata_contract() {
    let access = ExplainAccessPath::Union(vec![
        ExplainAccessPath::FullScan,
        ExplainAccessPath::Intersection(vec![
            ExplainAccessPath::IndexPrefix {
                name: "by_owner".into(),
                fields: vec!["owner".into(), "amount".into()],
                prefix_len: 1,
                values: vec![],
            },
            ExplainAccessPath::IndexBranchSet {
                name: "by_owner".into(),
                fields: vec!["owner".into(), "amount".into()],
                fixed_values: vec![],
                branch_values: vec![],
                branch_field: Some("owner".into()),
            },
        ]),
    ]);
    let rendered = render_logical(|out| write_access_json(&access, out)).unwrap();
    assert_eq!(
        rendered,
        concat!(
            r#"{"type":"Union","children":[{"type":"FullScan"},{"type":"Intersection","children":["#,
            r#"{"type":"IndexPrefix","name":"by_owner","fields":["owner","amount"],"prefix_len":1,"value_count":0,"bound_fields":["owner"],"unbound_fields":["amount"]},"#,
            r#"{"type":"IndexBranchSet","name":"by_owner","fields":["owner","amount"],"fixed_prefix_len":0,"branch_count":0,"ordered_suffix":"primary_key_asc","branch_field":"owner","bound_fields":["owner"]}]}]}"#,
        )
    );
}

#[test]
fn explain_access_strategy_label_projects_stable_render_labels() {
    for (access, expected) in [
        (
            ExplainAccessPath::ByKey {
                key: Value::Nat64(1),
            },
            "ByKey",
        ),
        (
            ExplainAccessPath::Union(vec![
                ExplainAccessPath::FullScan,
                ExplainAccessPath::ByKeys {
                    keys: vec![Value::Nat64(2)],
                },
            ]),
            "Union(2)",
        ),
    ] {
        let mut output = String::new();
        write_explain_access_strategy_label(&access, &mut output).unwrap();
        assert_eq!(output, expected);
    }
}
