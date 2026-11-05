use super::{DynamicMutationResult, DynamicTypedEntityBinding};

fn binding() -> DynamicTypedEntityBinding {
    DynamicTypedEntityBinding::new(
        [0; 16],
        "EntitySource".to_string(),
        "Entity".to_string(),
        1,
        1,
        [1; 16],
        1,
        Vec::new(),
        vec![("ChoiceSource".to_string(), "RenamedChoice".to_string())],
        vec![
            (
                "ChoiceSource".to_string(),
                "FirstSource".to_string(),
                "RenamedFirst".to_string(),
            ),
            (
                "ChoiceSource".to_string(),
                "SecondSource".to_string(),
                "Second".to_string(),
            ),
        ],
        Vec::new(),
    )
    .expect("test binding should be internally consistent")
}

#[test]
fn accepted_enum_variant_name_resolves_to_immutable_source_key() {
    let binding = binding();

    assert_eq!(
        binding.enum_variant_source_key("ChoiceSource", "RenamedFirst"),
        Some("FirstSource"),
    );
    assert_eq!(
        binding.enum_variant_source_key("ChoiceSource", "Second"),
        Some("SecondSource"),
    );
    assert_eq!(
        binding.enum_variant_source_key("OtherSource", "RenamedFirst"),
        None,
    );
    assert_eq!(
        binding.enum_variant_source_key("ChoiceSource", "FirstSource"),
        None,
    );
}

#[test]
fn dynamic_mutation_result_derives_cardinality_from_rows() {
    let result = DynamicMutationResult {
        entity: "Entity".to_string(),
        columns: Vec::new(),
        rows: vec![Vec::new()],
        affected_rows: 1,
    };

    assert_eq!(result.len(), 1);
    assert!(!result.is_empty());
}
