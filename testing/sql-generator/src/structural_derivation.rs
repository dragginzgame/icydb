//! Module: sql_generator::structural_derivation
//! Responsibility: canonical structural identity derived from validated typed SQL authority.
//! Does not own: fixture policy, planner expectations, execution observations, or verdicts.
//! Boundary: removes authored names, local IDs, and literal payloads while preserving topology.

use crate::{
    MutationSnapshot, MutationStatement, SelectQuery, SelectSnapshot, SqlGeneratorError,
    SqlGeneratorErrorKind, StructuralSignature, replay::canonical_json_bytes,
};

use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

const FIELD_TAG_PREFIX: &str = "field:";
const SIGNED_TAG_PREFIX: &str = "i64:";
const UNSIGNED_TAG_PREFIX: &str = "u64:";

#[derive(Serialize)]
struct SelectStructure<'a> {
    schema: SelectSchemaStructure,
    query: Value,
    result_kinds: Vec<crate::SelectValueKind>,
    result_order: crate::SelectResultOrder,
    max_expression_depth: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    violation: Option<&'a str>,
}

#[derive(Serialize)]
struct SelectSchemaStructure {
    fields: Vec<SelectFieldStructure>,
    indexes: Vec<SelectIndexStructure>,
}

#[derive(Serialize)]
struct SelectFieldStructure {
    slot: String,
    kind: crate::SelectFieldKind,
    nullable: bool,
    primary_key: bool,
    generated: bool,
}

#[derive(Serialize)]
struct SelectIndexStructure {
    slot: String,
    field_slots: Vec<String>,
}

#[derive(Serialize)]
struct MutationStructure<'a> {
    schema: MutationSchemaStructure,
    ingress: crate::MutationIngress,
    intent_class: crate::MutationIntentClass,
    statements: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    violation: Option<&'a str>,
}

#[derive(Serialize)]
struct MutationSchemaStructure {
    fields: Vec<MutationFieldStructure>,
}

#[derive(Serialize)]
struct MutationFieldStructure {
    slot: String,
    kind: crate::MutationFieldKind,
    role: crate::MutationFieldRole,
    nullable: bool,
    default_kind: Option<&'static str>,
    primary_key: bool,
    indexed: bool,
}

pub(crate) fn derive_select_structure(
    snapshot: &SelectSnapshot,
    query: &SelectQuery,
    violation: Option<&str>,
) -> Result<String, SqlGeneratorError> {
    let field_slots = snapshot
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| (field.id(), slot("field", ordinal)))
        .collect::<BTreeMap<_, _>>();
    let index_slots = snapshot
        .indexes()
        .iter()
        .enumerate()
        .map(|(ordinal, index)| (index.id(), slot("index", ordinal)))
        .collect::<BTreeMap<_, _>>();
    let schema = SelectSchemaStructure {
        fields: snapshot
            .fields()
            .iter()
            .map(|field| {
                Ok(SelectFieldStructure {
                    slot: required_field_slot(&field_slots, field.id())?.to_string(),
                    kind: field.kind(),
                    nullable: field.nullable(),
                    primary_key: field.primary_key(),
                    generated: field.generated(),
                })
            })
            .collect::<Result<_, SqlGeneratorError>>()?,
        indexes: snapshot
            .indexes()
            .iter()
            .map(|index| {
                Ok(SelectIndexStructure {
                    slot: index_slots
                        .get(&index.id())
                        .ok_or_else(|| invalid_structure("missing canonical index slot"))?
                        .clone(),
                    field_slots: index
                        .field_ids()
                        .iter()
                        .map(|field_id| {
                            Ok(required_field_slot(&field_slots, *field_id)?.to_string())
                        })
                        .collect::<Result<_, SqlGeneratorError>>()?,
                })
            })
            .collect::<Result<_, SqlGeneratorError>>()?,
    };
    let result_kinds = query.projection_kinds(snapshot)?;
    let result_order = query.result_order();
    let max_expression_depth = query.max_expression_depth();
    let query_value = serde_json::to_value(query).map_err(serialization_error)?;
    let aliases = projection_alias_slots(&query_value)?;
    let literals = LiteralSlots::from_value(&query_value)?;
    let query = normalize_select_value(query_value, &field_slots, &aliases, &literals, None)?;
    encode_structure(&SelectStructure {
        schema,
        query,
        result_kinds,
        result_order,
        max_expression_depth,
        violation,
    })
}

pub(crate) fn derive_select_signature(
    snapshot: &SelectSnapshot,
    query: &SelectQuery,
    violation: Option<crate::SelectViolation>,
) -> Result<StructuralSignature, SqlGeneratorError> {
    let violation_code = violation.map(crate::SelectViolation::code);
    let signature = StructuralSignature::derived(
        if violation.is_some() {
            "singly_invalid"
        } else {
            "accepted"
        },
        snapshot.fixture_family(),
        "select",
        violation_code.unwrap_or("none"),
        derive_select_structure(snapshot, query, violation_code)?,
    );
    signature.validate()?;
    Ok(signature)
}

pub(crate) fn derive_mutation_structure(
    snapshot: &MutationSnapshot,
    ingress: crate::MutationIngress,
    intent_class: crate::MutationIntentClass,
    statements: &[MutationStatement],
    violation: Option<&str>,
) -> Result<String, SqlGeneratorError> {
    let schema = MutationSchemaStructure {
        fields: snapshot
            .fields()
            .iter()
            .enumerate()
            .map(|(ordinal, field)| MutationFieldStructure {
                slot: slot("field", ordinal),
                kind: field.kind(),
                role: field.role(),
                nullable: field.nullable(),
                default_kind: field.default().map(|default| match default {
                    crate::MutationDefaultValue::NullText => "null",
                    crate::MutationDefaultValue::Text(_) => "text",
                    crate::MutationDefaultValue::UnsignedInteger(_) => "unsigned_integer",
                }),
                primary_key: field.primary_key(),
                indexed: field.indexed(),
            })
            .collect(),
    };
    let statements = serde_json::to_value(statements).map_err(serialization_error)?;
    let literals = LiteralSlots::from_value(&statements)?;
    let statements = normalize_mutation_value(statements, &literals, None)?;
    encode_structure(&MutationStructure {
        schema,
        ingress,
        intent_class,
        statements,
        violation,
    })
}

pub(crate) fn derive_mutation_signature(
    snapshot: &MutationSnapshot,
    ingress: crate::MutationIngress,
    intent_class: crate::MutationIntentClass,
    statements: &[MutationStatement],
    violation: Option<&str>,
) -> Result<StructuralSignature, SqlGeneratorError> {
    let signature = StructuralSignature::derived(
        if violation.is_some() {
            "singly_invalid"
        } else {
            "accepted"
        },
        snapshot.fixture_family(),
        "mutation_sequence",
        violation.unwrap_or("none"),
        derive_mutation_structure(snapshot, ingress, intent_class, statements, violation)?,
    );
    signature.validate()?;
    Ok(signature)
}

fn encode_structure(value: &impl Serialize) -> Result<String, SqlGeneratorError> {
    String::from_utf8(canonical_json_bytes(value)?).map_err(|_| {
        SqlGeneratorError::new(
            SqlGeneratorErrorKind::Serialization,
            "canonical structural signature was not UTF-8",
        )
    })
}

fn projection_alias_slots(value: &Value) -> Result<BTreeMap<String, String>, SqlGeneratorError> {
    let projections = value
        .as_object()
        .and_then(|object| object.get("projections"))
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_structure("serialized SELECT lacks projections"))?;
    let mut aliases = BTreeMap::new();
    for (ordinal, alias) in projections
        .iter()
        .filter_map(|projection| projection.get("alias").and_then(Value::as_str))
        .enumerate()
    {
        aliases
            .entry(alias.to_string())
            .or_insert_with(|| slot("alias", ordinal));
    }
    Ok(aliases)
}

fn normalize_select_value(
    value: Value,
    fields: &BTreeMap<u32, String>,
    aliases: &BTreeMap<String, String>,
    literals: &LiteralSlots,
    parent_key: Option<&str>,
) -> Result<Value, SqlGeneratorError> {
    match value {
        Value::Object(object) => {
            let typed_null = object.get("kind").and_then(Value::as_str) == Some("null");
            object
                .into_iter()
                .map(|(key, value)| {
                    let normalized = if typed_null && key == "value" {
                        value
                    } else {
                        normalize_select_value(
                            value,
                            fields,
                            aliases,
                            literals,
                            Some(key.as_str()),
                        )?
                    };
                    Ok((key, normalized))
                })
                .collect::<Result<Map<_, _>, SqlGeneratorError>>()
                .map(Value::Object)
        }
        Value::Array(values) => values
            .into_iter()
            .map(|value| normalize_select_value(value, fields, aliases, literals, parent_key))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::String(string) if string.starts_with(FIELD_TAG_PREFIX) => {
            let field_id = parse_hex_tag(&string, FIELD_TAG_PREFIX)?;
            Ok(Value::String(
                required_field_slot(fields, field_id)?.to_string(),
            ))
        }
        Value::String(string) if parent_key == Some("alias") => aliases
            .get(&string)
            .cloned()
            .map(Value::String)
            .ok_or_else(|| invalid_structure("SELECT alias reference has no definition")),
        Value::String(string) if parent_key == Some("prefix") => {
            Ok(Value::String(literals.text_slot(&string, true)?))
        }
        Value::String(string)
            if string.starts_with(SIGNED_TAG_PREFIX) || string.starts_with(UNSIGNED_TAG_PREFIX) =>
        {
            Ok(Value::String(literals.number_slot(&string)?))
        }
        Value::String(string) if parent_key == Some("value") => {
            Ok(Value::String(literals.text_slot(&string, false)?))
        }
        Value::Number(_) if matches!(parent_key, Some("limit" | "offset")) => {
            Ok(Value::String("present".to_string()))
        }
        other => Ok(other),
    }
}

fn normalize_mutation_value(
    value: Value,
    literals: &LiteralSlots,
    parent_key: Option<&str>,
) -> Result<Value, SqlGeneratorError> {
    match value {
        Value::Object(object) => object
            .into_iter()
            .map(|(key, value)| {
                let normalized = normalize_mutation_value(value, literals, Some(key.as_str()))?;
                Ok((key, normalized))
            })
            .collect::<Result<Map<_, _>, SqlGeneratorError>>()
            .map(Value::Object),
        Value::Array(values) => values
            .into_iter()
            .map(|value| normalize_mutation_value(value, literals, parent_key))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::String(string)
            if string.starts_with(SIGNED_TAG_PREFIX) || string.starts_with(UNSIGNED_TAG_PREFIX) =>
        {
            Ok(Value::String(literals.number_slot(&string)?))
        }
        Value::String(string)
            if matches!(
                parent_key,
                Some("value" | "text" | "name" | "tier" | "note")
            ) =>
        {
            Ok(Value::String(literals.text_slot(&string, false)?))
        }
        Value::Number(_) if matches!(parent_key, Some("limit" | "offset")) => {
            Ok(Value::String("present".to_string()))
        }
        other => Ok(other),
    }
}

struct LiteralSlots {
    numbers: BTreeMap<String, String>,
    texts: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum NumericLiteralKey {
    Signed(i64),
    Unsigned(u64),
}

impl LiteralSlots {
    fn from_value(value: &Value) -> Result<Self, SqlGeneratorError> {
        let mut numbers = BTreeSet::new();
        let mut texts = BTreeSet::new();
        collect_literals(value, None, &mut numbers, &mut texts);
        let mut numbers = numbers
            .into_iter()
            .map(|value| Ok((numeric_literal_key(&value)?, value)))
            .collect::<Result<Vec<_>, SqlGeneratorError>>()?;
        numbers.sort_by_key(|(key, _)| *key);
        Ok(Self {
            numbers: numbers
                .into_iter()
                .enumerate()
                .map(|(ordinal, (_, value))| (value, slot("number", ordinal)))
                .collect(),
            texts: texts
                .into_iter()
                .enumerate()
                .map(|(ordinal, value)| (value, slot("text", ordinal)))
                .collect(),
        })
    }

    fn number_slot(&self, value: &str) -> Result<String, SqlGeneratorError> {
        self.numbers
            .get(value)
            .cloned()
            .ok_or_else(|| invalid_structure("numeric literal has no canonical slot"))
    }

    fn text_slot(&self, value: &str, prefix: bool) -> Result<String, SqlGeneratorError> {
        let slot = self
            .texts
            .get(value)
            .cloned()
            .ok_or_else(|| invalid_structure("text literal has no canonical slot"))?;
        let class = match (prefix, value.is_empty()) {
            (true, true) => "prefix_empty",
            (true, false) => "prefix_nonempty",
            (false, true) => "empty",
            (false, false) => "nonempty",
        };
        Ok(format!("{slot}:{class}"))
    }
}

fn numeric_literal_key(value: &str) -> Result<NumericLiteralKey, SqlGeneratorError> {
    if let Some(value) = value.strip_prefix(SIGNED_TAG_PREFIX) {
        return value
            .parse::<i64>()
            .map(NumericLiteralKey::Signed)
            .map_err(|_| invalid_structure("signed numeric literal has a malformed payload"));
    }
    if let Some(value) = value.strip_prefix(UNSIGNED_TAG_PREFIX) {
        return u64::from_str_radix(value, 16)
            .map(NumericLiteralKey::Unsigned)
            .map_err(|_| invalid_structure("unsigned numeric literal has a malformed payload"));
    }
    Err(invalid_structure(
        "numeric literal has an unsupported tagged representation",
    ))
}

fn collect_literals(
    value: &Value,
    parent_key: Option<&str>,
    numbers: &mut BTreeSet<String>,
    texts: &mut BTreeSet<String>,
) {
    match value {
        Value::Object(object) => {
            let typed_null = object.get("kind").and_then(Value::as_str) == Some("null");
            for (key, value) in object {
                if typed_null && key == "value" {
                    continue;
                }
                collect_literals(value, Some(key.as_str()), numbers, texts);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_literals(value, parent_key, numbers, texts);
            }
        }
        Value::String(string)
            if string.starts_with(SIGNED_TAG_PREFIX) || string.starts_with(UNSIGNED_TAG_PREFIX) =>
        {
            numbers.insert(string.clone());
        }
        Value::String(string)
            if matches!(
                parent_key,
                Some("value" | "text" | "name" | "tier" | "note" | "prefix")
            ) =>
        {
            texts.insert(string.clone());
        }
        _ => {}
    }
}

fn required_field_slot(
    fields: &BTreeMap<u32, String>,
    field_id: u32,
) -> Result<&str, SqlGeneratorError> {
    fields
        .get(&field_id)
        .map(String::as_str)
        .ok_or_else(|| invalid_structure("typed SQL tree references an absent field slot"))
}

fn parse_hex_tag(tagged: &str, prefix: &str) -> Result<u32, SqlGeneratorError> {
    u32::from_str_radix(
        tagged
            .strip_prefix(prefix)
            .ok_or_else(|| invalid_structure("structural identity tag has the wrong prefix"))?,
        16,
    )
    .map_err(|_| {
        SqlGeneratorError::new(
            SqlGeneratorErrorKind::Serialization,
            "structural identity tag has malformed hexadecimal payload",
        )
    })
}

fn slot(kind: &str, ordinal: usize) -> String {
    format!("{kind}:{ordinal}")
}

fn serialization_error(source: serde_json::Error) -> SqlGeneratorError {
    SqlGeneratorError::with_json_source(
        SqlGeneratorErrorKind::Serialization,
        "failed to serialize typed SQL authority for structural derivation",
        source,
    )
}

fn invalid_structure(message: &'static str) -> SqlGeneratorError {
    SqlGeneratorError::new(SqlGeneratorErrorKind::InvalidCase, message)
}

#[cfg(test)]
mod tests {
    use super::{LiteralSlots, normalize_select_value, projection_alias_slots};
    use serde_json::{Value, json};
    use std::collections::BTreeMap;

    #[test]
    fn canonical_field_slots_preserve_reference_topology_across_local_id_changes() {
        let repeated_a = json!({
            "projections": [
                {"alias": null, "expression": {"field_id": "field:0000000a"}},
                {"alias": null, "expression": {"field_id": "field:0000000a"}}
            ]
        });
        let repeated_b = json!({
            "projections": [
                {"alias": null, "expression": {"field_id": "field:00000384"}},
                {"alias": null, "expression": {"field_id": "field:00000384"}}
            ]
        });
        let distinct = json!({
            "projections": [
                {"alias": null, "expression": {"field_id": "field:0000000a"}},
                {"alias": null, "expression": {"field_id": "field:00000014"}}
            ]
        });
        let fields_a = BTreeMap::from([(10, "field:0".to_string()), (20, "field:1".to_string())]);
        let fields_b = BTreeMap::from([(900, "field:0".to_string()), (901, "field:1".to_string())]);

        assert_eq!(
            normalize(repeated_a, &fields_a),
            normalize(repeated_b, &fields_b),
        );
        assert_ne!(
            normalize(
                json!({
                    "projections": [
                        {"alias": null, "expression": {"field_id": "field:0000000a"}},
                        {"alias": null, "expression": {"field_id": "field:0000000a"}}
                    ]
                }),
                &fields_a,
            ),
            normalize(distinct, &fields_a),
        );
    }

    #[test]
    fn alias_names_are_erased_while_alias_binding_topology_remains() {
        let fields = BTreeMap::new();
        let first = json!({
            "projections": [
                {"alias": "first_name", "expression": {"node": "literal", "value": "alpha"}},
                {"alias": "second_name", "expression": {"node": "literal", "value": "beta"}}
            ],
            "order": [{"target": {"alias": "first_name"}}]
        });
        let renamed = json!({
            "projections": [
                {"alias": "x", "expression": {"node": "literal", "value": "alpha"}},
                {"alias": "y", "expression": {"node": "literal", "value": "beta"}}
            ],
            "order": [{"target": {"alias": "x"}}]
        });
        let rebound = json!({
            "projections": [
                {"alias": "x", "expression": {"node": "literal", "value": "alpha"}},
                {"alias": "y", "expression": {"node": "literal", "value": "beta"}}
            ],
            "order": [{"target": {"alias": "y"}}]
        });

        assert_eq!(normalize(first, &fields), normalize(renamed, &fields),);
        assert_ne!(
            normalize(
                json!({
                    "projections": [
                        {"alias": "x", "expression": {"node": "literal", "value": "alpha"}},
                        {"alias": "y", "expression": {"node": "literal", "value": "beta"}}
                    ],
                    "order": [{"target": {"alias": "x"}}]
                }),
                &fields,
            ),
            normalize(rebound, &fields),
        );
    }

    #[test]
    fn numeric_literal_slots_follow_numeric_not_lexical_order() {
        let fields = BTreeMap::new();
        let low_high = json!({
            "projections": [],
            "predicate": ["i64:2", "i64:10"]
        });
        let shifted = json!({
            "projections": [],
            "predicate": ["i64:20", "i64:30"]
        });

        assert_eq!(normalize(low_high, &fields), normalize(shifted, &fields),);

        let unsigned_low_high = json!({
            "projections": [],
            "predicate": ["u64:0000000000000002", "u64:0000000000000010"]
        });
        let unsigned_shifted = json!({
            "projections": [],
            "predicate": ["u64:0000000000000020", "u64:0000000000000030"]
        });
        assert_eq!(
            normalize(unsigned_low_high, &fields),
            normalize(unsigned_shifted, &fields),
        );
    }

    #[test]
    fn typed_null_kind_remains_structurally_significant() {
        let fields = BTreeMap::new();
        let integer = json!({
            "projections": [],
            "expression": {"kind": "null", "value": "integer"}
        });
        let text = json!({
            "projections": [],
            "expression": {"kind": "null", "value": "text"}
        });

        assert_ne!(normalize(integer, &fields), normalize(text, &fields));
    }

    #[test]
    fn text_literal_classes_preserve_empty_and_prefix_roles() {
        let fields = BTreeMap::new();
        let empty = json!({
            "projections": [],
            "expression": {"kind": "text", "value": ""}
        });
        let ordinary = json!({
            "projections": [],
            "expression": {"kind": "text", "value": "alpha"}
        });
        let prefix = json!({
            "projections": [],
            "predicate": {"prefix": "alpha"}
        });

        assert_ne!(
            normalize(empty, &fields),
            normalize(ordinary.clone(), &fields)
        );
        assert_ne!(normalize(ordinary, &fields), normalize(prefix, &fields));
    }

    fn normalize(value: Value, fields: &BTreeMap<u32, String>) -> Value {
        let aliases = projection_alias_slots(&value).expect("test aliases should derive");
        let literals = LiteralSlots::from_value(&value).expect("test literals should derive");
        normalize_select_value(value, fields, &aliases, &literals, None)
            .expect("test structural value should normalize")
    }
}
