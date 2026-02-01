use crate::prelude::*;
use std::collections::HashSet;

/// Validate and return the resolved entity name for downstream checks.
pub fn validate_entity_name(
    name: Option<&LitStr>,
    def_ident: &Ident,
) -> Result<String, DarlingError> {
    // Prefer explicit name override when provided.
    if let Some(name) = name {
        let value = name.value();
        if value.len() > MAX_ENTITY_NAME_LEN {
            return Err(DarlingError::custom(format!(
                "entity name '{value}' exceeds max length {MAX_ENTITY_NAME_LEN}"
            ))
            .with_span(name));
        }
        if !value.is_ascii() {
            return Err(
                DarlingError::custom(format!("entity name '{value}' must be ASCII"))
                    .with_span(name),
            );
        }

        return Ok(value);
    }

    // Fall back to the struct identifier.
    let value = def_ident.to_string();
    if value.len() > MAX_ENTITY_NAME_LEN {
        return Err(DarlingError::custom(format!(
            "entity name '{value}' exceeds max length {MAX_ENTITY_NAME_LEN}"
        ))
        .with_span(def_ident));
    }
    if !value.is_ascii() {
        return Err(
            DarlingError::custom(format!("entity name '{value}' must be ASCII"))
                .with_span(def_ident),
        );
    }

    Ok(value)
}

/// Validate index definitions against local entity fields.
pub fn validate_entity_indexes(
    entity_name: &str,
    fields: &FieldList,
    indexes: &[Index],
) -> Result<(), DarlingError> {
    for index in indexes {
        // Basic shape.
        if index.fields.is_empty() {
            return Err(
                DarlingError::custom("index must reference at least one field")
                    .with_span(&index.store),
            );
        }
        if index.fields.len() > MAX_INDEX_FIELDS {
            return Err(DarlingError::custom(format!(
                "index has {} fields; maximum is {}",
                index.fields.len(),
                MAX_INDEX_FIELDS
            ))
            .with_span(&index.store));
        }

        // Field existence, uniqueness, and cardinality.
        let mut seen = HashSet::new();
        for field in &index.fields {
            let field_name = field.to_string();
            if !seen.insert(field_name.clone()) {
                return Err(DarlingError::custom(format!(
                    "index contains duplicate field '{field_name}'"
                ))
                .with_span(field));
            }

            let Some(entity_field) = fields.get(field) else {
                return Err(
                    DarlingError::custom(format!("index field '{field_name}' not found"))
                        .with_span(field),
                );
            };
            if entity_field.value.cardinality() == Cardinality::Many {
                return Err(DarlingError::custom(
                    "cannot add an index field with many cardinality",
                )
                .with_span(field));
            }
        }

        // Name length.
        let mut len = entity_name.len();
        for field in &index.fields {
            len = len.saturating_add(1 + field.to_string().len());
        }
        if len > MAX_INDEX_NAME_LEN {
            let fields = index
                .fields
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            return Err(DarlingError::custom(format!(
                "index name '{entity_name}|{fields:?}' exceeds max length {MAX_INDEX_NAME_LEN}"
            ))
            .with_span(&index.store));
        }
    }

    // Check for redundant indexes (prefix relationships).
    for (i, a) in indexes.iter().enumerate() {
        for b in indexes.iter().skip(i + 1) {
            if a.unique == b.unique {
                let a_fields = a.fields.iter().map(ToString::to_string).collect::<Vec<_>>();
                let b_fields = b.fields.iter().map(ToString::to_string).collect::<Vec<_>>();

                if is_prefix_of(&a.fields, &b.fields) {
                    return Err(DarlingError::custom(format!(
                        "index {a_fields:?} is redundant (prefix of {b_fields:?})"
                    ))
                    .with_span(&a.store));
                }
                if is_prefix_of(&b.fields, &a.fields) {
                    return Err(DarlingError::custom(format!(
                        "index {b_fields:?} is redundant (prefix of {a_fields:?})"
                    ))
                    .with_span(&b.store));
                }
            }
        }
    }

    Ok(())
}

fn is_prefix_of(a: &[Ident], b: &[Ident]) -> bool {
    a.len() < b.len() && b.iter().take(a.len()).zip(a).all(|(b, a)| b == a)
}
