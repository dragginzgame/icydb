//! Nullable unique-index acceptance over structurally valid schema metadata.

use crate::db::{
    predicate::{Predicate, normalize, parse_sql_predicate},
    schema::{
        FieldId, PersistedFieldSnapshot, PersistedIndexExpressionOp,
        PersistedIndexExpressionSnapshot, PersistedIndexFieldPathSnapshot,
        PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
        SchemaIndexId, SchemaRowLayout,
    },
};

/// One bounded semantic rejection for an otherwise structural unique index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum NullableUniqueIndexContractError {
    /// A nullable nested ancestor can make the maintained physical resolver
    /// fail instead of producing the omission required by this contract.
    UnsupportedNullableAncestor {
        index_id: SchemaIndexId,
        index_name: String,
        source: Vec<String>,
    },
    /// One or more physically omittable sources lack exact top-level guards.
    MissingGuards {
        index_id: SchemaIndexId,
        index_name: String,
        sources: Vec<Vec<String>>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceOmissionClass {
    NeverOmits,
    TerminalNullOmits,
    UnsupportedNullableAncestor,
    InvalidSourcePath,
}

/// Validate one index through the sole nullable-unique semantic authority.
///
/// Base snapshot/index integrity must run first for raw accepted state. SQL
/// candidate binding may call this directly because its key builder has
/// already resolved the same accepted row-layout and field metadata.
pub(in crate::db) fn validate_nullable_unique_index_contract(
    row_layout: &SchemaRowLayout,
    fields: &[PersistedFieldSnapshot],
    index: &PersistedIndexSnapshot,
) -> Result<(), super::SchemaSnapshotAcceptanceError> {
    if !index.unique() {
        return Ok(());
    }

    let sources = distinct_index_sources(index);
    let classes = sources
        .iter()
        .map(|source| source_omission_class(row_layout, fields, source))
        .collect::<Vec<_>>();
    if classes
        .iter()
        .all(|class| matches!(class, SourceOmissionClass::NeverOmits))
    {
        return Ok(());
    }
    if classes
        .iter()
        .any(|class| matches!(class, SourceOmissionClass::InvalidSourcePath))
    {
        return Err(super::SchemaSnapshotAcceptanceError::Structural);
    }

    let bound_predicate = index
        .predicate_sql()
        .map(|sql| {
            let predicate = parse_sql_predicate(sql)
                .map_err(|_| super::SchemaSnapshotAcceptanceError::Predicate)?;
            let predicate = normalize(&predicate);
            if !predicate_fields_bind(fields, &predicate) {
                return Err(super::SchemaSnapshotAcceptanceError::Predicate);
            }
            Ok(predicate)
        })
        .transpose()?;

    for (source, class) in sources.iter().zip(&classes) {
        if matches!(class, SourceOmissionClass::UnsupportedNullableAncestor) {
            return Err(super::SchemaSnapshotAcceptanceError::NullableUnique(
                NullableUniqueIndexContractError::UnsupportedNullableAncestor {
                    index_id: index.schema_id(),
                    index_name: index.name().to_string(),
                    source: source.path().to_vec(),
                },
            ));
        }
    }

    let guarded_fields = bound_predicate
        .as_ref()
        .map(|predicate| exact_top_level_non_null_guards(fields, predicate))
        .unwrap_or_default();
    let mut missing = Vec::new();
    for (source, class) in sources.iter().zip(classes) {
        if !matches!(class, SourceOmissionClass::TerminalNullOmits) {
            continue;
        }
        let guarded = source.path().len() == 1 && guarded_fields.contains(&source.field_id());
        if !guarded {
            missing.push(source.path().to_vec());
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(super::SchemaSnapshotAcceptanceError::NullableUnique(
            NullableUniqueIndexContractError::MissingGuards {
                index_id: index.schema_id(),
                index_name: index.name().to_string(),
                sources: missing,
            },
        ))
    }
}

fn distinct_index_sources(index: &PersistedIndexSnapshot) -> Vec<&PersistedIndexFieldPathSnapshot> {
    let mut sources = Vec::new();
    match index.key() {
        PersistedIndexKeySnapshot::FieldPath(paths) => {
            for path in paths {
                push_distinct_source(&mut sources, path);
            }
        }
        PersistedIndexKeySnapshot::Items(items) => {
            for item in items {
                let source = match item {
                    PersistedIndexKeyItemSnapshot::FieldPath(path) => path,
                    PersistedIndexKeyItemSnapshot::Expression(expression) => {
                        expression_source(expression)
                    }
                };
                push_distinct_source(&mut sources, source);
            }
        }
    }
    sources
}

fn push_distinct_source<'a>(
    sources: &mut Vec<&'a PersistedIndexFieldPathSnapshot>,
    source: &'a PersistedIndexFieldPathSnapshot,
) {
    if !sources.iter().any(|accepted| {
        accepted.field_id() == source.field_id() && accepted.path() == source.path()
    }) {
        sources.push(source);
    }
}

const fn expression_source(
    expression: &PersistedIndexExpressionSnapshot,
) -> &PersistedIndexFieldPathSnapshot {
    match expression.op() {
        PersistedIndexExpressionOp::Lower
        | PersistedIndexExpressionOp::Upper
        | PersistedIndexExpressionOp::Trim
        | PersistedIndexExpressionOp::LowerTrim
        | PersistedIndexExpressionOp::Date
        | PersistedIndexExpressionOp::Year
        | PersistedIndexExpressionOp::Month
        | PersistedIndexExpressionOp::Day => expression.source(),
    }
}

fn source_omission_class(
    row_layout: &SchemaRowLayout,
    fields: &[PersistedFieldSnapshot],
    source: &PersistedIndexFieldPathSnapshot,
) -> SourceOmissionClass {
    let Some(field) = fields.iter().find(|field| field.id() == source.field_id()) else {
        return SourceOmissionClass::InvalidSourcePath;
    };
    if row_layout.slot_for_field(field.id()) != Some(source.slot())
        || source.path().first().map(String::as_str) != Some(field.name())
    {
        return SourceOmissionClass::InvalidSourcePath;
    }
    let Some(relative_path) = source.path().get(1..) else {
        return SourceOmissionClass::InvalidSourcePath;
    };
    if relative_path.is_empty() {
        return if field.kind() == source.kind() && field.nullable() == source.nullable() {
            if source.nullable() {
                SourceOmissionClass::TerminalNullOmits
            } else {
                SourceOmissionClass::NeverOmits
            }
        } else {
            SourceOmissionClass::InvalidSourcePath
        };
    }
    if field.nullable() {
        return SourceOmissionClass::UnsupportedNullableAncestor;
    }
    for prefix_len in 1..relative_path.len() {
        let Some(ancestor) = field
            .nested_leaves()
            .iter()
            .find(|leaf| leaf.path() == &relative_path[..prefix_len])
        else {
            return SourceOmissionClass::InvalidSourcePath;
        };
        if ancestor.nullable() {
            return SourceOmissionClass::UnsupportedNullableAncestor;
        }
    }
    let Some(terminal) = field
        .nested_leaves()
        .iter()
        .find(|leaf| leaf.path() == relative_path)
    else {
        return SourceOmissionClass::InvalidSourcePath;
    };
    if terminal.kind() != source.kind() || terminal.nullable() != source.nullable() {
        return SourceOmissionClass::InvalidSourcePath;
    }
    if source.nullable() {
        SourceOmissionClass::TerminalNullOmits
    } else {
        SourceOmissionClass::NeverOmits
    }
}

fn predicate_fields_bind(fields: &[PersistedFieldSnapshot], predicate: &Predicate) -> bool {
    match predicate {
        Predicate::True | Predicate::False => true,
        Predicate::And(children) | Predicate::Or(children) => children
            .iter()
            .all(|child| predicate_fields_bind(fields, child)),
        Predicate::Not(inner) => predicate_fields_bind(fields, inner),
        Predicate::Compare(compare) => field_id_by_name(fields, compare.field()).is_some(),
        Predicate::CompareFields(compare) => {
            field_id_by_name(fields, compare.left_field()).is_some()
                && field_id_by_name(fields, compare.right_field()).is_some()
        }
        Predicate::IsNull { field }
        | Predicate::IsNotNull { field }
        | Predicate::IsMissing { field }
        | Predicate::IsEmpty { field }
        | Predicate::IsNotEmpty { field }
        | Predicate::TextContains { field, .. }
        | Predicate::TextContainsCi { field, .. } => field_id_by_name(fields, field).is_some(),
    }
}

fn exact_top_level_non_null_guards(
    fields: &[PersistedFieldSnapshot],
    predicate: &Predicate,
) -> Vec<FieldId> {
    let mut guards = Vec::new();
    collect_exact_top_level_non_null_guards(fields, predicate, &mut guards);
    guards
}

fn collect_exact_top_level_non_null_guards(
    fields: &[PersistedFieldSnapshot],
    predicate: &Predicate,
    guards: &mut Vec<FieldId>,
) {
    match predicate {
        Predicate::And(children) => {
            for child in children {
                collect_exact_top_level_non_null_guards(fields, child, guards);
            }
        }
        Predicate::IsNotNull { field } => {
            if let Some(field_id) = field_id_by_name(fields, field)
                && !guards.contains(&field_id)
            {
                guards.push(field_id);
            }
        }
        Predicate::True
        | Predicate::False
        | Predicate::Or(_)
        | Predicate::Not(_)
        | Predicate::Compare(_)
        | Predicate::CompareFields(_)
        | Predicate::IsNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => {}
    }
}

fn field_id_by_name(fields: &[PersistedFieldSnapshot], name: &str) -> Option<FieldId> {
    fields
        .iter()
        .find(|field| field.name() == name)
        .map(PersistedFieldSnapshot::id)
}
