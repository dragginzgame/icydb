use crate::{
    db::{
        primitives::{Cmp, FilterClause, FilterExpr, LimitExpr, Order, SortExpr},
        query::LoadQuery,
    },
    traits::EntityKind,
    value::Value,
};
use std::fmt;

use super::{
    plan::{AccessPath, LogicalPlan, OrderDirection, OrderSpec, PageSpec},
    predicate::{
        self, CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate, SchemaInfo,
    },
};

#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("schema unavailable: {0}")]
    SchemaUnavailable(#[from] predicate::ValidateError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdapterWarning {
    AccessPathIgnored { field: String, cmp: String },
    CaseInsensitiveLost { field: String, cmp: String },
    EnumCaseInsensitive { field: String },
    MapEntryMalformed { field: String },
    PredicateDropped { reason: String },
}

impl fmt::Display for AdapterWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AccessPathIgnored { field, cmp } => write!(
                f,
                "access path inference skipped: {cmp} on '{field}' preserved as filter"
            ),
            Self::CaseInsensitiveLost { field, cmp } => write!(
                f,
                "case-insensitive semantics not preserved for {cmp} on '{field}'"
            ),
            Self::EnumCaseInsensitive { field } => write!(
                f,
                "enum comparisons are case-insensitive in v1; v2 uses strict equality for '{field}'"
            ),
            Self::MapEntryMalformed { field } => {
                write!(f, "map entry literal malformed for '{field}'")
            }
            Self::PredicateDropped { reason } => write!(f, "predicate dropped: {reason}"),
        }
    }
}

impl AdapterWarning {
    pub fn log(&self) {
        println!("[v1->v2 adapter] {self}");
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdaptedLoadPlan {
    pub plan: LogicalPlan,
    pub warnings: Vec<AdapterWarning>,
}

impl AdaptedLoadPlan {
    pub fn emit_warnings(&self) {
        for warning in &self.warnings {
            warning.log();
        }
    }
}

pub fn adapt_load_query<E: EntityKind>(query: &LoadQuery) -> Result<AdaptedLoadPlan, AdapterError> {
    let schema = SchemaInfo::from_entity::<E>()?;
    let mut warnings = Vec::new();

    if let Some(filter) = &query.filter
        && contains_primary_key_predicate(filter, E::PRIMARY_KEY)
    {
        warnings.push(AdapterWarning::AccessPathIgnored {
            field: E::PRIMARY_KEY.to_string(),
            cmp: "Eq/In".to_string(),
        });
    }

    let predicate = query
        .filter
        .as_ref()
        .and_then(|filter| translate_filter(filter, &schema, &mut warnings));

    let order = query.sort.as_ref().map(map_sort);
    let page = query.limit.as_ref().map(map_limit);

    let plan = LogicalPlan {
        access: AccessPath::FullScan,
        predicate,
        order,
        page,
    };

    Ok(AdaptedLoadPlan { plan, warnings })
}

fn map_sort(sort: &SortExpr) -> OrderSpec {
    let fields = sort
        .iter()
        .map(|(field, dir)| {
            let direction = match dir {
                Order::Asc => OrderDirection::Asc,
                Order::Desc => OrderDirection::Desc,
            };
            (field.clone(), direction)
        })
        .collect();

    OrderSpec { fields }
}

fn map_limit(limit: &LimitExpr) -> PageSpec {
    PageSpec {
        limit: limit.limit,
        offset: limit.offset,
    }
}

fn translate_filter(
    expr: &FilterExpr,
    schema: &SchemaInfo,
    warnings: &mut Vec<AdapterWarning>,
) -> Option<Predicate> {
    match expr {
        FilterExpr::True => Some(Predicate::True),
        FilterExpr::False => Some(Predicate::False),
        FilterExpr::Clause(clause) => translate_clause(clause, schema, warnings),
        FilterExpr::And(children) => {
            let mut out = Vec::with_capacity(children.len());
            for child in children {
                let Some(mapped) = translate_filter(child, schema, warnings) else {
                    warnings.push(AdapterWarning::PredicateDropped {
                        reason: "unsupported AND child".to_string(),
                    });
                    return None;
                };
                out.push(mapped);
            }
            Some(Predicate::And(out))
        }
        FilterExpr::Or(children) => {
            let mut out = Vec::with_capacity(children.len());
            for child in children {
                let Some(mapped) = translate_filter(child, schema, warnings) else {
                    warnings.push(AdapterWarning::PredicateDropped {
                        reason: "unsupported OR child".to_string(),
                    });
                    return None;
                };
                out.push(mapped);
            }
            Some(Predicate::Or(out))
        }
        FilterExpr::Not(inner) => {
            let mapped = translate_filter(inner, schema, warnings)?;
            Some(Predicate::Not(Box::new(mapped)))
        }
    }
}

fn translate_clause(
    clause: &FilterClause,
    schema: &SchemaInfo,
    warnings: &mut Vec<AdapterWarning>,
) -> Option<Predicate> {
    let field = clause.field.clone();

    match clause.cmp {
        Cmp::IsNone => Some(Predicate::IsNull { field }),
        Cmp::IsSome => {
            let not_null = Predicate::Not(Box::new(Predicate::IsNull {
                field: field.clone(),
            }));
            let not_missing = Predicate::Not(Box::new(Predicate::IsMissing { field }));
            Some(Predicate::And(vec![not_null, not_missing]))
        }
        Cmp::IsEmpty => Some(Predicate::IsEmpty { field }),
        Cmp::IsNotEmpty => Some(Predicate::IsNotEmpty { field }),

        Cmp::MapContainsKey => Some(Predicate::MapContainsKey {
            field,
            key: clause.value.clone(),
            coercion: map_coercion(schema, &clause.field, &clause.value, warnings),
        }),
        Cmp::MapContainsValue => Some(Predicate::MapContainsValue {
            field,
            value: clause.value.clone(),
            coercion: map_coercion(schema, &clause.field, &clause.value, warnings),
        }),
        Cmp::MapContainsEntry => {
            map_contains_entry(field, clause.value.clone(), schema, &clause.field, warnings)
        }
        Cmp::MapNotContainsKey => Some(Predicate::Not(Box::new(Predicate::MapContainsKey {
            field,
            key: clause.value.clone(),
            coercion: map_coercion(schema, &clause.field, &clause.value, warnings),
        }))),
        Cmp::MapNotContainsValue => Some(Predicate::Not(Box::new(Predicate::MapContainsValue {
            field,
            value: clause.value.clone(),
            coercion: map_coercion(schema, &clause.field, &clause.value, warnings),
        }))),
        Cmp::MapNotContainsEntry => {
            let entry =
                map_contains_entry(field, clause.value.clone(), schema, &clause.field, warnings)?;
            Some(Predicate::Not(Box::new(entry)))
        }

        cmp => translate_compare(&clause.field, cmp, &clause.value, schema, warnings),
    }
}

fn map_contains_entry(
    field: String,
    literal: Value,
    schema: &SchemaInfo,
    field_name: &str,
    warnings: &mut Vec<AdapterWarning>,
) -> Option<Predicate> {
    let Value::List(mut pair) = literal else {
        warnings.push(AdapterWarning::MapEntryMalformed {
            field: field_name.to_string(),
        });
        return None;
    };

    if pair.len() != 2 {
        warnings.push(AdapterWarning::MapEntryMalformed {
            field: field_name.to_string(),
        });
        return None;
    }

    let value = pair.pop().unwrap_or(Value::Unsupported);
    let key = pair.pop().unwrap_or(Value::Unsupported);

    let coercion = map_coercion(schema, field_name, &value, warnings);

    Some(Predicate::MapContainsEntry {
        field,
        key,
        value,
        coercion,
    })
}

fn translate_compare(
    field: &str,
    cmp: Cmp,
    value: &Value,
    schema: &SchemaInfo,
    warnings: &mut Vec<AdapterWarning>,
) -> Option<Predicate> {
    let (op, coercion) = match cmp {
        Cmp::Eq => (
            CompareOp::Eq,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::Ne => (
            CompareOp::Ne,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::Lt => (
            CompareOp::Lt,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::Lte => (
            CompareOp::Lte,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::Gt => (
            CompareOp::Gt,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::Gte => (
            CompareOp::Gte,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::In => (
            CompareOp::In,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::NotIn => (
            CompareOp::NotIn,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::AnyIn => (
            CompareOp::AnyIn,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::AllIn => (
            CompareOp::AllIn,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::Contains => (
            CompareOp::Contains,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::StartsWith => (
            CompareOp::StartsWith,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::EndsWith => (
            CompareOp::EndsWith,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::EqCi => (
            CompareOp::Eq,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::NeCi => (
            CompareOp::Ne,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::InCi => (
            CompareOp::In,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::AnyInCi => (
            CompareOp::AnyIn,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::AllInCi => (
            CompareOp::AllIn,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::ContainsCi => (
            CompareOp::Contains,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::StartsWithCi => (
            CompareOp::StartsWith,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::EndsWithCi => (
            CompareOp::EndsWith,
            select_coercion(field, cmp, value, schema, warnings),
        ),
        Cmp::IsNone
        | Cmp::IsSome
        | Cmp::IsEmpty
        | Cmp::IsNotEmpty
        | Cmp::MapContainsKey
        | Cmp::MapContainsValue
        | Cmp::MapContainsEntry
        | Cmp::MapNotContainsKey
        | Cmp::MapNotContainsValue
        | Cmp::MapNotContainsEntry => return None,
    };

    Some(Predicate::Compare(ComparePredicate {
        field: field.to_string(),
        op,
        value: value.clone(),
        coercion,
    }))
}

fn select_coercion(
    field: &str,
    cmp: Cmp,
    value: &Value,
    schema: &SchemaInfo,
    warnings: &mut Vec<AdapterWarning>,
) -> CoercionSpec {
    let field_type = schema.field(field);
    let case_insensitive = matches!(
        cmp,
        Cmp::EqCi
            | Cmp::NeCi
            | Cmp::InCi
            | Cmp::AnyInCi
            | Cmp::AllInCi
            | Cmp::ContainsCi
            | Cmp::StartsWithCi
            | Cmp::EndsWithCi
    );

    if case_insensitive {
        if field_type_is_text(field_type) {
            return CoercionSpec::new(CoercionId::TextCasefold);
        }

        if field_type_is_text_element(field_type) {
            return CoercionSpec::new(CoercionId::TextCasefold);
        }

        warnings.push(AdapterWarning::CaseInsensitiveLost {
            field: field.to_string(),
            cmp: format!("{cmp:?}"),
        });

        return CoercionSpec::new(CoercionId::Strict);
    }

    if matches!(cmp, Cmp::Contains) && field_type_is_text(field_type) {
        return CoercionSpec::new(CoercionId::Strict);
    }

    if matches!(
        cmp,
        Cmp::Contains | Cmp::AnyIn | Cmp::AllIn | Cmp::In | Cmp::NotIn
    ) {
        if (field_type_is_identifier(field_type) && literal_contains_text(value))
            || (field_type_is_text(field_type) && literal_contains_identifier(value))
            || (field_type_is_identifier_element(field_type) && literal_contains_text(value))
            || (field_type_is_text_element(field_type) && literal_contains_identifier(value))
        {
            return CoercionSpec::new(CoercionId::IdentifierText);
        }

        return CoercionSpec::new(CoercionId::CollectionElement);
    }

    if matches!(
        cmp,
        Cmp::Eq | Cmp::Ne | Cmp::Lt | Cmp::Lte | Cmp::Gt | Cmp::Gte
    ) {
        if field_type_is_numeric(field_type) {
            return CoercionSpec::new(CoercionId::NumericWiden);
        }

        if field_type_is_enum(field_type) {
            warnings.push(AdapterWarning::EnumCaseInsensitive {
                field: field.to_string(),
            });
            return CoercionSpec::new(CoercionId::Strict);
        }

        if (field_type_is_identifier(field_type) && literal_contains_text(value))
            || (field_type_is_text(field_type) && literal_contains_identifier(value))
        {
            return CoercionSpec::new(CoercionId::IdentifierText);
        }

        return CoercionSpec::new(CoercionId::Strict);
    }

    CoercionSpec::new(CoercionId::Strict)
}

fn map_coercion(
    _schema: &SchemaInfo,
    _field: &str,
    _value: &Value,
    _warnings: &mut Vec<AdapterWarning>,
) -> CoercionSpec {
    CoercionSpec::new(CoercionId::Strict)
}

const fn field_type_is_numeric(field_type: Option<&predicate::validate::FieldType>) -> bool {
    matches!(
        field_type,
        Some(predicate::validate::FieldType::Scalar(
            predicate::validate::ScalarType::Date
                | predicate::validate::ScalarType::Decimal
                | predicate::validate::ScalarType::Duration
                | predicate::validate::ScalarType::E8s
                | predicate::validate::ScalarType::E18s
                | predicate::validate::ScalarType::Float32
                | predicate::validate::ScalarType::Float64
                | predicate::validate::ScalarType::Int
                | predicate::validate::ScalarType::Int128
                | predicate::validate::ScalarType::IntBig
                | predicate::validate::ScalarType::Timestamp
                | predicate::validate::ScalarType::Uint
                | predicate::validate::ScalarType::Uint128
                | predicate::validate::ScalarType::UintBig
        ))
    )
}

fn field_type_is_text(field_type: Option<&predicate::validate::FieldType>) -> bool {
    matches!(
        field_type,
        Some(predicate::validate::FieldType::Scalar(
            predicate::validate::ScalarType::Text
        ))
    )
}

fn field_type_is_text_element(field_type: Option<&predicate::validate::FieldType>) -> bool {
    match field_type {
        Some(
            predicate::validate::FieldType::List(inner)
            | predicate::validate::FieldType::Set(inner),
        ) => field_type_is_text(Some(inner)),
        _ => false,
    }
}

fn field_type_is_identifier(field_type: Option<&predicate::validate::FieldType>) -> bool {
    matches!(
        field_type,
        Some(predicate::validate::FieldType::Scalar(
            predicate::validate::ScalarType::Ulid
                | predicate::validate::ScalarType::Principal
                | predicate::validate::ScalarType::Account
        ))
    )
}

fn field_type_is_identifier_element(field_type: Option<&predicate::validate::FieldType>) -> bool {
    match field_type {
        Some(
            predicate::validate::FieldType::List(inner)
            | predicate::validate::FieldType::Set(inner),
        ) => field_type_is_identifier(Some(inner)),
        _ => false,
    }
}

fn field_type_is_enum(field_type: Option<&predicate::validate::FieldType>) -> bool {
    matches!(
        field_type,
        Some(predicate::validate::FieldType::Scalar(
            predicate::validate::ScalarType::Enum
        ))
    )
}

fn literal_contains_text(value: &Value) -> bool {
    match value {
        Value::Text(_) => true,
        Value::List(items) => items.iter().any(|item| matches!(item, Value::Text(_))),
        _ => false,
    }
}

fn literal_contains_identifier(value: &Value) -> bool {
    match value {
        Value::Account(_) | Value::Principal(_) | Value::Ulid(_) => true,
        Value::List(items) => items.iter().any(|item| {
            matches!(
                item,
                Value::Account(_) | Value::Principal(_) | Value::Ulid(_)
            )
        }),
        _ => false,
    }
}

fn contains_primary_key_predicate(expr: &FilterExpr, field: &str) -> bool {
    match expr {
        FilterExpr::Clause(clause) => {
            clause.field == field && matches!(clause.cmp, Cmp::Eq | Cmp::In | Cmp::EqCi | Cmp::InCi)
        }
        FilterExpr::And(children) | FilterExpr::Or(children) => children
            .iter()
            .any(|child| contains_primary_key_predicate(child, field)),
        FilterExpr::Not(inner) => contains_primary_key_predicate(inner, field),
        FilterExpr::True | FilterExpr::False => false,
    }
}
