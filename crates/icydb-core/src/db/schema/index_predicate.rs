//! Accepted filtered-index semantics, independent of SQL spelling and names.
//! Boundary: frontends bind catalog identity; consumers project current names
//! into the maintained predicate executor without parsing persisted SQL.

#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "sql"))]
use crate::db::schema::{PersistedSchemaSnapshot, check::bind_index_predicate_literal};

use crate::{
    db::{
        predicate::{CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate},
        schema::{
            AcceptedCheckCompareOpV1, AcceptedCheckExprV1, AcceptedCheckLiteralV1,
            AcceptedCheckValueExprV1, AcceptedValueCatalogHandle, FieldId, PersistedFieldSnapshot,
            check::decode_literal,
        },
    },
    error::InternalError,
    value::Value,
};

// Filtered predicates currently execute against direct physical slots. Native
// nested key paths remain index-key metadata, not a predicate execution route.
#[cfg(any(test, feature = "sql"))]
fn bind_field(name: &str, fields: &[PersistedFieldSnapshot]) -> Result<FieldId, InternalError> {
    fields
        .iter()
        .find(|field| field.name() == name)
        .map(PersistedFieldSnapshot::id)
        .ok_or_else(InternalError::store_unsupported)
}

fn field_kind(
    id: FieldId,
    fields: &[PersistedFieldSnapshot],
) -> Result<&crate::db::schema::AcceptedFieldKind, InternalError> {
    fields
        .iter()
        .find(|field| field.id() == id)
        .map(PersistedFieldSnapshot::kind)
        .ok_or_else(InternalError::store_corruption)
}

fn field_name(id: FieldId, fields: &[PersistedFieldSnapshot]) -> Result<String, InternalError> {
    fields
        .iter()
        .find(|field| field.id() == id)
        .map(|field| field.name().to_string())
        .ok_or_else(InternalError::store_corruption)
}

#[cfg(any(test, feature = "sql"))]
fn numeric_literal_kind(
    input: &Value,
) -> Result<crate::db::schema::AcceptedFieldKind, InternalError> {
    Ok(match input {
        Value::Int64(_) => crate::db::schema::AcceptedFieldKind::Int64,
        Value::Int128(_) => crate::db::schema::AcceptedFieldKind::Int128,
        Value::IntBig(_) => crate::db::schema::AcceptedFieldKind::IntBig {
            max_bytes: crate::db::schema::MAX_SCHEMA_SNAPSHOT_BYTES,
        },
        Value::Nat64(_) => crate::db::schema::AcceptedFieldKind::Nat64,
        Value::Nat128(_) => crate::db::schema::AcceptedFieldKind::Nat128,
        Value::NatBig(_) => crate::db::schema::AcceptedFieldKind::NatBig {
            max_bytes: crate::db::schema::MAX_SCHEMA_SNAPSHOT_BYTES,
        },
        Value::Decimal(value) => crate::db::schema::AcceptedFieldKind::Decimal {
            scale: value.scale(),
        },
        Value::Float32(_) => crate::db::schema::AcceptedFieldKind::Float32,
        Value::Float64(_) => crate::db::schema::AcceptedFieldKind::Float64,
        Value::U256(_) => crate::db::schema::AcceptedFieldKind::U256,
        _ => return Err(InternalError::store_unsupported()),
    })
}

/// Current bound filtered-index tree. Operators and coercions are the same
/// maintained predicate vocabulary; literals retain accepted canonical payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedIndexPredicate {
    True,
    False,
    Not(Box<Self>),
    And(Vec<Self>),
    Or(Vec<Self>),
    Compare {
        field: FieldId,
        op: CompareOp,
        coercion: CoercionId,
        // IN/NOT IN retain each member, including explicit NULL. Other operators
        // have exactly one item; no runtime scalar payload is persisted.
        values: Vec<Option<AcceptedCheckLiteralV1>>,
    },
    CompareFields {
        left: FieldId,
        op: CompareOp,
        right: FieldId,
        coercion: CoercionId,
    },
    IsNull(FieldId),
    IsNotNull(FieldId),
}

impl AcceptedIndexPredicate {
    /// Bind frontend predicates before discarding any authored branch. Every
    /// referenced field must be valid even if normalization removes the branch.
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) fn bind(
        predicate: &Predicate,
        snapshot: &PersistedSchemaSnapshot,
        catalog: &AcceptedValueCatalogHandle,
    ) -> Result<Self, InternalError> {
        let bound = Self::bind_fields(predicate, snapshot.fields(), catalog)?;
        let normalized =
            crate::db::predicate::normalize(bound.to_predicate(snapshot.fields(), catalog)?);
        Self::bind_fields(&normalized, snapshot.fields(), catalog)
    }

    #[cfg(any(test, feature = "sql"))]
    fn bind_fields(
        predicate: &Predicate,
        fields: &[PersistedFieldSnapshot],
        catalog: &AcceptedValueCatalogHandle,
    ) -> Result<Self, InternalError> {
        let bind = |predicate: &Predicate| Self::bind_fields(predicate, fields, catalog);
        let bound = match predicate {
            Predicate::True => Self::True,
            Predicate::False => Self::False,
            Predicate::Not(inner) => Self::Not(Box::new(bind(inner)?)),
            Predicate::And(children) => {
                Self::And(children.iter().map(bind).collect::<Result<_, _>>()?)
            }
            Predicate::Or(children) => {
                Self::Or(children.iter().map(bind).collect::<Result<_, _>>()?)
            }
            Predicate::Compare(compare) => {
                let field = bind_field(compare.field(), fields)?;
                let kind = crate::db::schema::query_field_kind_from_persisted_kind(
                    field_kind(field, fields)?,
                    catalog.composite_catalog(),
                );
                let element_kind = match (&kind, compare.op()) {
                    (
                        crate::db::schema::AcceptedFieldKind::List(inner)
                        | crate::db::schema::AcceptedFieldKind::Set(inner),
                        CompareOp::Contains,
                    ) => inner.as_ref(),
                    _ => &kind,
                };
                let inputs = match (compare.op(), compare.value()) {
                    (CompareOp::In | CompareOp::NotIn, Value::List(values)) => values.as_slice(),
                    _ => std::slice::from_ref(compare.value()),
                };
                let mut values = Vec::with_capacity(inputs.len());
                for input in inputs {
                    let literal = if matches!(input, Value::Null) {
                        None
                    } else {
                        let literal_kind = if compare.coercion().id() == CoercionId::NumericWiden {
                            numeric_literal_kind(input)?
                        } else {
                            element_kind.clone()
                        };
                        let input = match input {
                            Value::Enum(value) if value.payload().is_none() => {
                                let resolved = catalog.enum_catalog().resolve_value(value.canonical())
                                    .map_err(|_| InternalError::store_unsupported())?;
                                crate::value::InputValue::loose_enum(resolved.variant_name())
                            }
                            _ => crate::db::schema::input_value_from_strict_sql_literal_for_persisted_kind(&literal_kind, input)
                                .ok_or_else(InternalError::store_unsupported)?,
                        };
                        Some(
                            bind_index_predicate_literal(
                                input,
                                literal_kind,
                                catalog.enum_catalog(),
                                catalog.composite_catalog(),
                            )
                            .map_err(|_| InternalError::store_unsupported())?,
                        )
                    };
                    values.push(literal);
                }
                Self::Compare {
                    field,
                    op: compare.op(),
                    coercion: compare.coercion().id(),
                    values,
                }
            }
            Predicate::CompareFields(compare) => Self::CompareFields {
                left: bind_field(compare.left_field(), fields)?,
                op: compare.op(),
                right: bind_field(compare.right_field(), fields)?,
                coercion: compare.coercion().id(),
            },
            Predicate::IsNull { field } => Self::IsNull(bind_field(field, fields)?),
            Predicate::IsNotNull { field } => Self::IsNotNull(bind_field(field, fields)?),
            Predicate::IsMissing { .. }
            | Predicate::IsEmpty { .. }
            | Predicate::IsNotEmpty { .. }
            | Predicate::TextContains { .. }
            | Predicate::TextContainsCi { .. } => return Err(InternalError::store_unsupported()),
        };
        bound.canonicalize()
    }

    #[cfg(test)]
    pub(in crate::db) fn bind_test_sql(sql: &str, fields: &[PersistedFieldSnapshot]) -> Self {
        let catalog = AcceptedValueCatalogHandle::new_for_tests(
            crate::db::schema::empty_accepted_enum_catalog_for_tests(),
            crate::db::schema::AcceptedCompositeCatalog::empty(),
            crate::db::schema::AcceptedSchemaRevision::INITIAL,
        );
        let predicate =
            crate::db::predicate::parse_sql_predicate(sql).expect("valid current frontend fixture");
        let bound =
            Self::bind_fields(&predicate, fields, &catalog).expect("fixture binds accepted fields");
        let normalized =
            crate::db::predicate::normalize(bound.to_predicate(fields, &catalog).unwrap());
        Self::bind_fields(&normalized, fields, &catalog).expect("normalized fixture binds")
    }

    #[cfg(test)]
    pub(in crate::db) const fn test_non_null(id: u32) -> Self {
        Self::IsNotNull(FieldId::new(id))
    }

    /// Check current accepted semantics without applying frontend-only admission
    /// limits to generated strict field comparisons. Execution remains shared.
    pub(in crate::db::schema) fn validate_semantics(
        &self,
        fields: &[PersistedFieldSnapshot],
        catalog: &AcceptedValueCatalogHandle,
        schema: &crate::db::schema::SchemaInfo,
    ) -> Result<(), InternalError> {
        match self {
            Self::And(children) | Self::Or(children) => {
                for child in children {
                    child.validate_semantics(fields, catalog, schema)?;
                }
                Ok(())
            }
            Self::Not(child) => child.validate_semantics(fields, catalog, schema),
            Self::Compare {
                field,
                op,
                coercion,
                values,
            } => {
                // Literal kinds retain admission bounds and nominal identity;
                // coarse runtime scalar families alone cannot prove this contract.
                if *coercion != CoercionId::NumericWiden {
                    let kind = crate::db::schema::query_field_kind_from_persisted_kind(
                        field_kind(*field, fields)?,
                        catalog.composite_catalog(),
                    );
                    let expected = match (&kind, op) {
                        (
                            crate::db::schema::AcceptedFieldKind::List(inner)
                            | crate::db::schema::AcceptedFieldKind::Set(inner),
                            CompareOp::Contains,
                        ) => inner.as_ref(),
                        _ => &kind,
                    };
                    for literal in values.iter().flatten() {
                        let actual = crate::db::schema::query_field_kind_from_persisted_kind(
                            literal.kind(),
                            catalog.composite_catalog(),
                        );
                        if &actual != expected {
                            return Err(InternalError::store_invariant());
                        }
                    }
                }
                crate::db::query::predicate::validate_predicate(
                    schema,
                    &self.to_predicate(fields, catalog)?,
                )
                .map_err(|_| InternalError::store_invariant())
            }
            Self::CompareFields {
                left,
                op,
                right,
                coercion: CoercionId::Strict,
            } => {
                let resolve = |id| {
                    Ok::<_, InternalError>(crate::db::schema::query_field_kind_from_persisted_kind(
                        field_kind(id, fields)?,
                        catalog.composite_catalog(),
                    ))
                };
                let left = resolve(*left)?;
                let right = resolve(*right)?;
                let semantics = crate::db::schema::classify_accepted_field_kind(&left);
                if !op.supports_field_compare()
                    || !semantics.is_scalar()
                    || !semantics.is_sql_comparable()
                    || (!op.is_equality_family() && !semantics.is_orderable())
                    || crate::db::schema::field_type_from_persisted_kind(&left)
                        != crate::db::schema::field_type_from_persisted_kind(&right)
                    || (matches!(left, crate::db::schema::AcceptedFieldKind::Enum { .. })
                        && left != right)
                {
                    return Err(InternalError::store_invariant());
                }
                Ok(())
            }
            _ => crate::db::query::predicate::validate_predicate(
                schema,
                &self.to_predicate(fields, catalog)?,
            )
            .map_err(|_| InternalError::store_invariant()),
        }
    }

    /// Generated declarations already have admitted literals and field identity;
    /// preserve those facts directly instead of rendering and reparsing them.
    pub(in crate::db::schema) fn from_check(
        expression: &AcceptedCheckExprV1,
        fields: &[PersistedFieldSnapshot],
        composites: &crate::db::schema::AcceptedCompositeCatalog,
    ) -> Result<Self, InternalError> {
        let field = |operand: &AcceptedCheckValueExprV1| match operand {
            AcceptedCheckValueExprV1::Field(root) => Ok(*root),
            _ => Err(InternalError::store_unsupported()),
        };
        let bound = match expression {
            AcceptedCheckExprV1::True => Self::True,
            AcceptedCheckExprV1::False => Self::False,
            AcceptedCheckExprV1::Not(inner) => {
                Self::Not(Box::new(Self::from_check(inner, fields, composites)?))
            }
            AcceptedCheckExprV1::And(children) => Self::And(
                children
                    .iter()
                    .map(|child| Self::from_check(child, fields, composites))
                    .collect::<Result<_, _>>()?,
            ),
            AcceptedCheckExprV1::Or(children) => Self::Or(
                children
                    .iter()
                    .map(|child| Self::from_check(child, fields, composites))
                    .collect::<Result<_, _>>()?,
            ),
            AcceptedCheckExprV1::IsNull(value) => Self::IsNull(field(value)?),
            AcceptedCheckExprV1::IsNotNull(value) => Self::IsNotNull(field(value)?),
            AcceptedCheckExprV1::Compare { left, op, right } => {
                let op = match op {
                    AcceptedCheckCompareOpV1::Eq => CompareOp::Eq,
                    AcceptedCheckCompareOpV1::Ne => CompareOp::Ne,
                    AcceptedCheckCompareOpV1::Lt => CompareOp::Lt,
                    AcceptedCheckCompareOpV1::Lte => CompareOp::Lte,
                    AcceptedCheckCompareOpV1::Gt => CompareOp::Gt,
                    AcceptedCheckCompareOpV1::Gte => CompareOp::Gte,
                };
                match (left, right) {
                    (_, AcceptedCheckValueExprV1::Literal(value)) => Self::Compare {
                        field: field(left)?,
                        op,
                        coercion: CoercionId::Strict,
                        values: vec![Some(value.clone())],
                    },
                    (AcceptedCheckValueExprV1::Literal(value), _) => Self::Compare {
                        field: field(right)?,
                        op: op.flipped(),
                        coercion: CoercionId::Strict,
                        values: vec![Some(value.clone())],
                    },
                    _ => {
                        let left = field(left)?;
                        let right = field(right)?;
                        let kind = crate::db::schema::query_field_kind_from_persisted_kind(
                            field_kind(left, fields)?,
                            composites,
                        );
                        // The source CHECK has already proved equal operand kinds.
                        // Numeric field comparisons use the existing executor's
                        // qualified field-to-field coercion contract.
                        let numeric = crate::db::schema::field_type_from_persisted_kind(&kind)
                            .supports_numeric_coercion();
                        Self::CompareFields {
                            left,
                            op,
                            right,
                            coercion: if numeric {
                                CoercionId::NumericWiden
                            } else {
                                CoercionId::Strict
                            },
                        }
                    }
                }
            }
        };
        bound.canonicalize()
    }

    /// Project bound identity into current accepted names and decode admitted
    /// payloads for the existing executor. This never invokes the SQL parser.
    pub(in crate::db) fn to_predicate(
        &self,
        fields: &[PersistedFieldSnapshot],
        catalog: &AcceptedValueCatalogHandle,
    ) -> Result<Predicate, InternalError> {
        self.project(&|field| field_name(*field, fields), catalog)
    }

    /// Project a catalog-derived root-name binding retained by schema metadata.
    pub(in crate::db) fn to_predicate_with_names(
        &self,
        names: &[(FieldId, String)],
        catalog: &AcceptedValueCatalogHandle,
    ) -> Result<Predicate, InternalError> {
        self.project(
            &|field| {
                let (_, root) = names
                    .iter()
                    .find(|(id, _)| id == field)
                    .ok_or_else(InternalError::store_corruption)?;
                let name = root.clone();
                Ok(name)
            },
            catalog,
        )
    }

    pub(in crate::db) fn for_row_contract(
        &self,
        contract: &crate::db::data::StructuralRowContract,
    ) -> Result<Predicate, InternalError> {
        self.project(
            &|field| {
                let mut name = None;
                for slot in 0..contract.field_count() {
                    if !contract.has_active_field_slot(slot) {
                        continue;
                    }
                    if contract.required_accepted_field_contract(slot)?.field_id() == *field {
                        name = Some(contract.field_name(slot)?.to_string());
                        break;
                    }
                }
                let name = name.ok_or_else(InternalError::store_corruption)?;
                Ok(name)
            },
            contract.accepted_value_catalog_handle(),
        )
    }

    fn project(
        &self,
        name: &dyn Fn(&FieldId) -> Result<String, InternalError>,
        catalog: &AcceptedValueCatalogHandle,
    ) -> Result<Predicate, InternalError> {
        let project = |predicate: &Self| predicate.project(name, catalog);
        let literal = |value: &AcceptedCheckLiteralV1| {
            decode_literal(value, catalog).map_err(|_| InternalError::store_corruption())
        };
        Ok(match self {
            Self::True => Predicate::True,
            Self::False => Predicate::False,
            Self::Not(inner) => Predicate::Not(Box::new(project(inner)?)),
            Self::And(children) => {
                Predicate::And(children.iter().map(project).collect::<Result<_, _>>()?)
            }
            Self::Or(children) => {
                Predicate::Or(children.iter().map(project).collect::<Result<_, _>>()?)
            }
            Self::Compare {
                field,
                op,
                coercion,
                values,
            } => {
                let values = values
                    .iter()
                    .map(|value| {
                        value
                            .as_ref()
                            .map(literal)
                            .transpose()
                            .map(|value| value.unwrap_or(Value::Null))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let value = if matches!(op, CompareOp::In | CompareOp::NotIn) {
                    Value::List(values)
                } else {
                    values
                        .into_iter()
                        .next()
                        .ok_or_else(InternalError::store_corruption)?
                };
                Predicate::Compare(ComparePredicate::with_coercion(
                    name(field)?,
                    *op,
                    value,
                    *coercion,
                ))
            }
            Self::CompareFields {
                left,
                op,
                right,
                coercion,
            } => Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                name(left)?,
                *op,
                name(right)?,
                *coercion,
            )),
            Self::IsNull(field) => Predicate::IsNull {
                field: name(field)?,
            },
            Self::IsNotNull(field) => Predicate::IsNotNull {
                field: name(field)?,
            },
        })
    }

    /// Render display metadata from the accepted tree through current names.
    pub(in crate::db) fn render_sql(
        &self,
        fields: &[PersistedFieldSnapshot],
        catalog: &AcceptedValueCatalogHandle,
    ) -> Result<String, InternalError> {
        Self::render_predicate(self.to_predicate(fields, catalog)?, catalog)
    }

    pub(in crate::db) fn render_sql_with_names(
        &self,
        names: &[(FieldId, String)],
        catalog: &AcceptedValueCatalogHandle,
    ) -> Result<String, InternalError> {
        Self::render_predicate(self.to_predicate_with_names(names, catalog)?, catalog)
    }

    fn render_predicate(
        mut predicate: Predicate,
        catalog: &AcceptedValueCatalogHandle,
    ) -> Result<String, InternalError> {
        fn display_value(
            value: Value,
            catalog: &AcceptedValueCatalogHandle,
        ) -> Result<Value, InternalError> {
            Ok(match value {
                Value::Enum(value) if value.payload().is_none() => Value::Text(
                    catalog
                        .enum_catalog()
                        .resolve_value(value.canonical())
                        .map_err(|_| InternalError::store_corruption())?
                        .variant_name()
                        .to_string(),
                ),
                Value::Ulid(value) => Value::Text(value.to_string()),
                Value::List(values) => Value::List(
                    values
                        .into_iter()
                        .map(|value| display_value(value, catalog))
                        .collect::<Result<_, _>>()?,
                ),
                other => other,
            })
        }
        fn display(
            predicate: &mut Predicate,
            catalog: &AcceptedValueCatalogHandle,
        ) -> Result<(), InternalError> {
            match predicate {
                Predicate::And(children) | Predicate::Or(children) => {
                    for child in children {
                        display(child, catalog)?;
                    }
                }
                Predicate::Not(inner) => display(inner, catalog)?,
                Predicate::Compare(compare) => {
                    compare.value = display_value(compare.value.clone(), catalog)?;
                }
                _ => {}
            }
            Ok(())
        }
        display(&mut predicate, catalog)?;
        crate::db::predicate::render_sql_predicate(&predicate)
            .ok_or_else(InternalError::store_corruption)
    }

    pub(in crate::db::schema) fn validate_fields(
        &self,
        fields: &[PersistedFieldSnapshot],
    ) -> Result<(), InternalError> {
        if self != &self.clone().canonicalize()? {
            return Err(InternalError::store_corruption());
        }
        match self {
            Self::True | Self::False => {}
            Self::Not(inner) => inner.validate_fields(fields)?,
            Self::And(children) | Self::Or(children) => {
                for child in children {
                    child.validate_fields(fields)?;
                }
            }
            Self::CompareFields { left, right, .. } => {
                field_kind(*left, fields)?;
                field_kind(*right, fields)?;
            }
            Self::Compare { field, .. } | Self::IsNull(field) | Self::IsNotNull(field) => {
                field_kind(*field, fields)?;
            }
        }
        Ok(())
    }

    // Nullable uniqueness requires an exact non-null conjunction guard, not a
    // second literal evaluator. Read the same bound root identity directly.
    pub(in crate::db::schema) fn exact_non_null_guards(&self) -> Vec<FieldId> {
        match self {
            Self::And(children) => children
                .iter()
                .flat_map(Self::exact_non_null_guards)
                .collect(),
            Self::IsNotNull(field) => vec![*field],
            _ => Vec::new(),
        }
    }

    pub(in crate::db) fn references_field(&self, id: FieldId) -> bool {
        match self {
            Self::True | Self::False => false,
            Self::Not(inner) => inner.references_field(id),
            Self::And(children) | Self::Or(children) => {
                children.iter().any(|child| child.references_field(id))
            }
            Self::CompareFields { left, right, .. } => *left == id || *right == id,
            Self::Compare { field, .. } | Self::IsNull(field) | Self::IsNotNull(field) => {
                *field == id
            }
        }
    }

    fn canonicalize(self) -> Result<Self, InternalError> {
        let is_and = matches!(&self, Self::And(_));
        let normalized = match self {
            Self::Not(inner) => Self::Not(Box::new(inner.canonicalize()?)),
            Self::And(children) | Self::Or(children) => {
                // Canonical bytes, rather than editable names, own commutative
                // child identity. Every child has already been bound.
                let mut flattened = Vec::new();
                for child in children {
                    let child = child.canonicalize()?;
                    match child {
                        Self::And(inner) if is_and => flattened.extend(inner),
                        Self::Or(inner) if !is_and => flattened.extend(inner),
                        child => flattened.push(child),
                    }
                }
                let mut keyed = flattened
                    .into_iter()
                    .map(|child| Ok((child.canonical_bytes()?, child)))
                    .collect::<Result<Vec<_>, InternalError>>()?;
                keyed.sort_by(|left, right| left.0.cmp(&right.0));
                keyed.dedup_by(|left, right| left.0 == right.0);
                let mut children: Vec<_> = keyed.into_iter().map(|(_, child)| child).collect();
                if children.len() == 1 {
                    return children.pop().ok_or_else(InternalError::store_invariant);
                }
                if children.is_empty() {
                    return Ok(if is_and { Self::True } else { Self::False });
                }
                if is_and {
                    Self::And(children)
                } else {
                    Self::Or(children)
                }
            }
            Self::CompareFields {
                mut left,
                op,
                mut right,
                coercion,
            } => {
                if matches!(op, CompareOp::Eq | CompareOp::Ne) && left < right {
                    std::mem::swap(&mut left, &mut right);
                }
                Self::CompareFields {
                    left,
                    op,
                    right,
                    coercion,
                }
            }
            other => other,
        };
        normalized.canonical_bytes()?;
        Ok(normalized)
    }

    pub(in crate::db) fn canonical_bytes(&self) -> Result<Vec<u8>, InternalError> {
        crate::db::schema::codec::encode_index_predicate_bytes(self)
    }
}

crate::retained::retained_fields!(AcceptedIndexPredicate {
    Self::True | Self::False => [],
    Self::Not(inner) => [inner],
    Self::And(children) | Self::Or(children) => [children],
    Self::Compare {field, op, coercion, values} => [field, op, coercion, values],
    Self::CompareFields {left, op, right, coercion} => [left, op, right, coercion],
    Self::IsNull(field) | Self::IsNotNull(field) => [field],
});
