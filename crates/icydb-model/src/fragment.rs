//! Module: fragment
//!
//! Responsibility: lower one sealed host graph into a store-free database closure.
//!
//! Does not own: proposal routing, accepted identity, deployment configuration, or persistence.
//!
//! Boundary: converts compiler-authored logical facts into bounded public schema fragments.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use icydb_schema::{
    Account, Blob, ConstraintFragment, DEFAULT_BIG_INT_MAX_BYTES, Date, Decimal, Duration,
    EntityFragment, EntitySourceKey, EnumTypeFragment, EnumVariantFragment, FieldFragment,
    FieldInsertPolicy, FieldManagementPolicy, FieldSourceKey, FieldType, Float32, Float64,
    IndexFragment, IndexKeyFragment, IntBig, NamedTypeFragment, NatBig, Principal,
    RecordFieldFragment, RecordTypeFragment, RelationDeleteAction, RelationFragment, RuleSourceKey,
    ScalarLiteral, ScalarType, SchemaContractError, SchemaFragment, SchemaName,
    SourceRuleOperation, Subaccount, TargetedRuleFragment, Timestamp, TupleElementFragment,
    TypeSourceKey, Ulid, Unit,
};
use thiserror::Error;

use crate::{
    node::{
        Arg, ArgNumber, Canister, CheckConstraint, Entity, Enum, Field, FieldWriteManagement,
        Index, IndexExpression, IndexKeyItem, IndexKeyItemsRef, Item, ItemTarget, List, Map,
        Record, RelationEdge, Schema, SchemaNode, Set, SourceRule, SourceRuleKind, Store, Tuple,
        Value,
    },
    types::{Cardinality, Primitive},
};

/// Failure while projecting one validated host graph into public fragments.

#[derive(Debug, Error)]
pub enum FragmentLoweringError {
    /// A selected canister has no registered stores.
    #[error("schema canister has no registered stores: {0}")]
    CanisterHasNoStores(String),

    /// The selected canister path is not registered.
    #[error("schema canister path is not registered: {0}")]
    CanisterNotFound(String),

    /// The public bounded proposal contract rejected the projection.
    #[error(transparent)]
    Contract(#[from] SchemaContractError),

    /// Fragment projection requires the immutable post-validation graph.
    #[error("schema graph must be sealed before fragment lowering")]
    GraphNotSealed,

    /// A declared default cannot be represented by the public proposal atom.
    #[error("schema field default cannot be lowered: {0}")]
    InvalidDefault(String),

    /// One graph reference no longer resolves to the expected node kind.
    #[error("schema fragment reference is invalid: {0}")]
    InvalidReference(String),

    /// One authored value cardinality has no accepted proposal representation.
    #[error("schema value cardinality is unsupported at {0}")]
    UnsupportedCardinality(String),
}

// -----------------------------------------------------------------------------
// Database closure
// -----------------------------------------------------------------------------

impl Schema {
    /// Lower every persisted entity belonging to one canister, plus its exact
    /// reachable named-type and relation closure, into one store-free fragment.
    ///
    /// Store assignment remains a later proposal-composition concern.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the graph is not sealed, the selected
    /// canister/store closure is incomplete, or one authored fact cannot be
    /// represented by the bounded public proposal contract.
    pub fn schema_fragment_for_canister(
        &self,
        canister_path: &str,
    ) -> Result<SchemaFragment, FragmentLoweringError> {
        if !self.is_sealed() {
            return Err(FragmentLoweringError::GraphNotSealed);
        }
        self.cast_node::<Canister>(canister_path)
            .map_err(|_| FragmentLoweringError::CanisterNotFound(canister_path.to_string()))?;
        let stores = self
            .filter_nodes::<Store>(|store| store.canister() == canister_path)
            .map(|(path, _)| path.to_string())
            .collect::<BTreeSet<_>>();
        if stores.is_empty() {
            return Err(FragmentLoweringError::CanisterHasNoStores(
                canister_path.to_string(),
            ));
        }

        let entities = self
            .get_nodes::<Entity>()
            .filter(|(_, entity)| stores.contains(entity.store()))
            .map(|(_, entity)| entity)
            .collect::<Vec<_>>();
        let selected_entities = entities
            .iter()
            .map(|entity| entity.def().path())
            .collect::<BTreeSet<_>>();
        for entity in &entities {
            ensure_relation_targets_in_database(self, entity, &selected_entities)?;
        }

        let mut pending_types = Vec::new();
        let entity_fragments = entities
            .iter()
            .map(|entity| lower_entity(self, entity, &mut pending_types))
            .collect::<Result<Vec<_>, _>>()?;
        let types = lower_reachable_types(self, pending_types)?;

        SchemaFragment::try_new(entity_fragments, types).map_err(Into::into)
    }
}

fn ensure_relation_targets_in_database(
    schema: &Schema,
    entity: &Entity,
    selected_entities: &BTreeSet<String>,
) -> Result<(), FragmentLoweringError> {
    for target in entity
        .fields()
        .fields()
        .iter()
        .filter_map(|field| field.value().item().relation())
        .chain(entity.relations().iter().map(RelationEdge::target))
    {
        schema
            .cast_node::<Entity>(target)
            .map_err(|_| FragmentLoweringError::InvalidReference(target.to_string()))?;
        if !selected_entities.contains(target) {
            return Err(FragmentLoweringError::InvalidReference(format!(
                "relation target '{target}' is outside the selected database"
            )));
        }
    }
    Ok(())
}

fn lower_entity(
    schema: &Schema,
    entity: &Entity,
    pending_types: &mut Vec<String>,
) -> Result<EntityFragment, FragmentLoweringError> {
    let fields = entity
        .fields()
        .fields()
        .iter()
        .map(|field| lower_entity_field(schema, field, pending_types))
        .collect::<Result<Vec<_>, _>>()?;
    let primary_key = entity
        .primary_key()
        .fields()
        .iter()
        .map(|name| entity_field_source_key(entity, name))
        .collect::<Result<Vec<_>, _>>()?;
    let indexes = entity
        .indexes()
        .iter()
        .map(|index| lower_index(schema, entity, index))
        .collect::<Result<Vec<_>, _>>()?;
    let mut relations = entity
        .fields()
        .fields()
        .iter()
        .filter(|field| field.value().item().relation().is_some())
        .map(|field| lower_scalar_relation(schema, entity, field))
        .collect::<Result<Vec<_>, _>>()?;
    relations.extend(
        entity
            .relations()
            .iter()
            .map(|relation| lower_composite_relation(schema, entity, relation))
            .collect::<Result<Vec<_>, _>>()?,
    );
    let mut constraints = entity
        .constraints()
        .iter()
        .map(|constraint| lower_constraint(schema, constraint))
        .collect::<Result<Vec<_>, _>>()?;
    for field in entity.fields().fields() {
        constraints.extend(lower_field_rules(schema, field)?);
    }

    EntityFragment::try_new(
        SchemaName::try_new(entity.name())?,
        fields,
        primary_key,
        indexes,
        relations,
        constraints,
    )
    .map_err(Into::into)
}

fn lower_entity_field(
    schema: &Schema,
    field: &Field,
    pending_types: &mut Vec<String>,
) -> Result<FieldFragment, FragmentLoweringError> {
    let field_type = lower_value_type(schema, field.value(), pending_types)?;
    let nullable = field.value().cardinality() == Cardinality::Opt;
    let insert_policy = if field.generated().is_some() {
        FieldInsertPolicy::Generated
    } else if let Some(default) = field.default() {
        FieldInsertPolicy::Default(lower_default(schema, field, default)?)
    } else if nullable {
        FieldInsertPolicy::Nullable
    } else {
        FieldInsertPolicy::Required
    };
    let management = match field.write_management() {
        Some(FieldWriteManagement::CreatedAt) => Some(FieldManagementPolicy::CreatedAt),
        Some(FieldWriteManagement::UpdatedAt) => Some(FieldManagementPolicy::UpdatedAt),
        None => None,
    };
    Ok(FieldFragment::new(
        SchemaName::try_new(field.name())?,
        field_type,
        nullable,
        insert_policy,
        management,
    ))
}

fn lower_index(
    schema: &Schema,
    entity: &Entity,
    index: &Index,
) -> Result<IndexFragment, FragmentLoweringError> {
    let key = match index.key_items() {
        IndexKeyItemsRef::Fields(fields) => fields
            .iter()
            .map(|field| entity_field_source_key(entity, field).map(IndexKeyFragment::Field))
            .collect::<Result<Vec<_>, _>>()?,
        IndexKeyItemsRef::Items(items) => items
            .iter()
            .map(|item| lower_index_key(entity, item))
            .collect::<Result<Vec<_>, _>>()?,
    };
    IndexFragment::try_new(
        SchemaName::try_new(index.name())?,
        key,
        index.is_unique(),
        index.source_predicate(schema)?,
    )
    .map_err(Into::into)
}

fn lower_index_key(
    entity: &Entity,
    item: &IndexKeyItem,
) -> Result<IndexKeyFragment, FragmentLoweringError> {
    let field = entity_field_source_key(entity, item.field())?;
    Ok(match item {
        IndexKeyItem::Field(_) => IndexKeyFragment::Field(field),
        IndexKeyItem::Expression(IndexExpression::Lower(_)) => IndexKeyFragment::Lower(field),
        IndexKeyItem::Expression(IndexExpression::Upper(_)) => IndexKeyFragment::Upper(field),
        IndexKeyItem::Expression(IndexExpression::Trim(_)) => IndexKeyFragment::Trim(field),
        IndexKeyItem::Expression(IndexExpression::LowerTrim(_)) => {
            IndexKeyFragment::LowerTrim(field)
        }
        IndexKeyItem::Expression(IndexExpression::Date(_)) => IndexKeyFragment::Date(field),
        IndexKeyItem::Expression(IndexExpression::Year(_)) => IndexKeyFragment::Year(field),
        IndexKeyItem::Expression(IndexExpression::Month(_)) => IndexKeyFragment::Month(field),
        IndexKeyItem::Expression(IndexExpression::Day(_)) => IndexKeyFragment::Day(field),
    })
}

fn lower_scalar_relation(
    schema: &Schema,
    entity: &Entity,
    field: &Field,
) -> Result<RelationFragment, FragmentLoweringError> {
    let target_path = field
        .value()
        .item()
        .relation()
        .ok_or_else(|| FragmentLoweringError::InvalidReference(field.name().to_string()))?;
    let target = schema
        .cast_node::<Entity>(target_path)
        .map_err(|_| FragmentLoweringError::InvalidReference(target_path.to_string()))?;
    RelationFragment::try_new(
        SchemaName::try_new(field.name())?,
        vec![entity_field_source_key(entity, field.name())?],
        EntitySourceKey::try_new(target.name())?,
        target
            .primary_key()
            .fields()
            .iter()
            .map(|field| entity_field_source_key(target, field))
            .collect::<Result<Vec<_>, _>>()?,
        RelationDeleteAction::Restrict,
    )
    .map_err(Into::into)
}

fn lower_composite_relation(
    schema: &Schema,
    entity: &Entity,
    relation: &RelationEdge,
) -> Result<RelationFragment, FragmentLoweringError> {
    let target = schema
        .cast_node::<Entity>(relation.target())
        .map_err(|_| FragmentLoweringError::InvalidReference(relation.target().to_string()))?;
    RelationFragment::try_new(
        SchemaName::try_new(relation.name())?,
        relation
            .local_fields()
            .iter()
            .map(|field| entity_field_source_key(entity, field))
            .collect::<Result<Vec<_>, _>>()?,
        EntitySourceKey::try_new(target.name())?,
        target
            .primary_key()
            .fields()
            .iter()
            .map(|field| entity_field_source_key(target, field))
            .collect::<Result<Vec<_>, _>>()?,
        RelationDeleteAction::Restrict,
    )
    .map_err(Into::into)
}

fn lower_constraint(
    schema: &Schema,
    constraint: &CheckConstraint,
) -> Result<ConstraintFragment, FragmentLoweringError> {
    Ok(ConstraintFragment::check(
        SchemaName::try_new(constraint.name())?,
        constraint.source_expression(schema)?,
    ))
}

fn lower_field_rules(
    schema: &Schema,
    field: &Field,
) -> Result<Vec<ConstraintFragment>, FragmentLoweringError> {
    let field_source = FieldSourceKey::try_new(field.name())?;
    reachable_source_rules(schema, field.value().item())?
        .into_iter()
        .map(|(target_type, rule, target)| {
            let operation = lower_source_rule_operation(schema, target, rule)?;
            Ok(ConstraintFragment::targeted_rule(
                TargetedRuleFragment::new(
                    field_source.clone(),
                    target_type,
                    SchemaName::try_new(rule.name())?,
                    operation,
                ),
            ))
        })
        .collect()
}

type ReachableSourceRule<'schema> = (
    TypeSourceKey,
    &'schema SourceRule,
    &'schema crate::node::SchemaNode,
);

fn reachable_source_rules<'schema>(
    schema: &'schema Schema,
    item: &Item,
) -> Result<Vec<ReachableSourceRule<'schema>>, FragmentLoweringError> {
    let mut pending = Vec::new();
    push_item_reference(item, &mut pending);
    let mut visited = BTreeSet::new();
    let mut rules = BTreeMap::new();
    while let Some(path) = pending.pop() {
        let node = schema
            .get_node(path.as_str())
            .ok_or_else(|| FragmentLoweringError::InvalidReference(path.clone()))?;
        let target_type = TypeSourceKey::try_new(
            named_type_name(node)
                .ok_or_else(|| FragmentLoweringError::InvalidReference(path.clone()))?,
        )?;
        if !visited.insert(target_type.clone()) {
            continue;
        }
        for rule in schema_node_type(node)?.rules() {
            let key = (target_type.clone(), RuleSourceKey::try_new(rule.name())?);
            if rules.insert(key, (rule, node)).is_some() {
                return Err(FragmentLoweringError::InvalidReference(format!(
                    "duplicate durable rule '{}' on type '{}'",
                    rule.name(),
                    target_type
                )));
            }
        }
        push_schema_node_references(node, &mut pending);
    }
    Ok(rules
        .into_iter()
        .map(|((target_type, _), (rule, node))| (target_type, rule, node))
        .collect())
}

fn schema_node_type(node: &SchemaNode) -> Result<&crate::node::Type, FragmentLoweringError> {
    match node {
        SchemaNode::Newtype(node) => Ok(node.ty()),
        SchemaNode::Record(node) => Ok(node.ty()),
        SchemaNode::Enum(node) => Ok(node.ty()),
        SchemaNode::List(node) => Ok(node.ty()),
        SchemaNode::Map(node) => Ok(node.ty()),
        SchemaNode::Set(node) => Ok(node.ty()),
        SchemaNode::Tuple(node) => Ok(node.ty()),
        SchemaNode::Canister(_)
        | SchemaNode::Entity(_)
        | SchemaNode::Normalizer(_)
        | SchemaNode::Store(_)
        | SchemaNode::Validator(_) => Err(FragmentLoweringError::InvalidReference(
            "durable-rule target is not a named type".to_string(),
        )),
    }
}

fn push_schema_node_references(node: &SchemaNode, pending: &mut Vec<String>) {
    match node {
        SchemaNode::Newtype(newtype) => push_item_reference(newtype.item(), pending),
        SchemaNode::Record(record) => {
            for field in record.fields().fields() {
                push_item_reference(field.value().item(), pending);
            }
        }
        SchemaNode::Enum(r#enum) => {
            for value in r#enum
                .variants()
                .iter()
                .filter_map(crate::node::EnumVariant::value)
            {
                push_item_reference(value.item(), pending);
            }
        }
        SchemaNode::List(list) => push_item_reference(list.item(), pending),
        SchemaNode::Map(map) => {
            push_item_reference(map.key(), pending);
            push_item_reference(map.value().item(), pending);
        }
        SchemaNode::Set(set) => push_item_reference(set.item(), pending),
        SchemaNode::Tuple(tuple) => {
            for value in tuple.values() {
                push_item_reference(value.item(), pending);
            }
        }
        SchemaNode::Canister(_)
        | SchemaNode::Entity(_)
        | SchemaNode::Normalizer(_)
        | SchemaNode::Store(_)
        | SchemaNode::Validator(_) => {}
    }
}

fn push_item_reference(item: &Item, pending: &mut Vec<String>) {
    if let ItemTarget::Is(path) = item.target() {
        pending.push((*path).to_string());
    }
}

fn lower_source_rule_operation(
    schema: &Schema,
    target: &SchemaNode,
    rule: &SourceRule,
) -> Result<SourceRuleOperation, FragmentLoweringError> {
    let args = rule.args().0;
    let length_bound = |index: usize| {
        args.get(index)
            .and_then(|arg| match arg {
                Arg::Number(value) => arg_u128(value),
                _ => None,
            })
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| {
                FragmentLoweringError::InvalidReference(format!(
                    "rule '{}' has an invalid length bound",
                    rule.name()
                ))
            })
    };

    let operation = match rule.kind() {
        SourceRuleKind::NumericMinimum => {
            let RuleValueShape::Scalar(primitive, item) = resolve_rule_value_shape(schema, target)?
            else {
                return Err(invalid_rule_target(rule));
            };
            let value = args
                .first()
                .and_then(|arg| lower_scalar_default(primitive, item, arg))
                .ok_or_else(|| invalid_rule_target(rule))?;
            SourceRuleOperation::NumericMinimumInclusive { value }
        }
        SourceRuleKind::NumericRange => {
            let RuleValueShape::Scalar(primitive, item) = resolve_rule_value_shape(schema, target)?
            else {
                return Err(invalid_rule_target(rule));
            };
            let literal = |index: usize| {
                args.get(index)
                    .and_then(|arg| lower_scalar_default(primitive, item, arg))
                    .ok_or_else(|| invalid_rule_target(rule))
            };
            SourceRuleOperation::NumericRangeInclusive {
                min: literal(0)?,
                max: literal(1)?,
            }
        }
        SourceRuleKind::LengthRange => {
            let shape = resolve_rule_value_shape(schema, target)?;
            if !matches!(
                shape,
                RuleValueShape::Collection
                    | RuleValueShape::Scalar(Primitive::Blob | Primitive::Text, _)
            ) {
                return Err(invalid_rule_target(rule));
            }
            SourceRuleOperation::LengthRangeInclusive {
                min: length_bound(0)?,
                max: length_bound(1)?,
            }
        }
    };
    Ok(operation)
}

#[derive(Clone, Copy)]
enum RuleValueShape<'schema> {
    Collection,
    Scalar(Primitive, &'schema Item),
}

fn resolve_rule_value_shape<'schema>(
    schema: &'schema Schema,
    mut target: &'schema SchemaNode,
) -> Result<RuleValueShape<'schema>, FragmentLoweringError> {
    let mut visited = BTreeSet::new();
    loop {
        let source = named_type_name(target)
            .ok_or_else(|| FragmentLoweringError::InvalidReference("non-type rule".to_string()))?;
        if !visited.insert(source) {
            return Err(FragmentLoweringError::InvalidReference(format!(
                "durable-rule target cycle at '{source}'"
            )));
        }
        match target {
            SchemaNode::List(_) | SchemaNode::Map(_) | SchemaNode::Set(_) => {
                return Ok(RuleValueShape::Collection);
            }
            SchemaNode::Newtype(newtype) => match newtype.item().target() {
                ItemTarget::Primitive(primitive) => {
                    return Ok(RuleValueShape::Scalar(*primitive, newtype.item()));
                }
                ItemTarget::Is(path) => {
                    target = schema
                        .get_node(path)
                        .ok_or_else(|| FragmentLoweringError::InvalidReference(path.to_string()))?;
                }
            },
            SchemaNode::Record(_) | SchemaNode::Enum(_) | SchemaNode::Tuple(_) => {
                return Err(FragmentLoweringError::InvalidReference(format!(
                    "durable-rule target '{source}' has no supported scalar or collection value"
                )));
            }
            SchemaNode::Canister(_)
            | SchemaNode::Entity(_)
            | SchemaNode::Normalizer(_)
            | SchemaNode::Store(_)
            | SchemaNode::Validator(_) => {
                return Err(FragmentLoweringError::InvalidReference(
                    "non-type durable-rule target".to_string(),
                ));
            }
        }
    }
}

fn invalid_rule_target(rule: &SourceRule) -> FragmentLoweringError {
    FragmentLoweringError::InvalidReference(format!(
        "durable rule '{}' does not match its nominal target",
        rule.name()
    ))
}

fn entity_field_source_key(
    entity: &Entity,
    field_name: &str,
) -> Result<FieldSourceKey, FragmentLoweringError> {
    let field = entity
        .fields()
        .get(field_name)
        .ok_or_else(|| FragmentLoweringError::InvalidReference(field_name.to_string()))?;
    FieldSourceKey::try_new(field.name()).map_err(Into::into)
}

// -----------------------------------------------------------------------------
// Reachable named-type closure
// -----------------------------------------------------------------------------

fn lower_reachable_types(
    schema: &Schema,
    mut pending: Vec<String>,
) -> Result<Vec<NamedTypeFragment>, FragmentLoweringError> {
    let mut lowered = BTreeMap::new();
    while let Some(path) = pending.pop() {
        let node = schema
            .get_node(path.as_str())
            .ok_or_else(|| FragmentLoweringError::InvalidReference(path.clone()))?;
        let source_key = named_type_name(node)
            .ok_or_else(|| FragmentLoweringError::InvalidReference(path.clone()))?;
        if lowered.contains_key(source_key) {
            continue;
        }
        let fragment = lower_named_type(schema, node, &mut pending)?;
        lowered.insert(source_key.to_string(), fragment);
    }
    Ok(lowered.into_values().collect())
}

const fn named_type_name(node: &crate::node::SchemaNode) -> Option<&str> {
    match node {
        crate::node::SchemaNode::Enum(node) => Some(node.name()),
        crate::node::SchemaNode::List(node) => Some(node.name()),
        crate::node::SchemaNode::Map(node) => Some(node.name()),
        crate::node::SchemaNode::Newtype(node) => Some(node.name()),
        crate::node::SchemaNode::Record(node) => Some(node.name()),
        crate::node::SchemaNode::Set(node) => Some(node.name()),
        crate::node::SchemaNode::Tuple(node) => Some(node.name()),
        crate::node::SchemaNode::Canister(_)
        | crate::node::SchemaNode::Entity(_)
        | crate::node::SchemaNode::Normalizer(_)
        | crate::node::SchemaNode::Store(_)
        | crate::node::SchemaNode::Validator(_) => None,
    }
}

fn lower_named_type(
    schema: &Schema,
    node: &crate::node::SchemaNode,
    pending: &mut Vec<String>,
) -> Result<NamedTypeFragment, FragmentLoweringError> {
    match node {
        crate::node::SchemaNode::Record(record) => lower_record(schema, record, pending),
        crate::node::SchemaNode::Enum(r#enum) => lower_enum(schema, r#enum, pending),
        crate::node::SchemaNode::Newtype(newtype) => Ok(NamedTypeFragment::newtype(
            SchemaName::try_new(newtype.name())?,
            lower_item_type(schema, newtype.item(), pending)?,
        )),
        crate::node::SchemaNode::List(list) => lower_list(schema, list, pending),
        crate::node::SchemaNode::Set(set) => lower_set(schema, set, pending),
        crate::node::SchemaNode::Map(map) => lower_map(schema, map, pending),
        crate::node::SchemaNode::Tuple(tuple) => lower_tuple(schema, tuple, pending),
        crate::node::SchemaNode::Canister(_)
        | crate::node::SchemaNode::Entity(_)
        | crate::node::SchemaNode::Normalizer(_)
        | crate::node::SchemaNode::Store(_)
        | crate::node::SchemaNode::Validator(_) => Err(FragmentLoweringError::InvalidReference(
            "non-type graph node".to_string(),
        )),
    }
}

fn lower_record(
    schema: &Schema,
    record: &Record,
    pending: &mut Vec<String>,
) -> Result<NamedTypeFragment, FragmentLoweringError> {
    let fields = record
        .fields()
        .fields()
        .iter()
        .map(|field| {
            Ok(RecordFieldFragment::new(
                SchemaName::try_new(field.name())?,
                lower_value_type(schema, field.value(), pending)?,
                field.value().cardinality() == Cardinality::Opt,
            ))
        })
        .collect::<Result<Vec<_>, FragmentLoweringError>>()?;
    Ok(NamedTypeFragment::Record(RecordTypeFragment::try_new(
        SchemaName::try_new(record.name())?,
        fields,
    )?))
}

fn lower_enum(
    schema: &Schema,
    r#enum: &Enum,
    pending: &mut Vec<String>,
) -> Result<NamedTypeFragment, FragmentLoweringError> {
    let variants = r#enum
        .variants()
        .iter()
        .map(|variant| {
            let name = SchemaName::try_new(variant.name())?;
            match variant.value() {
                Some(value) if value.cardinality() == Cardinality::Opt => {
                    Err(FragmentLoweringError::UnsupportedCardinality(format!(
                        "{}::{}",
                        r#enum.def().path(),
                        variant.name()
                    )))
                }
                Some(value) => Ok(EnumVariantFragment::with_payload(
                    name,
                    lower_value_type(schema, value, pending)?,
                )),
                None => Ok(EnumVariantFragment::new(name)),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(NamedTypeFragment::Enum(EnumTypeFragment::try_new(
        SchemaName::try_new(r#enum.name())?,
        variants,
    )?))
}

fn lower_list(
    schema: &Schema,
    list: &List,
    pending: &mut Vec<String>,
) -> Result<NamedTypeFragment, FragmentLoweringError> {
    Ok(NamedTypeFragment::list(
        SchemaName::try_new(list.name())?,
        lower_item_type(schema, list.item(), pending)?,
    ))
}

fn lower_set(
    schema: &Schema,
    set: &Set,
    pending: &mut Vec<String>,
) -> Result<NamedTypeFragment, FragmentLoweringError> {
    Ok(NamedTypeFragment::set(
        SchemaName::try_new(set.name())?,
        lower_item_type(schema, set.item(), pending)?,
    ))
}

fn lower_map(
    schema: &Schema,
    map: &Map,
    pending: &mut Vec<String>,
) -> Result<NamedTypeFragment, FragmentLoweringError> {
    if map.value().cardinality() == Cardinality::Opt {
        return Err(FragmentLoweringError::UnsupportedCardinality(
            map.def().path(),
        ));
    }
    Ok(NamedTypeFragment::map(
        SchemaName::try_new(map.name())?,
        lower_item_type(schema, map.key(), pending)?,
        lower_value_type(schema, map.value(), pending)?,
    ))
}

fn lower_tuple(
    schema: &Schema,
    tuple: &Tuple,
    pending: &mut Vec<String>,
) -> Result<NamedTypeFragment, FragmentLoweringError> {
    let members = tuple
        .values()
        .iter()
        .map(|value| {
            Ok::<_, FragmentLoweringError>(TupleElementFragment::new(
                lower_value_type(schema, value, pending)?,
                value.cardinality() == Cardinality::Opt,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(NamedTypeFragment::tuple(
        SchemaName::try_new(tuple.name())?,
        members,
    ))
}

// -----------------------------------------------------------------------------
// Exact field contracts
// -----------------------------------------------------------------------------

fn lower_value_type(
    schema: &Schema,
    value: &Value,
    pending: &mut Vec<String>,
) -> Result<FieldType, FragmentLoweringError> {
    let item = lower_item_type(schema, value.item(), pending)?;
    Ok(if value.cardinality() == Cardinality::Many {
        FieldType::List(Box::new(item))
    } else {
        item
    })
}

fn lower_item_type(
    schema: &Schema,
    item: &Item,
    pending: &mut Vec<String>,
) -> Result<FieldType, FragmentLoweringError> {
    match item.target() {
        ItemTarget::Is(path) => {
            pending.push((*path).to_string());
            Ok(FieldType::Named(TypeSourceKey::try_new(
                type_source_key_for_path(schema, path)?,
            )?))
        }
        ItemTarget::Primitive(primitive) => {
            Ok(FieldType::Scalar(lower_scalar_type(*primitive, item)))
        }
    }
}

fn type_source_key_for_path<'schema>(
    schema: &'schema Schema,
    path: &str,
) -> Result<&'schema str, FragmentLoweringError> {
    let source = schema
        .get_node(path)
        .and_then(named_type_name)
        .ok_or_else(|| FragmentLoweringError::InvalidReference(path.to_string()))?;
    Ok(source)
}

fn lower_scalar_type(primitive: Primitive, item: &Item) -> ScalarType {
    match primitive {
        Primitive::Account => ScalarType::Account,
        Primitive::Blob => ScalarType::Blob {
            max_len: item.max_len(),
        },
        Primitive::Bool => ScalarType::Bool,
        Primitive::Date => ScalarType::Date,
        Primitive::Decimal => ScalarType::Decimal {
            scale: item.scale().unwrap_or(0),
        },
        Primitive::Duration => ScalarType::Duration,
        Primitive::Float32 => ScalarType::Float32,
        Primitive::Float64 => ScalarType::Float64,
        Primitive::Int8 => ScalarType::Int8,
        Primitive::Int16 => ScalarType::Int16,
        Primitive::Int32 => ScalarType::Int32,
        Primitive::Int64 => ScalarType::Int64,
        Primitive::Int128 => ScalarType::Int128,
        Primitive::IntBig => ScalarType::IntBig {
            max_bytes: item.max_bytes().unwrap_or(DEFAULT_BIG_INT_MAX_BYTES),
        },
        Primitive::Nat8 => ScalarType::Nat8,
        Primitive::Nat16 => ScalarType::Nat16,
        Primitive::Nat32 => ScalarType::Nat32,
        Primitive::Nat64 => ScalarType::Nat64,
        Primitive::Nat128 => ScalarType::Nat128,
        Primitive::NatBig => ScalarType::NatBig {
            max_bytes: item.max_bytes().unwrap_or(DEFAULT_BIG_INT_MAX_BYTES),
        },
        Primitive::Principal => ScalarType::Principal,
        Primitive::Subaccount => ScalarType::Subaccount,
        Primitive::Text => ScalarType::Text {
            max_len: item.max_len(),
        },
        Primitive::Timestamp => ScalarType::Timestamp,
        Primitive::Ulid => ScalarType::Ulid,
        Primitive::Unit => ScalarType::Unit,
    }
}

// -----------------------------------------------------------------------------
// Authored database defaults
// -----------------------------------------------------------------------------

fn lower_default(
    schema: &Schema,
    field: &Field,
    default: &Arg,
) -> Result<ScalarLiteral, FragmentLoweringError> {
    if let ItemTarget::Is(path) = field.value().item().target() {
        let Arg::ConstPath(default_path) = default else {
            return Err(FragmentLoweringError::InvalidDefault(
                field.name().to_string(),
            ));
        };
        let variant = default_path.rsplit("::").next().unwrap_or(default_path);
        return schema
            .enum_unit_literal(path, variant)
            .map_err(FragmentLoweringError::from);
    }
    let ItemTarget::Primitive(primitive) = field.value().item().target() else {
        return Err(FragmentLoweringError::InvalidDefault(
            field.name().to_string(),
        ));
    };
    lower_scalar_default(*primitive, field.value().item(), default)
        .ok_or_else(|| FragmentLoweringError::InvalidDefault(field.name().to_string()))
}

fn lower_scalar_default(primitive: Primitive, item: &Item, default: &Arg) -> Option<ScalarLiteral> {
    if default_constructor_is_zero(default) {
        return zero_scalar_literal(primitive, item);
    }
    match (primitive, default) {
        (Primitive::Account, Arg::String(value)) => {
            Account::from_str(value).ok().map(ScalarLiteral::Account)
        }
        (Primitive::Blob, Arg::String(value)) => Blob::try_new(value.as_bytes().to_vec())
            .ok()
            .map(ScalarLiteral::Blob),
        (Primitive::Bool, Arg::Bool(value)) => Some(ScalarLiteral::Bool(*value)),
        (Primitive::Date, Arg::String(value)) => Date::parse(value).map(ScalarLiteral::Date),
        (Primitive::Date, Arg::Number(value)) => arg_i128(value)
            .and_then(|value| i32::try_from(value).ok())
            .map(Date::from_days_since_epoch)
            .map(ScalarLiteral::Date),
        (Primitive::Decimal, Arg::String(value)) => Decimal::from_str(value)
            .ok()
            .and_then(|value| decimal_at_scale(value, item.scale().unwrap_or(0)))
            .map(ScalarLiteral::Decimal),
        (Primitive::Decimal, Arg::Number(value)) => arg_decimal(value)
            .and_then(|value| decimal_at_scale(value, item.scale().unwrap_or(0)))
            .map(ScalarLiteral::Decimal),
        (Primitive::Duration, Arg::String(value)) => Duration::parse_flexible(value)
            .ok()
            .map(ScalarLiteral::Duration),
        (Primitive::Duration, Arg::Number(value)) => arg_u128(value)
            .and_then(|value| u64::try_from(value).ok())
            .map(Duration::from_millis)
            .map(ScalarLiteral::Duration),
        (Primitive::Float32, Arg::Number(ArgNumber::Float32(value))) => {
            Float32::try_new(*value).map(ScalarLiteral::Float32)
        }
        (Primitive::Float64, Arg::Number(ArgNumber::Float64(value))) => {
            Float64::try_new(*value).map(ScalarLiteral::Float64)
        }
        (
            Primitive::Int8
            | Primitive::Int16
            | Primitive::Int32
            | Primitive::Int64
            | Primitive::Int128,
            Arg::Number(value),
        ) => arg_i128(value).map(ScalarLiteral::Int),
        (Primitive::IntBig, Arg::Number(value)) => arg_i128(value)
            .map(|value| value.to_string())
            .and_then(|value| IntBig::from_str(value.as_str()).ok())
            .map(ScalarLiteral::IntBig),
        (Primitive::IntBig, Arg::String(value)) => {
            IntBig::from_str(value).ok().map(ScalarLiteral::IntBig)
        }
        (
            Primitive::Nat8
            | Primitive::Nat16
            | Primitive::Nat32
            | Primitive::Nat64
            | Primitive::Nat128,
            Arg::Number(value),
        ) => arg_u128(value).map(ScalarLiteral::Nat),
        (Primitive::NatBig, Arg::Number(value)) => arg_u128(value)
            .map(|value| value.to_string())
            .and_then(|value| NatBig::from_str(value.as_str()).ok())
            .map(ScalarLiteral::NatBig),
        (Primitive::NatBig, Arg::String(value)) => {
            NatBig::from_str(value).ok().map(ScalarLiteral::NatBig)
        }
        (Primitive::Principal, Arg::String(value)) => Principal::from_str(value)
            .ok()
            .map(ScalarLiteral::Principal),
        (Primitive::Subaccount, Arg::String(value)) => parse_subaccount(value)
            .map(Subaccount::from_array)
            .map(ScalarLiteral::Subaccount),
        (Primitive::Text, Arg::String(value)) => Some(ScalarLiteral::Text((*value).to_string())),
        (Primitive::Timestamp, Arg::String(value)) => Timestamp::parse_flexible(value)
            .ok()
            .map(ScalarLiteral::Timestamp),
        (Primitive::Timestamp, Arg::Number(value)) => arg_i128(value)
            .and_then(|value| i64::try_from(value).ok())
            .map(Timestamp::from_millis)
            .map(ScalarLiteral::Timestamp),
        (Primitive::Ulid, Arg::String(value)) => {
            Ulid::from_str(value).ok().map(ScalarLiteral::Ulid)
        }
        (Primitive::Unit, Arg::ConstPath(path)) if path.ends_with("Unit") => {
            Some(ScalarLiteral::Unit(Unit))
        }
        _ => None,
    }
}

fn default_constructor_is_zero(default: &Arg) -> bool {
    let Arg::FuncPath(path) = default else {
        return false;
    };
    path.ends_with("::default")
        || path.ends_with("::new")
        || path.ends_with("::EPOCH")
        || path.ends_with("::nil")
}

fn zero_scalar_literal(primitive: Primitive, item: &Item) -> Option<ScalarLiteral> {
    match primitive {
        Primitive::Blob => Blob::try_new(Vec::new()).ok().map(ScalarLiteral::Blob),
        Primitive::Bool => Some(ScalarLiteral::Bool(false)),
        Primitive::Date => Some(ScalarLiteral::Date(Date::EPOCH)),
        Primitive::Decimal => Decimal::try_from_i128_with_scale(0, item.scale().unwrap_or(0))
            .map(ScalarLiteral::Decimal),
        Primitive::Duration => Some(ScalarLiteral::Duration(Duration::ZERO)),
        Primitive::Float32 => Float32::try_new(0.0).map(ScalarLiteral::Float32),
        Primitive::Float64 => Float64::try_new(0.0).map(ScalarLiteral::Float64),
        Primitive::Int8
        | Primitive::Int16
        | Primitive::Int32
        | Primitive::Int64
        | Primitive::Int128 => Some(ScalarLiteral::Int(0)),
        Primitive::IntBig => IntBig::from_str("0").ok().map(ScalarLiteral::IntBig),
        Primitive::Nat8
        | Primitive::Nat16
        | Primitive::Nat32
        | Primitive::Nat64
        | Primitive::Nat128 => Some(ScalarLiteral::Nat(0)),
        Primitive::NatBig => NatBig::from_str("0").ok().map(ScalarLiteral::NatBig),
        Primitive::Text => Some(ScalarLiteral::Text(String::new())),
        Primitive::Timestamp => Some(ScalarLiteral::Timestamp(Timestamp::EPOCH)),
        Primitive::Ulid => Some(ScalarLiteral::Ulid(Ulid::nil())),
        Primitive::Unit => Some(ScalarLiteral::Unit(Unit)),
        Primitive::Account | Primitive::Principal | Primitive::Subaccount => None,
    }
}

// -----------------------------------------------------------------------------
// Literal conversion helpers
// -----------------------------------------------------------------------------

fn decimal_at_scale(value: Decimal, scale: u32) -> Option<Decimal> {
    match value.scale().cmp(&scale) {
        std::cmp::Ordering::Equal => Some(value),
        std::cmp::Ordering::Less => value
            .scale_to_integer(scale)
            .and_then(|mantissa| Decimal::try_from_i128_with_scale(mantissa, scale)),
        std::cmp::Ordering::Greater => Some(value.round_dp(scale)),
    }
}

fn arg_i128(value: &ArgNumber) -> Option<i128> {
    match value {
        ArgNumber::Int8(value) => Some(i128::from(*value)),
        ArgNumber::Int16(value) => Some(i128::from(*value)),
        ArgNumber::Int32(value) => Some(i128::from(*value)),
        ArgNumber::Int64(value) => Some(i128::from(*value)),
        ArgNumber::Int128(value) => Some(*value),
        ArgNumber::Nat8(value) => Some(i128::from(*value)),
        ArgNumber::Nat16(value) => Some(i128::from(*value)),
        ArgNumber::Nat32(value) => Some(i128::from(*value)),
        ArgNumber::Nat64(value) => Some(i128::from(*value)),
        ArgNumber::Nat128(value) => i128::try_from(*value).ok(),
        ArgNumber::Float32(_) | ArgNumber::Float64(_) => None,
    }
}

fn arg_u128(value: &ArgNumber) -> Option<u128> {
    match value {
        ArgNumber::Int8(value) => u128::try_from(*value).ok(),
        ArgNumber::Int16(value) => u128::try_from(*value).ok(),
        ArgNumber::Int32(value) => u128::try_from(*value).ok(),
        ArgNumber::Int64(value) => u128::try_from(*value).ok(),
        ArgNumber::Int128(value) => u128::try_from(*value).ok(),
        ArgNumber::Nat8(value) => Some(u128::from(*value)),
        ArgNumber::Nat16(value) => Some(u128::from(*value)),
        ArgNumber::Nat32(value) => Some(u128::from(*value)),
        ArgNumber::Nat64(value) => Some(u128::from(*value)),
        ArgNumber::Nat128(value) => Some(*value),
        ArgNumber::Float32(_) | ArgNumber::Float64(_) => None,
    }
}

fn arg_decimal(value: &ArgNumber) -> Option<Decimal> {
    match value {
        ArgNumber::Float32(value) => Decimal::from_f32_lossy(*value),
        ArgNumber::Float64(value) => Decimal::from_f64_lossy(*value),
        _ => arg_i128(value).and_then(Decimal::from_i128),
    }
}

fn parse_subaccount(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut bytes = [0; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk).ok()?;
        bytes[index] = u8::from_str_radix(text, 16).ok()?;
    }
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use icydb_schema::{
        ConstraintFragmentKind, ConstraintSourceKey, FieldSourceKey, FieldType, NamedTypeFragment,
        RuleSourceKey, ScalarType, SourceRuleOperation, TypeSourceKey,
    };

    use super::{Schema, lower_field_rules};
    use crate::{
        node::{
            Arg, ArgNumber, Args, Canister, Def, Entity, Enum, EnumVariant, Field, FieldList, Item,
            ItemTarget, Newtype, PrimaryKey, PrimaryKeySource, Record, SchemaNode, SourceRule,
            SourceRuleKind, Store, StoreHeapConfig, Type, Value,
        },
        types::{Cardinality, Primitive},
    };

    static EMPTY_TYPE: Type = Type::new(&[], &[], &[]);
    static NUMERIC_RULE_ARGS: [Arg; 2] = [
        Arg::Number(ArgNumber::Int32(0)),
        Arg::Number(ArgNumber::Int32(360)),
    ];
    static NUMERIC_RULES: [SourceRule; 1] = [SourceRule::new(
        "range",
        SourceRuleKind::NumericRange,
        Args(&NUMERIC_RULE_ARGS),
    )];
    static NUMERIC_RULE_TYPE: Type = Type::new(&[], &[], &NUMERIC_RULES);
    static LENGTH_RULE_ARGS: [Arg; 2] = [
        Arg::Number(ArgNumber::Int32(2)),
        Arg::Number(ArgNumber::Int32(40)),
    ];
    static LENGTH_RULES: [SourceRule; 1] = [SourceRule::new(
        "length",
        SourceRuleKind::LengthRange,
        Args(&LENGTH_RULE_ARGS),
    )];
    static LENGTH_RULE_TYPE: Type = Type::new(&[], &[], &LENGTH_RULES);
    static NESTED_RULE_FIELDS: [Field; 1] = [Field::new(
        "degrees",
        Value::new(
            Cardinality::One,
            Item::new(
                ItemTarget::Is("test::Degrees"),
                None,
                None,
                None,
                None,
                &[],
                &[],
                false,
            ),
        ),
        None,
        None,
        None,
    )];
    static STATUS_VARIANTS: [EnumVariant; 2] = [
        EnumVariant::new("Active", None),
        EnumVariant::new(
            "Retries",
            Some(Value::new(
                Cardinality::Many,
                Item::new(
                    ItemTarget::Primitive(Primitive::Nat16),
                    None,
                    None,
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            )),
        ),
    ];

    #[test]
    fn durable_rules_nested_below_structural_fields_lower_to_nominal_targets() {
        let mut schema = Schema::new();
        schema.insert_node(SchemaNode::Newtype(Newtype::new(
            Def::new("test", "Degrees"),
            "Degrees",
            Item::new(
                ItemTarget::Primitive(Primitive::Nat16),
                None,
                None,
                None,
                None,
                &[],
                &[],
                false,
            ),
            None,
            NUMERIC_RULE_TYPE.clone(),
        )));
        schema.insert_node(SchemaNode::Record(Record::new(
            Def::new("test", "Nested"),
            "Nested",
            FieldList::new(&NESTED_RULE_FIELDS),
            EMPTY_TYPE.clone(),
        )));

        let outer = Field::new(
            "nested",
            Value::new(
                Cardinality::One,
                Item::new(
                    ItemTarget::Is("test::Nested"),
                    None,
                    None,
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
            None,
            None,
        );
        let constraints =
            lower_field_rules(&schema, &outer).expect("nested durable rule should lower");
        assert_eq!(constraints.len(), 1);
        let ConstraintFragmentKind::TargetedRule(rule) = constraints[0].kind() else {
            panic!("nested durable rule should use the targeted-rule contract")
        };
        assert_eq!(rule.root().as_str(), "nested");
        assert_eq!(rule.target_type().as_str(), "Degrees");
        assert!(matches!(
            rule.operation(),
            SourceRuleOperation::NumericRangeInclusive { .. }
        ));
    }

    static ENTITY_FIELDS: [Field; 5] = [
        Field::new(
            "id",
            Value::new(
                Cardinality::One,
                Item::new(
                    ItemTarget::Primitive(Primitive::Nat64),
                    None,
                    None,
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
            None,
            None,
        ),
        Field::new(
            "tags",
            Value::new(
                Cardinality::Many,
                Item::new(
                    ItemTarget::Primitive(Primitive::Text),
                    None,
                    None,
                    Some(32),
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
            None,
            None,
        ),
        Field::new(
            "status",
            Value::new(
                Cardinality::One,
                Item::new(
                    ItemTarget::Is("test::Status"),
                    None,
                    None,
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            Some(crate::node::Arg::ConstPath("test::Status::Active")),
            None,
            None,
        ),
        Field::new(
            "degrees",
            Value::new(
                Cardinality::One,
                Item::new(
                    ItemTarget::Is("test::Degrees"),
                    None,
                    None,
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
            None,
            None,
        ),
        Field::new(
            "label",
            Value::new(
                Cardinality::One,
                Item::new(
                    ItemTarget::Is("test::Label"),
                    None,
                    None,
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
            None,
            None,
        ),
    ];

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one graph fixture proves the complete field, type, relation, and durable-rule closure"
    )]
    fn sealed_canister_graph_emits_store_free_database_closure() {
        let mut schema = Schema::new();
        schema.insert_node(SchemaNode::Canister(Canister::new(
            Def::new("test", "Canister"),
            "test",
            0,
            10,
            9,
            8,
        )));
        schema.insert_node(SchemaNode::Store(Store::new_heap(
            Def::new("test", "Store"),
            "test::Canister",
            StoreHeapConfig::new(),
        )));
        schema.insert_node(SchemaNode::Enum(Enum::new(
            Def::new("test", "Status"),
            "Status",
            &STATUS_VARIANTS,
            EMPTY_TYPE.clone(),
        )));
        schema.insert_node(SchemaNode::Newtype(Newtype::new(
            Def::new("test", "Degrees"),
            "Degrees",
            Item::new(
                ItemTarget::Primitive(Primitive::Nat16),
                None,
                None,
                None,
                None,
                &[],
                &[],
                false,
            ),
            None,
            NUMERIC_RULE_TYPE.clone(),
        )));
        schema.insert_node(SchemaNode::Newtype(Newtype::new(
            Def::new("test", "Label"),
            "Label",
            Item::new(
                ItemTarget::Primitive(Primitive::Text),
                None,
                None,
                None,
                None,
                &[],
                &[],
                false,
            ),
            None,
            LENGTH_RULE_TYPE.clone(),
        )));
        schema.insert_node(SchemaNode::Entity(Entity::new(
            Def::new("test", "Task"),
            "test::Store",
            1,
            PrimaryKey::new(&["id"], PrimaryKeySource::External),
            &[],
            &[],
            &[],
            FieldList::new(&ENTITY_FIELDS),
            EMPTY_TYPE.clone(),
        )));
        schema.seal().expect("fixture graph should seal");

        let fragment = schema
            .schema_fragment_for_canister("test::Canister")
            .expect("sealed database closure should lower");

        assert_eq!(fragment.entities().len(), 1);
        assert_eq!(fragment.types().len(), 3);
        let fields = fragment.entities()[0].fields();
        assert!(matches!(
            fields
                .iter()
                .find(|field| field.name().as_str() == "tags")
                .map(icydb_schema::FieldFragment::field_type),
            Some(FieldType::List(item))
                if matches!(item.as_ref(), FieldType::Scalar(ScalarType::Text { max_len: Some(32) }))
        ));
        assert!(matches!(
            fields
                .iter()
                .find(|field| field.name().as_str() == "status")
                .map(icydb_schema::FieldFragment::insert_policy),
            Some(icydb_schema::FieldInsertPolicy::Default(
                icydb_schema::ScalarLiteral::EnumUnit { .. }
            ))
        ));
        let NamedTypeFragment::Enum(status) = fragment
            .types()
            .iter()
            .find(|fragment| matches!(fragment, NamedTypeFragment::Enum(_)))
            .expect("reachable status type should remain an enum")
        else {
            panic!("reachable status type should remain an enum")
        };
        assert!(matches!(
            status
                .variants()
                .iter()
                .find(|variant| variant.name().as_str() == "Retries")
                .and_then(|variant| variant.payload()),
            Some(FieldType::List(item))
                if matches!(item.as_ref(), FieldType::Scalar(ScalarType::Nat16))
        ));

        let constraints = fragment.entities()[0].constraints();
        assert_eq!(constraints.len(), 2);
        let degrees_source = ConstraintSourceKey::for_targeted_field_rule(
            &FieldSourceKey::try_new("degrees").expect("field name"),
            &TypeSourceKey::try_new("Degrees").expect("type name"),
            &RuleSourceKey::try_new("range").expect("rule name"),
        );
        let degrees = constraints
            .iter()
            .find(|constraint| constraint.source_key() == &degrees_source)
            .expect("numeric rule should become one field-owned constraint");
        let ConstraintFragmentKind::TargetedRule(degrees) = degrees.kind() else {
            panic!("numeric rule should use the targeted-rule contract")
        };
        assert_eq!(degrees.root().as_str(), "degrees");
        assert_eq!(degrees.target_type().as_str(), "Degrees");
        assert!(matches!(
            degrees.operation(),
            SourceRuleOperation::NumericRangeInclusive { .. }
        ));
        let label = constraints
            .iter()
            .find(|constraint| constraint.source_key() != &degrees_source)
            .expect("length rule should become one field-owned constraint");
        let ConstraintFragmentKind::TargetedRule(label) = label.kind() else {
            panic!("length rule should use the targeted-rule contract")
        };
        assert_eq!(label.target_type().as_str(), "Label");
        assert!(matches!(
            label.operation(),
            SourceRuleOperation::LengthRangeInclusive { min: 2, max: 40 }
        ));
    }
}
