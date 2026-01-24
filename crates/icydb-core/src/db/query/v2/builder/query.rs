use crate::{
    db::query::v2::{
        plan::{
            AccessPlan, LogicalPlan, OrderDirection, OrderSpec, PageSpec, plan_access,
            validate_plan, validate_plan_invariants,
        },
        predicate::{Predicate, SchemaInfo, validate_model},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel},
        index::IndexModel,
    },
    traits::EntityKind,
};
use icydb_schema::{
    node::{Entity, Enum, Item, ItemTarget, List, Map, Newtype, Record, Schema, Set, Tuple},
    types::{Cardinality, Primitive},
};
use std::marker::PhantomData;

///
/// QueryBuilder
///
/// Typed, declarative query builder for v2 queries.
///
/// This builder:
/// - Collects predicate, ordering, and pagination into a `QuerySpec`
/// - Is purely declarative (no schema access, planning, or execution)
/// - Is parameterized by the entity type `E`
///
/// Important design notes:
/// - No validation occurs here beyond structural composition
/// - Field names are accepted as strings; validity is checked later
/// - Schema is consulted only during `QuerySpec::plan`
///
/// This separation allows query construction to remain lightweight,
/// testable, and independent of runtime context.
///

pub struct QueryBuilder<E: EntityKind> {
    predicate: Option<Predicate>,
    order: Option<OrderSpec>,
    page: Option<PageSpec>,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> Default for QueryBuilder<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: EntityKind> QueryBuilder<E> {
    /// Create a new empty query builder.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            predicate: None,
            order: None,
            page: None,
            _marker: PhantomData,
        }
    }

    /// Add a predicate, implicitly AND-ing with any existing predicate.
    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.predicate = match self.predicate.take() {
            Some(existing) => Some(Predicate::And(vec![existing, predicate])),
            None => Some(predicate),
        };
        self
    }

    /// Explicit AND combinator for predicates.
    #[must_use]
    pub fn and(mut self, predicate: Predicate) -> Self {
        self.predicate = match self.predicate.take() {
            Some(existing) => Some(Predicate::And(vec![existing, predicate])),
            None => Some(predicate),
        };
        self
    }

    /// Explicit OR combinator for predicates.
    #[must_use]
    pub fn or(mut self, predicate: Predicate) -> Self {
        self.predicate = match self.predicate.take() {
            Some(existing) => Some(Predicate::Or(vec![existing, predicate])),
            None => Some(predicate),
        };
        self
    }

    /// Append an ascending sort key.
    #[must_use]
    pub fn order_by(mut self, field: &'static str) -> Self {
        self.order = Some(push_order(self.order, field, OrderDirection::Asc));
        self
    }

    /// Append a descending sort key.
    #[must_use]
    pub fn order_by_desc(mut self, field: &'static str) -> Self {
        self.order = Some(push_order(self.order, field, OrderDirection::Desc));
        self
    }

    /// Set or replace the result limit.
    #[must_use]
    pub const fn limit(mut self, n: u32) -> Self {
        self.page = Some(match self.page {
            Some(mut page) => {
                page.limit = Some(n);
                page
            }
            None => PageSpec {
                limit: Some(n),
                offset: 0,
            },
        });
        self
    }

    /// Set or replace the result offset.
    #[must_use]
    pub const fn offset(mut self, n: u32) -> Self {
        self.page = Some(match self.page {
            Some(mut page) => {
                page.offset = n;
                page
            }
            None => PageSpec {
                limit: None,
                offset: n,
            },
        });
        self
    }

    /// Finalize the builder into an immutable `QuerySpec`.
    #[must_use]
    pub fn build(self) -> QuerySpec {
        QuerySpec {
            predicate: self.predicate,
            order: self.order,
            page: self.page,
        }
    }
}

///
/// QuerySpec
///
/// Immutable specification produced by `QueryBuilder`.
///
/// `QuerySpec` represents a fully constructed query intent, but
/// not yet a concrete execution plan. It is the handoff point
/// between the builder layer and the planner/executor pipeline.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerySpec {
    pub predicate: Option<Predicate>,
    pub order: Option<OrderSpec>,
    pub page: Option<PageSpec>,
}

impl QuerySpec {
    /// Convert this query specification into a validated `LogicalPlan`.
    ///
    /// This is the boundary where:
    /// - Schema is consulted (via macro-generated metadata)
    /// - Predicate validation and coercion checks occur
    /// - Index eligibility is determined by the planner
    ///
    /// Composite access plans (union/intersection) are explicitly rejected
    /// here, as executors currently require a single concrete access path.
    pub fn plan<E: EntityKind>(&self, schema: &Schema) -> Result<LogicalPlan, InternalError> {
        let entity = schema.cast_node::<Entity>(E::PATH).map_err(|err| {
            InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Query,
                format!("schema unavailable: {err}"),
            )
        })?;
        let model = entity_model_from_schema(entity, schema)?;
        if let Some(predicate) = &self.predicate {
            validate_model(&model, predicate).map_err(|err| {
                InternalError::new(ErrorClass::Unsupported, ErrorOrigin::Query, err.to_string())
            })?;
        }

        let schema_info = SchemaInfo::from_entity_model(&model);

        let access_plan = plan_access::<E>(self.predicate.as_ref()).map_err(|err| {
            InternalError::new(ErrorClass::Unsupported, ErrorOrigin::Query, err.to_string())
        })?;
        validate_plan_invariants::<E>(&access_plan, &schema_info, self.predicate.as_ref());

        let access = match access_plan {
            AccessPlan::Path(path) => path,
            AccessPlan::Union(_) | AccessPlan::Intersection(_) => {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Query,
                    "planner produced composite access plan; executor requires a single access path"
                        .to_string(),
                ));
            }
        };

        let plan = LogicalPlan {
            access,
            predicate: self.predicate.clone(),
            order: self.order.clone(),
            page: self.page.clone(),
        };
        validate_plan::<E>(&plan).map_err(|err| {
            InternalError::new(ErrorClass::Unsupported, ErrorOrigin::Query, err.to_string())
        })?;

        Ok(plan)
    }
}

/// Helper to append an ordering field while preserving existing order spec.
fn push_order(
    order: Option<OrderSpec>,
    field: &'static str,
    direction: OrderDirection,
) -> OrderSpec {
    match order {
        Some(mut spec) => {
            spec.fields.push((field.to_string(), direction));
            spec
        }
        None => OrderSpec {
            fields: vec![(field.to_string(), direction)],
        },
    }
}

fn entity_model_from_schema(
    entity: &Entity,
    schema: &Schema,
) -> Result<EntityModel, InternalError> {
    let mut fields = Vec::with_capacity(entity.fields.fields.len());
    for field in entity.fields.fields {
        let kind = model_kind_from_value(&field.value, schema);
        fields.push(EntityFieldModel {
            name: field.ident,
            kind,
        });
    }

    let pk_index = fields
        .iter()
        .position(|field| field.name == entity.primary_key)
        .ok_or_else(|| {
            InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Query,
                format!("primary key field '{}' not found", entity.primary_key),
            )
        })?;

    let fields: &'static [EntityFieldModel] = Box::leak(fields.into_boxed_slice());
    let primary_key = &fields[pk_index];

    let mut index_models = Vec::with_capacity(entity.indexes.len());
    for index in entity.indexes {
        index_models.push(IndexModel::new(index.store, index.fields, index.unique));
    }
    let index_models: &'static [IndexModel] = Box::leak(index_models.into_boxed_slice());
    let index_refs: Vec<&'static IndexModel> = index_models.iter().collect();
    let indexes: &'static [&'static IndexModel] = Box::leak(index_refs.into_boxed_slice());

    let path: &'static str = Box::leak(entity.def.path().into_boxed_str());

    Ok(EntityModel {
        path,
        entity_name: entity.resolved_name(),
        primary_key,
        fields,
        indexes,
    })
}

fn model_kind_from_value(value: &icydb_schema::node::Value, schema: &Schema) -> EntityFieldKind {
    let base = model_kind_from_item(&value.item, schema);
    match value.cardinality {
        Cardinality::Many => EntityFieldKind::List(Box::new(base)),
        Cardinality::One | Cardinality::Opt => base,
    }
}

fn model_kind_from_item(item: &Item, schema: &Schema) -> EntityFieldKind {
    match &item.target {
        ItemTarget::Primitive(prim) => model_kind_from_primitive(*prim),
        ItemTarget::Is(path) => {
            if schema.cast_node::<Enum>(path).is_ok() {
                return EntityFieldKind::Enum;
            }
            if let Ok(node) = schema.cast_node::<Newtype>(path) {
                return model_kind_from_item(&node.item, schema);
            }
            if let Ok(node) = schema.cast_node::<List>(path) {
                return EntityFieldKind::List(Box::new(model_kind_from_item(&node.item, schema)));
            }
            if let Ok(node) = schema.cast_node::<Set>(path) {
                return EntityFieldKind::Set(Box::new(model_kind_from_item(&node.item, schema)));
            }
            if let Ok(node) = schema.cast_node::<Map>(path) {
                let key = model_kind_from_item(&node.key, schema);
                let value = model_kind_from_value(&node.value, schema);
                return EntityFieldKind::Map {
                    key: Box::new(key),
                    value: Box::new(value),
                };
            }
            if schema.cast_node::<Record>(path).is_ok() {
                return EntityFieldKind::Unsupported;
            }
            if schema.cast_node::<Tuple>(path).is_ok() {
                return EntityFieldKind::Unsupported;
            }

            EntityFieldKind::Unsupported
        }
    }
}

const fn model_kind_from_primitive(prim: Primitive) -> EntityFieldKind {
    match prim {
        Primitive::Account => EntityFieldKind::Account,
        Primitive::Blob => EntityFieldKind::Blob,
        Primitive::Bool => EntityFieldKind::Bool,
        Primitive::Date => EntityFieldKind::Date,
        Primitive::Decimal => EntityFieldKind::Decimal,
        Primitive::Duration => EntityFieldKind::Duration,
        Primitive::E8s => EntityFieldKind::E8s,
        Primitive::E18s => EntityFieldKind::E18s,
        Primitive::Float32 => EntityFieldKind::Float32,
        Primitive::Float64 => EntityFieldKind::Float64,
        Primitive::Int => EntityFieldKind::IntBig,
        Primitive::Int8 | Primitive::Int16 | Primitive::Int32 | Primitive::Int64 => {
            EntityFieldKind::Int
        }
        Primitive::Int128 => EntityFieldKind::Int128,
        Primitive::Nat => EntityFieldKind::UintBig,
        Primitive::Nat8 | Primitive::Nat16 | Primitive::Nat32 | Primitive::Nat64 => {
            EntityFieldKind::Uint
        }
        Primitive::Nat128 => EntityFieldKind::Uint128,
        Primitive::Principal => EntityFieldKind::Principal,
        Primitive::Subaccount => EntityFieldKind::Subaccount,
        Primitive::Text => EntityFieldKind::Text,
        Primitive::Timestamp => EntityFieldKind::Timestamp,
        Primitive::Ulid => EntityFieldKind::Ulid,
        Primitive::Unit => EntityFieldKind::Unit,
    }
}
