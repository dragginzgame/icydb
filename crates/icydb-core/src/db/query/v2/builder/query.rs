use crate::{
    db::query::v2::{
        plan::{
            AccessPlan, LogicalPlan, OrderDirection, OrderSpec, PageSpec, plan_access,
            validate_plan,
        },
        predicate::{Predicate, SchemaInfo},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::EntityKind,
};
use icydb_schema::node::{Entity, Schema};
use std::marker::PhantomData;

pub struct QueryBuilder<E: EntityKind> {
    predicate: Option<Predicate>,
    order: Option<OrderSpec>,
    page: Option<PageSpec>,
    _marker: PhantomData<E>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerySpec {
    pub predicate: Option<Predicate>,
    pub order: Option<OrderSpec>,
    pub page: Option<PageSpec>,
}

impl<E: EntityKind> Default for QueryBuilder<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: EntityKind> QueryBuilder<E> {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            predicate: None,
            order: None,
            page: None,
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.predicate = match self.predicate.take() {
            Some(existing) => Some(Predicate::And(vec![existing, predicate])),
            None => Some(predicate),
        };
        self
    }

    #[must_use]
    pub fn and(mut self, predicate: Predicate) -> Self {
        self.predicate = match self.predicate.take() {
            Some(existing) => Some(Predicate::And(vec![existing, predicate])),
            None => Some(predicate),
        };
        self
    }

    #[must_use]
    pub fn or(mut self, predicate: Predicate) -> Self {
        self.predicate = match self.predicate.take() {
            Some(existing) => Some(Predicate::Or(vec![existing, predicate])),
            None => Some(predicate),
        };
        self
    }

    #[must_use]
    pub fn order_by(mut self, field: &'static str) -> Self {
        self.order = Some(push_order(self.order, field, OrderDirection::Asc));
        self
    }

    #[must_use]
    pub fn order_by_desc(mut self, field: &'static str) -> Self {
        self.order = Some(push_order(self.order, field, OrderDirection::Desc));
        self
    }

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

    #[must_use]
    pub fn build(self) -> QuerySpec {
        QuerySpec {
            predicate: self.predicate,
            order: self.order,
            page: self.page,
        }
    }
}

impl QuerySpec {
    pub fn plan<E: EntityKind>(&self, schema: &Schema) -> Result<LogicalPlan, InternalError> {
        let entity = schema.cast_node::<Entity>(E::PATH).map_err(|err| {
            InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Query,
                format!("schema unavailable: {err}"),
            )
        })?;
        let schema_info = SchemaInfo::from_entity_schema(entity, schema);

        let access_plan = plan_access::<E>(self.predicate.as_ref()).map_err(|err| {
            InternalError::new(ErrorClass::Unsupported, ErrorOrigin::Query, err.to_string())
        })?;
        access_plan.validate_invariants::<E>(&schema_info, self.predicate.as_ref());

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
