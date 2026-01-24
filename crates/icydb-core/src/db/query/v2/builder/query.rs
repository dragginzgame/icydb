use crate::{
    db::query::v2::{
        plan::{OrderDirection, OrderSpec, PageSpec},
        predicate::Predicate,
    },
    traits::EntityKind,
};
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
