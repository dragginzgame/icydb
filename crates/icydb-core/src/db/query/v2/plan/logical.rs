use crate::db::query::v2::predicate::Predicate;

use super::{AccessPath, OrderSpec, PageSpec};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalPlan {
    pub access: AccessPath,
    pub predicate: Option<Predicate>,
    pub order: Option<OrderSpec>,
    pub page: Option<PageSpec>,
}

impl LogicalPlan {
    #[must_use]
    pub const fn new(access: AccessPath) -> Self {
        Self {
            access,
            predicate: None,
            order: None,
            page: None,
        }
    }
}
