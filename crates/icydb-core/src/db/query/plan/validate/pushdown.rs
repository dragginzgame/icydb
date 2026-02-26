pub(crate) use crate::db::access::{
    PushdownSurfaceEligibility, SecondaryOrderPushdownEligibility, SecondaryOrderPushdownRejection,
    assess_secondary_order_pushdown,
};

#[cfg(test)]
pub(crate) use crate::db::access::{
    PushdownApplicability, assess_secondary_order_pushdown_if_applicable,
    assess_secondary_order_pushdown_if_applicable_validated,
};
