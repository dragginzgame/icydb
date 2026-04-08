pub mod fixtures;
pub mod schema;

pub use schema::{
    Customer, CustomerAccount, CustomerOrder, CustomerOrderProfile, PlannerChoice,
    PlannerPrefixChoice, SqlParityCanister, SqlParityStore,
};
