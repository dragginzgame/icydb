use icydb::design::prelude::Ulid;
use icydb_testing_quickstart_fixtures::schema::{
    Order, User, orders::order_views::OrderCreate, users::user_views::UserCreate,
};

/// Build one deterministic baseline user fixture batch.
#[must_use]
pub fn users() -> Vec<User> {
    vec![
        User::from(UserCreate {
            name: "alice".to_string(),
            age: 31,
        }),
        User::from(UserCreate {
            name: "bob".to_string(),
            age: 24,
        }),
        User::from(UserCreate {
            name: "charlie".to_string(),
            age: 43,
        }),
    ]
}

/// Build one deterministic baseline order fixture batch.
#[must_use]
pub fn orders() -> Vec<Order> {
    vec![
        Order::from(OrderCreate {
            user_id: Ulid::generate(),
            status: "paid".to_string(),
            total_cents: 1_250,
        }),
        Order::from(OrderCreate {
            user_id: Ulid::generate(),
            status: "pending".to_string(),
            total_cents: 3_999,
        }),
        Order::from(OrderCreate {
            user_id: Ulid::generate(),
            status: "failed".to_string(),
            total_cents: 520,
        }),
    ]
}
