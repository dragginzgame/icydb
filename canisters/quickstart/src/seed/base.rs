use icydb::design::prelude::Ulid;
use icydb_testing_quickstart_fixtures::schema::{ActiveUser, Order, User};

/// Build one deterministic baseline user fixture batch.
#[must_use]
pub fn users() -> Vec<User> {
    vec![
        User {
            name: "alice".to_string(),
            age: 31,
            ..Default::default()
        },
        User {
            name: "bob".to_string(),
            age: 24,
            ..Default::default()
        },
        User {
            name: "charlie".to_string(),
            age: 43,
            ..Default::default()
        },
    ]
}

/// Build one deterministic baseline order fixture batch.
#[must_use]
pub fn orders() -> Vec<Order> {
    vec![
        Order {
            user_id: Ulid::generate(),
            status: "paid".to_string(),
            total_cents: 1_250,
            ..Default::default()
        },
        Order {
            user_id: Ulid::generate(),
            status: "pending".to_string(),
            total_cents: 3_999,
            ..Default::default()
        },
        Order {
            user_id: Ulid::generate(),
            status: "failed".to_string(),
            total_cents: 520,
            ..Default::default()
        },
    ]
}

/// Build one deterministic filtered-index fixture batch.
#[must_use]
pub fn active_users() -> Vec<ActiveUser> {
    vec![
        ActiveUser {
            name: "amber".to_string(),
            active: false,
            ..Default::default()
        },
        ActiveUser {
            name: "bravo".to_string(),
            active: true,
            ..Default::default()
        },
        ActiveUser {
            name: "charlie".to_string(),
            active: true,
            ..Default::default()
        },
        ActiveUser {
            name: "delta".to_string(),
            active: false,
            ..Default::default()
        },
    ]
}
