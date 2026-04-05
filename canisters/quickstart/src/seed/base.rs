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
            tier: "gold".to_string(),
            handle: "bramble".to_string(),
            ..Default::default()
        },
        ActiveUser {
            name: "bravo".to_string(),
            active: true,
            tier: "gold".to_string(),
            handle: "bravo".to_string(),
            ..Default::default()
        },
        ActiveUser {
            name: "charlie".to_string(),
            active: true,
            tier: "gold".to_string(),
            handle: "bristle".to_string(),
            ..Default::default()
        },
        ActiveUser {
            name: "delta".to_string(),
            active: false,
            tier: "silver".to_string(),
            handle: "brisk".to_string(),
            ..Default::default()
        },
        ActiveUser {
            name: "echo".to_string(),
            active: true,
            tier: "silver".to_string(),
            handle: "Brisk".to_string(),
            ..Default::default()
        },
    ]
}
