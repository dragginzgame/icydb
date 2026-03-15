use crate::schema::{Order, User};

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
            status: "paid".to_string(),
            total_cents: 1_250,
            ..Default::default()
        },
        Order {
            status: "pending".to_string(),
            total_cents: 3_999,
            ..Default::default()
        },
        Order {
            status: "failed".to_string(),
            total_cents: 520,
            ..Default::default()
        },
    ]
}
