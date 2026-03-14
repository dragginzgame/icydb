use crate::schema::{FixtureOrder, FixtureUser};

/// Build one deterministic baseline user fixture batch.
#[must_use]
pub fn base_users() -> Vec<FixtureUser> {
    vec![
        FixtureUser {
            name: "alice".to_string(),
            age: 31,
            ..Default::default()
        },
        FixtureUser {
            name: "bob".to_string(),
            age: 24,
            ..Default::default()
        },
        FixtureUser {
            name: "charlie".to_string(),
            age: 43,
            ..Default::default()
        },
    ]
}

/// Build one deterministic baseline order fixture batch.
#[must_use]
pub fn base_orders() -> Vec<FixtureOrder> {
    vec![
        FixtureOrder {
            status: "paid".to_string(),
            total_cents: 1_250,
            ..Default::default()
        },
        FixtureOrder {
            status: "pending".to_string(),
            total_cents: 3_999,
            ..Default::default()
        },
        FixtureOrder {
            status: "failed".to_string(),
            total_cents: 520,
            ..Default::default()
        },
    ]
}
