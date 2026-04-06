use crate::schema::{Customer, CustomerAccount, CustomerOrder};

/// Build one deterministic baseline customer fixture batch.
#[must_use]
pub fn customers() -> Vec<Customer> {
    vec![
        Customer {
            name: "alice".to_string(),
            age: 31,
            ..Default::default()
        },
        Customer {
            name: "bob".to_string(),
            age: 24,
            ..Default::default()
        },
        Customer {
            name: "charlie".to_string(),
            age: 43,
            ..Default::default()
        },
    ]
}

/// Build one deterministic baseline customer-account fixture batch.
#[must_use]
pub fn customer_accounts() -> Vec<CustomerAccount> {
    vec![
        CustomerAccount {
            name: "amber".to_string(),
            active: false,
            tier: "gold".to_string(),
            handle: "bramble".to_string(),
            ..Default::default()
        },
        CustomerAccount {
            name: "bravo".to_string(),
            active: true,
            tier: "gold".to_string(),
            handle: "bravo".to_string(),
            ..Default::default()
        },
        CustomerAccount {
            name: "charlie".to_string(),
            active: true,
            tier: "gold".to_string(),
            handle: "bristle".to_string(),
            ..Default::default()
        },
        CustomerAccount {
            name: "delta".to_string(),
            active: false,
            tier: "silver".to_string(),
            handle: "brisk".to_string(),
            ..Default::default()
        },
        CustomerAccount {
            name: "echo".to_string(),
            active: true,
            tier: "silver".to_string(),
            handle: "Brisk".to_string(),
            ..Default::default()
        },
    ]
}

/// Build one deterministic baseline customer-order fixture batch.
#[must_use]
pub fn customer_orders() -> Vec<CustomerOrder> {
    vec![
        CustomerOrder {
            name: "A-100".to_string(),
            priority: 10,
            status: "Alpha".to_string(),
            ..Default::default()
        },
        CustomerOrder {
            name: "A-101".to_string(),
            priority: 20,
            status: "Backlog".to_string(),
            ..Default::default()
        },
        CustomerOrder {
            name: "A-102".to_string(),
            priority: 20,
            status: "Billing".to_string(),
            ..Default::default()
        },
        CustomerOrder {
            name: "B-200".to_string(),
            priority: 20,
            status: "Closed".to_string(),
            ..Default::default()
        },
        CustomerOrder {
            name: "C-300".to_string(),
            priority: 20,
            status: "Draft".to_string(),
            ..Default::default()
        },
        CustomerOrder {
            name: "Z-900".to_string(),
            priority: 30,
            status: "Closed".to_string(),
            ..Default::default()
        },
    ]
}
