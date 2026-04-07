use crate::schema::{Customer, CustomerAccount, CustomerOrder, CustomerOrderProfile};

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
            labels: vec!["new".to_string(), "priority-low".to_string()],
            profile: CustomerOrderProfile {
                summary: "starter".to_string(),
                bucket: 1,
            },
            ..Default::default()
        },
        CustomerOrder {
            name: "A-101".to_string(),
            priority: 20,
            status: "Backlog".to_string(),
            labels: vec!["priority-high".to_string(), "billing".to_string()],
            profile: CustomerOrderProfile {
                summary: "billing prep".to_string(),
                bucket: 2,
            },
            ..Default::default()
        },
        CustomerOrder {
            name: "A-102".to_string(),
            priority: 20,
            status: "Billing".to_string(),
            labels: vec!["priority-high".to_string(), "billing".to_string()],
            profile: CustomerOrderProfile {
                summary: "billing".to_string(),
                bucket: 2,
            },
            ..Default::default()
        },
        CustomerOrder {
            name: "B-200".to_string(),
            priority: 20,
            status: "Closed".to_string(),
            labels: vec!["priority-high".to_string(), "closed".to_string()],
            profile: CustomerOrderProfile {
                summary: "closed".to_string(),
                bucket: 3,
            },
            ..Default::default()
        },
        CustomerOrder {
            name: "C-300".to_string(),
            priority: 20,
            status: "Draft".to_string(),
            labels: vec!["priority-high".to_string(), "draft".to_string()],
            profile: CustomerOrderProfile {
                summary: "draft".to_string(),
                bucket: 4,
            },
            ..Default::default()
        },
        CustomerOrder {
            name: "Z-900".to_string(),
            priority: 30,
            status: "Closed".to_string(),
            labels: vec!["priority-low".to_string(), "closed".to_string()],
            profile: CustomerOrderProfile {
                summary: "archived".to_string(),
                bucket: 9,
            },
            ..Default::default()
        },
    ]
}
