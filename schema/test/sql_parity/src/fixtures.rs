use crate::schema::{
    Customer, CustomerAccount, CustomerOrder, CustomerOrderProfile, PlannerChoice,
    PlannerPrefixChoice, PlannerUniquePrefixChoice, SqlWriteProbe,
};

const PERF_CUSTOMER_NAMES: &[&str] = &[
    "alice", "bob", "charlie", "diana", "eve", "frank", "grace", "heidi", "ivan", "judy",
    "mallory", "niaj", "olivia", "peggy", "trent", "victor",
];
const PERF_CUSTOMER_AGES: &[i32] = &[21, 24, 27, 30, 33, 36, 39, 42, 45, 48, 51, 54];
const PERF_ACCOUNT_TIERS: &[&str] = &["bronze", "silver", "gold", "platinum"];
const PERF_ACCOUNT_HANDLES: &[&str] = &[
    "amber", "bramble", "bravo", "brisk", "bristle", "cinder", "delta", "ember",
];
const PERF_ORDER_STATUSES: &[&str] = &[
    "Alpha",
    "Backlog",
    "Billing",
    "Closed",
    "Draft",
    "Escalated",
];
const PERF_ORDER_PRIORITIES: &[u16] = &[10, 20, 20, 30, 40, 50];

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

/// Build one larger deterministic customer fixture batch for perf audits.
#[must_use]
pub fn perf_audit_customers() -> Vec<Customer> {
    let cohort_count = 4usize;
    let mut rows = Vec::with_capacity(PERF_CUSTOMER_NAMES.len() * cohort_count);

    for cohort in 0..cohort_count {
        for (name_index, name) in PERF_CUSTOMER_NAMES.iter().enumerate() {
            let age = PERF_CUSTOMER_AGES[(cohort + name_index) % PERF_CUSTOMER_AGES.len()];
            rows.push(Customer {
                name: (*name).to_string(),
                age,
                ..Default::default()
            });
        }
    }

    rows
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

/// Build one larger deterministic customer-account fixture batch for perf audits.
#[must_use]
pub fn perf_audit_customer_accounts() -> Vec<CustomerAccount> {
    let row_count = 24usize;
    let mut rows = Vec::with_capacity(row_count);

    for offset in 0..row_count {
        let tier = PERF_ACCOUNT_TIERS[(offset / 8) % PERF_ACCOUNT_TIERS.len()];
        let handle_seed = PERF_ACCOUNT_HANDLES[offset % PERF_ACCOUNT_HANDLES.len()];
        let handle = if offset % 5 == 0 {
            handle_seed.to_ascii_uppercase()
        } else {
            format!("{handle_seed}-{offset:03}")
        };
        rows.push(CustomerAccount {
            name: format!("acct-{offset:03}"),
            active: offset % 4 != 0,
            tier: tier.to_string(),
            handle,
            ..Default::default()
        });
    }

    rows
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

/// Build one larger deterministic customer-order fixture batch for perf audits.
#[must_use]
pub fn perf_audit_customer_orders() -> Vec<CustomerOrder> {
    let row_count = 36usize;
    let mut rows = Vec::with_capacity(row_count);

    for offset in 0..row_count {
        let status = PERF_ORDER_STATUSES[offset % PERF_ORDER_STATUSES.len()];
        let priority = PERF_ORDER_PRIORITIES[offset % PERF_ORDER_PRIORITIES.len()];
        rows.push(CustomerOrder {
            name: format!("P-{offset:04}"),
            priority,
            status: status.to_string(),
            labels: vec![
                format!("priority-{priority}"),
                status.to_ascii_lowercase(),
                format!("bucket-{}", offset % 12),
            ],
            profile: CustomerOrderProfile {
                summary: format!("order-{offset:04}"),
                bucket: u16::try_from(offset % 12).expect("perf order bucket must fit in u16"),
            },
            ..Default::default()
        });
    }

    rows
}

/// Build one deterministic baseline SQL write-probe fixture batch.
#[must_use]
pub fn sql_write_probes() -> Vec<SqlWriteProbe> {
    vec![SqlWriteProbe {
        id: 1,
        name: "seed".to_string(),
        age: 21,
        ..Default::default()
    }]
}

/// Build one larger deterministic SQL write-probe fixture batch for perf audits.
#[must_use]
pub fn perf_audit_sql_write_probes() -> Vec<SqlWriteProbe> {
    vec![SqlWriteProbe {
        id: 1,
        name: "perf-seed".to_string(),
        age: 21,
        ..Default::default()
    }]
}

/// Build one deterministic planner-choice fixture batch.
#[must_use]
pub fn planner_choices() -> Vec<PlannerChoice> {
    vec![
        PlannerChoice {
            tier: "gold".to_string(),
            score: 5,
            handle: "zephyr".to_string(),
            label: "amber".to_string(),
            alpha: "delta".to_string(),
            beta: "alpha".to_string(),
            ..Default::default()
        },
        PlannerChoice {
            tier: "gold".to_string(),
            score: 20,
            handle: "bravo".to_string(),
            label: "cello".to_string(),
            alpha: "alpha".to_string(),
            beta: "echo".to_string(),
            ..Default::default()
        },
        PlannerChoice {
            tier: "gold".to_string(),
            score: 30,
            handle: "charlie".to_string(),
            label: "bravo".to_string(),
            alpha: "bravo".to_string(),
            beta: "delta".to_string(),
            ..Default::default()
        },
        PlannerChoice {
            tier: "gold".to_string(),
            score: 30,
            handle: "echo".to_string(),
            label: "bravo".to_string(),
            alpha: "foxtrot".to_string(),
            beta: "golf".to_string(),
            ..Default::default()
        },
        PlannerChoice {
            tier: "gold".to_string(),
            score: 30,
            handle: "lima".to_string(),
            label: "bravo".to_string(),
            alpha: "hotel".to_string(),
            beta: "india".to_string(),
            ..Default::default()
        },
        PlannerChoice {
            tier: "silver".to_string(),
            score: 40,
            handle: "delta".to_string(),
            label: "delta".to_string(),
            alpha: "charlie".to_string(),
            beta: "charlie".to_string(),
            ..Default::default()
        },
    ]
}

/// Build one deterministic equality-prefix planner-choice fixture batch.
#[must_use]
pub fn planner_prefix_choices() -> Vec<PlannerPrefixChoice> {
    vec![
        PlannerPrefixChoice {
            tier: "gold".to_string(),
            handle: "bravo".to_string(),
            label: "amber".to_string(),
            ..Default::default()
        },
        PlannerPrefixChoice {
            tier: "gold".to_string(),
            handle: "charlie".to_string(),
            label: "bravo".to_string(),
            ..Default::default()
        },
        PlannerPrefixChoice {
            tier: "silver".to_string(),
            handle: "delta".to_string(),
            label: "delta".to_string(),
            ..Default::default()
        },
    ]
}

/// Build one deterministic unique-prefix planner-choice fixture batch.
#[must_use]
pub fn planner_unique_prefix_choices() -> Vec<PlannerUniquePrefixChoice> {
    vec![
        PlannerUniquePrefixChoice {
            tier: "gold".to_string(),
            handle: "amber".to_string(),
            note: "A".to_string(),
            ..Default::default()
        },
        PlannerUniquePrefixChoice {
            tier: "gold".to_string(),
            handle: "bravo".to_string(),
            note: "B".to_string(),
            ..Default::default()
        },
        PlannerUniquePrefixChoice {
            tier: "gold".to_string(),
            handle: "charlie".to_string(),
            note: "C".to_string(),
            ..Default::default()
        },
        PlannerUniquePrefixChoice {
            tier: "gold".to_string(),
            handle: "delta".to_string(),
            note: "D".to_string(),
            ..Default::default()
        },
        PlannerUniquePrefixChoice {
            tier: "silver".to_string(),
            handle: "echo".to_string(),
            note: "E".to_string(),
            ..Default::default()
        },
    ]
}
