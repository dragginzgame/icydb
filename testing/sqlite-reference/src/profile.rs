//! Module: sqlite_reference::profile
//! Responsibility: compact required SQLite SELECT profile and shared fixture facts.
//! Does not own: SQL feature authority or IcyDB execution.
//! Boundary: declares typed scenario intent before rendering entity-specific SQL.

use crate::{
    SqliteAdapterError, SqliteAdapterErrorKind, SqliteReferenceColumnKind, SqliteReferenceRowOrder,
};

use std::collections::BTreeSet;

use icydb_testing_sql_generator::{
    EligibleProvider, EvidenceStrength, GeneratedExpressionDepth, MutationKind, NullabilityClass,
    PredicateFamily, QueryShape, RouteFamily, StatementFamily, TierCCoverageLabels,
    TierCDistributionError, TierCExpectedAcceptance, TierCScenarioDeclaration, ValueTypeFamily,
    WindowBehavior,
};

const REFERENCE_ENTITY_TOKEN: &str = "{entity}";
pub(crate) const SQLITE_REFERENCE_ENTITY: &str = "IcyDbSqliteReferenceUser";

const TEXT_INTEGER_COLUMNS: &[SqliteReferenceColumnKind] = &[
    SqliteReferenceColumnKind::Text,
    SqliteReferenceColumnKind::Integer,
];
const INTEGER_COLUMNS: &[SqliteReferenceColumnKind] = &[SqliteReferenceColumnKind::Integer];
const TWO_INTEGER_COLUMNS: &[SqliteReferenceColumnKind] = &[
    SqliteReferenceColumnKind::Integer,
    SqliteReferenceColumnKind::Integer,
];
const VALUE_CASE_COLUMNS: &[SqliteReferenceColumnKind] = &[
    SqliteReferenceColumnKind::Text,
    SqliteReferenceColumnKind::Integer,
    SqliteReferenceColumnKind::Text,
    SqliteReferenceColumnKind::Text,
];
const TEXT_COLUMNS: &[SqliteReferenceColumnKind] = &[SqliteReferenceColumnKind::Text];

///
/// SqliteReferenceFamily
///
/// High-level SQLite reference family used to prove compact native and live
/// profiles cover every required comparison shape without classifying SQL text.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SqliteReferenceFamily {
    /// Global or grouped aggregate semantics.
    Aggregate,
    /// Scalar expression evaluation semantics.
    Expression,
    /// Grouping, grouped projection, or grouped ordering semantics.
    Grouped,
    /// Predicate evaluation semantics.
    Predicate,
    /// Non-aggregate row selection and projection semantics.
    Scalar,
}

///
/// SqliteReferencePredicateFamily
///
/// Predicate shape declared by a reference scenario before SQL rendering.
/// The live runner maps this closed vocabulary into its shared harness model.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqliteReferencePredicateFamily {
    /// A predicate composed from more than one expression or logical condition.
    Compound,
    /// A comparison whose operands both resolve from row fields.
    FieldComparison,
    /// Membership in an explicitly bounded value set.
    Membership,
    /// No predicate participates in the scenario.
    None,
    /// A lower-and-upper-bound predicate.
    Range,
}

///
/// SqliteReferenceWindow
///
/// Row-order and bound facts declared independently of rendered SQL text.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqliteReferenceWindow {
    /// Deterministically ordered rows without an explicit result bound.
    Ordered,
    /// Deterministically ordered rows with explicit SQL limit and offset facts.
    OrderedLimit {
        /// Maximum number of rows returned by the scenario.
        limit: usize,
        /// Number of ordered rows skipped before collection.
        offset: usize,
    },
    /// Rows compared as a canonicalized multiset.
    Unordered,
}

///
/// SqliteReferenceFixtureRow
///
/// Shared row facts mirrored by the native session fixture, live SQL canister,
/// and bundled SQLite table used in the compact required profile.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SqliteReferenceFixtureRow {
    name: &'static str,
    age: i64,
    rank: i64,
}

impl SqliteReferenceFixtureRow {
    const fn new(name: &'static str, age: i64, rank: i64) -> Self {
        Self { name, age, rank }
    }

    /// Borrow the fixture name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        self.name
    }

    /// Return the fixture age.
    #[must_use]
    pub const fn age(self) -> i64 {
        self.age
    }

    /// Return the fixture rank.
    #[must_use]
    pub const fn rank(self) -> i64 {
        self.rank
    }
}

/// Fixture rows matching the required live SQL canister baseline.
pub const SQLITE_REFERENCE_FIXTURE_ROWS: &[SqliteReferenceFixtureRow] = &[
    SqliteReferenceFixtureRow::new("alice", 31, 28),
    SqliteReferenceFixtureRow::new("bob", 24, 25),
    SqliteReferenceFixtureRow::new("charlie", 43, 43),
];

///
/// SqliteReferenceScenario
///
/// One compact differential scenario with explicit contract features, value
/// mapping, row ordering, and family facts. SQL is rendered only after these
/// facts have been declared.
///

#[derive(Clone, Copy, Debug)]
pub struct SqliteReferenceScenario {
    id: &'static str,
    contract_features: &'static [&'static str],
    families: &'static [SqliteReferenceFamily],
    sql_template: &'static str,
    columns: &'static [SqliteReferenceColumnKind],
    row_order: SqliteReferenceRowOrder,
    predicate: SqliteReferencePredicateFamily,
    window: SqliteReferenceWindow,
    nullable: bool,
}

impl SqliteReferenceScenario {
    /// Return the stable scenario identity.
    #[must_use]
    pub const fn id(self) -> &'static str {
        self.id
    }

    /// Borrow the contract features explicitly covered by this scenario.
    #[must_use]
    pub const fn contract_features(self) -> &'static [&'static str] {
        self.contract_features
    }

    /// Borrow the declared high-level reference families.
    #[must_use]
    pub const fn families(self) -> &'static [SqliteReferenceFamily] {
        self.families
    }

    /// Borrow the exact common result-column kinds.
    #[must_use]
    pub const fn columns(self) -> &'static [SqliteReferenceColumnKind] {
        self.columns
    }

    /// Return the row-order comparison contract.
    #[must_use]
    pub const fn row_order(self) -> SqliteReferenceRowOrder {
        self.row_order
    }

    /// Return the predicate shape declared before SQL rendering.
    #[must_use]
    pub const fn predicate(self) -> SqliteReferencePredicateFamily {
        self.predicate
    }

    /// Return the ordering and row-bound facts declared before SQL rendering.
    #[must_use]
    pub const fn window(self) -> SqliteReferenceWindow {
        self.window
    }

    /// Return whether the scenario can produce null output values.
    #[must_use]
    pub const fn nullable(self) -> bool {
        self.nullable
    }

    /// Project this profile-owned scenario into the shared scheduled coverage contract.
    ///
    /// # Errors
    ///
    /// Returns a typed distribution error if the checked-in profile facts do not
    /// form one complete current Tier C declaration.
    pub fn tier_c_declaration(self) -> Result<TierCScenarioDeclaration, TierCDistributionError> {
        let shape = if self.families.contains(&SqliteReferenceFamily::Grouped) {
            QueryShape::Grouped
        } else if self.families.contains(&SqliteReferenceFamily::Aggregate) {
            QueryShape::GlobalAggregate
        } else {
            QueryShape::Scalar
        };
        let value_types = self
            .columns
            .iter()
            .map(|column| match column {
                SqliteReferenceColumnKind::Blob => ValueTypeFamily::Blob,
                SqliteReferenceColumnKind::Boolean => ValueTypeFamily::Boolean,
                SqliteReferenceColumnKind::Decimal | SqliteReferenceColumnKind::Integer => {
                    ValueTypeFamily::Numeric
                }
                SqliteReferenceColumnKind::Text => ValueTypeFamily::Text,
            })
            .collect::<BTreeSet<_>>();
        let value_type = if value_types.len() == 1 {
            value_types
                .first()
                .copied()
                .unwrap_or(ValueTypeFamily::Mixed)
        } else {
            ValueTypeFamily::Mixed
        };
        let predicate = match self.predicate {
            SqliteReferencePredicateFamily::Compound => PredicateFamily::Compound,
            SqliteReferencePredicateFamily::FieldComparison => PredicateFamily::FieldComparison,
            SqliteReferencePredicateFamily::Membership => PredicateFamily::Membership,
            SqliteReferencePredicateFamily::None => PredicateFamily::None,
            SqliteReferencePredicateFamily::Range => PredicateFamily::Range,
        };
        let window = match self.window {
            SqliteReferenceWindow::Ordered => WindowBehavior::Ordered,
            SqliteReferenceWindow::OrderedLimit { offset: 0, .. } => WindowBehavior::OrderedLimit,
            SqliteReferenceWindow::OrderedLimit { .. } => WindowBehavior::OrderedLimitOffset,
            SqliteReferenceWindow::Unordered => WindowBehavior::None,
        };
        let labels = TierCCoverageLabels::try_new(
            BTreeSet::from([EvidenceStrength::ReferenceOracle]),
            BTreeSet::from([GeneratedExpressionDepth::NotApplicable]),
            BTreeSet::from([MutationKind::None]),
            BTreeSet::from([if self.nullable {
                NullabilityClass::Nullable
            } else {
                NullabilityClass::NonNullable
            }]),
            BTreeSet::from([predicate]),
            BTreeSet::from([EligibleProvider::SqliteReference]),
            BTreeSet::from([RouteFamily::NotContractual]),
            BTreeSet::from([shape]),
            BTreeSet::from([StatementFamily::Select]),
            BTreeSet::from([value_type]),
            BTreeSet::from([window]),
        )?;

        TierCScenarioDeclaration::try_new(
            self.id,
            self.contract_features
                .iter()
                .map(|feature| (*feature).to_string())
                .collect(),
            BTreeSet::from(["sqlite.required_profile".to_string()]),
            TierCExpectedAcceptance::Accepted,
            labels,
        )
    }

    /// Render the scenario for one validated IcyDB or SQLite entity identifier.
    ///
    /// # Errors
    ///
    /// Returns a typed identifier or query error when the requested entity name
    /// is unsafe or the checked-in scenario template is malformed.
    pub fn render_sql(self, entity: &str) -> Result<String, SqliteAdapterError> {
        if !valid_identifier(entity) {
            return Err(SqliteAdapterError::new(
                SqliteAdapterErrorKind::Identifier,
                format!("invalid SQL reference entity identifier {entity:?}"),
            ));
        }
        if self.sql_template.matches(REFERENCE_ENTITY_TOKEN).count() != 1 {
            return Err(SqliteAdapterError::new(
                SqliteAdapterErrorKind::Query,
                format!("scenario {:?} has a malformed entity template", self.id),
            ));
        }

        Ok(self.sql_template.replace(REFERENCE_ENTITY_TOKEN, entity))
    }

    pub(crate) fn render_sqlite_sql(self) -> Result<String, SqliteAdapterError> {
        self.render_sql(SQLITE_REFERENCE_ENTITY)
    }
}

const REQUIRED_SQLITE_REFERENCE_SCENARIOS: &[SqliteReferenceScenario] = &[
    SqliteReferenceScenario {
        id: "sqlite.required.scalar_window",
        contract_features: &[
            "naming.single_binding",
            "pagination.scalar_limit_offset",
            "predicate.boolean_comparison",
            "predicate.range",
            "projection.scalar",
            "select.scalar_rows",
        ],
        families: &[
            SqliteReferenceFamily::Predicate,
            SqliteReferenceFamily::Scalar,
        ],
        sql_template: "SELECT u.name, u.age FROM {entity} AS u WHERE u.age >= 24 AND u.age <= 43 ORDER BY u.age ASC, u.name ASC LIMIT 2 OFFSET 1",
        columns: TEXT_INTEGER_COLUMNS,
        row_order: SqliteReferenceRowOrder::Ordered,
        predicate: SqliteReferencePredicateFamily::Compound,
        window: SqliteReferenceWindow::OrderedLimit {
            limit: 2,
            offset: 1,
        },
        nullable: false,
    },
    SqliteReferenceScenario {
        id: "sqlite.required.scalar_distinct_membership",
        contract_features: &["predicate.membership", "select.scalar_distinct"],
        families: &[
            SqliteReferenceFamily::Predicate,
            SqliteReferenceFamily::Scalar,
        ],
        sql_template: "SELECT DISTINCT age FROM {entity} WHERE age IN (24, 31, 43) ORDER BY age ASC",
        columns: INTEGER_COLUMNS,
        row_order: SqliteReferenceRowOrder::Ordered,
        predicate: SqliteReferencePredicateFamily::Membership,
        window: SqliteReferenceWindow::Ordered,
        nullable: false,
    },
    SqliteReferenceScenario {
        id: "sqlite.required.global_aggregate",
        contract_features: &[
            "having.global_aggregate",
            "projection.aggregate",
            "select.aggregate_distinct_filter",
            "select.global_aggregate",
        ],
        families: &[SqliteReferenceFamily::Aggregate],
        sql_template: "SELECT COUNT(DISTINCT age) AS distinct_ages, COUNT(*) FILTER (WHERE age >= 30) AS senior_count FROM {entity} HAVING COUNT(*) > 0",
        columns: TWO_INTEGER_COLUMNS,
        row_order: SqliteReferenceRowOrder::Unordered,
        predicate: SqliteReferencePredicateFamily::Compound,
        window: SqliteReferenceWindow::Unordered,
        nullable: false,
    },
    SqliteReferenceScenario {
        id: "sqlite.required.grouped_aggregate",
        contract_features: &[
            "having.grouped_aggregate",
            "predicate.field_comparison",
            "predicate.grouped_where_field_comparison",
            "projection.grouped_layout",
            "select.grouped_aggregate",
        ],
        families: &[
            SqliteReferenceFamily::Aggregate,
            SqliteReferenceFamily::Grouped,
            SqliteReferenceFamily::Predicate,
        ],
        sql_template: "SELECT age, COUNT(*) AS row_count FROM {entity} WHERE age = age GROUP BY age HAVING COUNT(*) >= 1 ORDER BY age ASC LIMIT 10",
        columns: TWO_INTEGER_COLUMNS,
        row_order: SqliteReferenceRowOrder::Ordered,
        predicate: SqliteReferencePredicateFamily::FieldComparison,
        window: SqliteReferenceWindow::OrderedLimit {
            limit: 10,
            offset: 0,
        },
        nullable: false,
    },
    SqliteReferenceScenario {
        id: "sqlite.required.grouped_rank",
        contract_features: &["projection.grouped_layout", "select.grouped_aggregate"],
        families: &[SqliteReferenceFamily::Grouped],
        sql_template: "SELECT rank, COUNT(*) AS row_count FROM {entity} GROUP BY rank ORDER BY rank ASC LIMIT 10",
        columns: TWO_INTEGER_COLUMNS,
        row_order: SqliteReferenceRowOrder::Ordered,
        predicate: SqliteReferencePredicateFamily::None,
        window: SqliteReferenceWindow::OrderedLimit {
            limit: 10,
            offset: 0,
        },
        nullable: false,
    },
    SqliteReferenceScenario {
        id: "sqlite.required.numeric_order_alias",
        contract_features: &[
            "expression.numeric_functions",
            "ordering.projection_alias",
            "projection.aliases",
        ],
        families: &[
            SqliteReferenceFamily::Expression,
            SqliteReferenceFamily::Scalar,
        ],
        sql_template: "SELECT name AS label, age AS source_age FROM {entity} ORDER BY ABS(age) ASC, label ASC",
        columns: TEXT_INTEGER_COLUMNS,
        row_order: SqliteReferenceRowOrder::Ordered,
        predicate: SqliteReferencePredicateFamily::None,
        window: SqliteReferenceWindow::Ordered,
        nullable: false,
    },
    SqliteReferenceScenario {
        id: "sqlite.required.value_case_null_order",
        contract_features: &[
            "expression.searched_case",
            "expression.value_selection",
            "ordering.null_values",
            "predicate.null",
            "select.computed_projection",
        ],
        families: &[
            SqliteReferenceFamily::Expression,
            SqliteReferenceFamily::Predicate,
        ],
        sql_template: "SELECT name, NULLIF(age, 31) AS maybe_age, CASE WHEN age >= 30 THEN 'senior' ELSE 'junior' END AS cohort, COALESCE(NULLIF(name, name), 'missing') AS fallback FROM {entity} WHERE NULLIF(name, '') IS NOT NULL ORDER BY maybe_age ASC, name ASC",
        columns: VALUE_CASE_COLUMNS,
        row_order: SqliteReferenceRowOrder::Ordered,
        predicate: SqliteReferencePredicateFamily::Compound,
        window: SqliteReferenceWindow::Ordered,
        nullable: true,
    },
    SqliteReferenceScenario {
        id: "sqlite.required.field_bound_range",
        contract_features: &["predicate.field_bound_range"],
        families: &[SqliteReferenceFamily::Predicate],
        sql_template: "SELECT name FROM {entity} WHERE age BETWEEN rank AND age ORDER BY name ASC",
        columns: TEXT_COLUMNS,
        row_order: SqliteReferenceRowOrder::Ordered,
        predicate: SqliteReferencePredicateFamily::Range,
        window: SqliteReferenceWindow::Ordered,
        nullable: false,
    },
];

/// Borrow the compact required native and live SQLite SELECT profile.
#[must_use]
pub const fn required_sqlite_reference_scenarios() -> &'static [SqliteReferenceScenario] {
    REQUIRED_SQLITE_REFERENCE_SCENARIOS
}

fn valid_identifier(identifier: &str) -> bool {
    let mut bytes = identifier.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}
