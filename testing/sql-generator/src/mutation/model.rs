//! Module: sql_generator::mutation::model
//! Responsibility: accepted-snapshot DML types, SQL rendering, and independent atomic row-state transitions.
//! Does not own: product parsing, planning, storage, or reference-engine execution.
//! Boundary: validates one current mutation contract and derives expected state without product helpers.

use crate::{SqlGeneratorError, SqlGeneratorErrorKind};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fmt::Write as _};

/// Required native mutation-state budgets for the 0.204 Tier A lane.
pub const TIER_A_MUTATION_BUDGETS: MutationBudgets = MutationBudgets::new(16, 8, 256, 512, 262_144);

/// Required scheduled and closeout mutation-state budgets for the 0.204 Tier C lane.
pub const TIER_C_MUTATION_BUDGETS: MutationBudgets =
    MutationBudgets::new(64, 32, 4_096, 8_192, 1_048_576);

///
/// MutationBudgets
///
/// Deterministic bounds for one generated mutation sequence and its failure artifact.
/// Owned by the generator and enforced again by replay and shrinking.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[expect(
    clippy::struct_field_names,
    reason = "each field is a distinct hard ceiling and the max prefix is contractual"
)]
pub struct MutationBudgets {
    max_fixture_rows: u32,
    max_statements: u32,
    max_shrink_candidates: u32,
    max_evaluations: u32,
    max_artifact_bytes: u32,
}

impl MutationBudgets {
    /// Build explicit deterministic mutation budgets.
    #[must_use]
    pub const fn new(
        max_fixture_rows: u32,
        max_statements: u32,
        max_shrink_candidates: u32,
        max_evaluations: u32,
        max_artifact_bytes: u32,
    ) -> Self {
        Self {
            max_fixture_rows,
            max_statements,
            max_shrink_candidates,
            max_evaluations,
            max_artifact_bytes,
        }
    }

    /// Return the initial fixture-row bound.
    #[must_use]
    pub const fn max_fixture_rows(self) -> u32 {
        self.max_fixture_rows
    }

    /// Return the statement-count bound.
    #[must_use]
    pub const fn max_statements(self) -> u32 {
        self.max_statements
    }

    /// Return the shrink-candidate bound.
    #[must_use]
    pub const fn max_shrink_candidates(self) -> u32 {
        self.max_shrink_candidates
    }

    /// Return the complete-evaluation bound.
    #[must_use]
    pub const fn max_evaluations(self) -> u32 {
        self.max_evaluations
    }

    /// Return the canonical replay byte bound.
    #[must_use]
    pub const fn max_artifact_bytes(self) -> u32 {
        self.max_artifact_bytes
    }

    fn validate(self) -> Result<(), SqlGeneratorError> {
        if self.max_fixture_rows == 0
            || self.max_statements == 0
            || self.max_shrink_candidates == 0
            || self.max_evaluations == 0
            || self.max_artifact_bytes == 0
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "mutation budgets must all be non-zero",
            ));
        }

        Ok(())
    }
}

///
/// MutationFieldKind
///
/// Value kind admitted by the first accepted-snapshot mutation schema family.
/// This is a generator fact, not a product storage or runtime value type.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationFieldKind {
    /// UTF-8 text used by predicates and assignments.
    Text,

    /// Non-negative 64-bit integer representable by both current providers.
    UnsignedInteger,
}

///
/// MutationFieldRole
///
/// Semantic role assigned to one accepted field in the compact mutation family.
/// Roles let renderers consume current field names without reconstructing schema facts.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationFieldRole {
    /// Literal-addressable unique primary key.
    Key,

    /// Mutable unsigned numeric field.
    Number,

    /// Mutable text field.
    Text,
}

///
/// MutationField
///
/// Minimal accepted-snapshot field fact required by mutation generation.
/// The accepted catalog supplies these facts; generated entity models do not.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutationField {
    id: u32,
    name: String,
    kind: MutationFieldKind,
    role: MutationFieldRole,
}

impl MutationField {
    /// Build one accepted mutation field fact.
    #[must_use]
    pub fn new(
        id: u32,
        name: impl Into<String>,
        kind: MutationFieldKind,
        role: MutationFieldRole,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            kind,
            role,
        }
    }

    /// Return the accepted field identifier.
    #[must_use]
    pub const fn id(&self) -> u32 {
        self.id
    }

    /// Borrow the accepted field name.
    #[must_use]
    pub const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return the accepted field kind.
    #[must_use]
    pub const fn kind(&self) -> MutationFieldKind {
        self.kind
    }

    /// Return the mutation role.
    #[must_use]
    pub const fn role(&self) -> MutationFieldRole {
        self.role
    }
}

///
/// MutationSnapshot
///
/// Durable accepted-schema facts embedded in a generated mutation sequence.
/// It is the sole schema authority used by typed rendering and replay.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutationSnapshot {
    fixture_family: String,
    entity_path: String,
    entity_name: String,
    version: u32,
    fields: Vec<MutationField>,
}

impl MutationSnapshot {
    /// Build and validate one compact accepted mutation snapshot.
    ///
    /// # Errors
    ///
    /// Returns a typed snapshot error unless the entity and three required
    /// roles are unique, identifier-safe, and kind-compatible.
    pub fn try_new(
        fixture_family: impl Into<String>,
        entity_path: impl Into<String>,
        entity_name: impl Into<String>,
        version: u32,
        mut fields: Vec<MutationField>,
    ) -> Result<Self, SqlGeneratorError> {
        fields.sort_by_key(MutationField::id);
        let snapshot = Self {
            fixture_family: fixture_family.into(),
            entity_path: entity_path.into(),
            entity_name: entity_name.into(),
            version,
            fields,
        };
        snapshot.validate()?;

        Ok(snapshot)
    }

    /// Borrow the stable fixture-family identity.
    #[must_use]
    pub const fn fixture_family(&self) -> &str {
        self.fixture_family.as_str()
    }

    /// Borrow the accepted entity path.
    #[must_use]
    pub const fn entity_path(&self) -> &str {
        self.entity_path.as_str()
    }

    /// Borrow the accepted SQL entity name.
    #[must_use]
    pub const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Return the accepted schema version.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Borrow accepted fields in canonical field-id order.
    #[must_use]
    pub const fn fields(&self) -> &[MutationField] {
        self.fields.as_slice()
    }

    /// Borrow the unique field assigned to a semantic role, when present.
    #[must_use]
    pub fn field(&self, role: MutationFieldRole) -> Option<&MutationField> {
        self.fields.iter().find(|field| field.role == role)
    }

    /// Compute the canonical accepted-snapshot fingerprint embedded in replay.
    ///
    /// # Errors
    ///
    /// Returns a typed serialization error when canonical encoding fails.
    pub fn fingerprint(&self) -> Result<String, SqlGeneratorError> {
        let bytes = crate::replay::canonical_json_bytes(self)?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }

    pub(crate) fn validate(&self) -> Result<(), SqlGeneratorError> {
        if self.fixture_family.is_empty()
            || self.entity_path.is_empty()
            || !is_identifier(self.entity_name.as_str())
            || self.version == 0
            || self.fields.len() != 3
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidSnapshot,
                "mutation snapshot requires non-empty identity, a safe entity name, non-zero version, and three fields",
            ));
        }
        let mut ids = BTreeSet::new();
        let mut names = BTreeSet::new();
        let mut roles = BTreeSet::new();
        for field in &self.fields {
            if field.id == 0
                || !is_identifier(field.name.as_str())
                || !ids.insert(field.id)
                || !names.insert(field.name.as_str())
                || !roles.insert(field.role)
            {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidSnapshot,
                    "mutation fields require non-zero unique IDs, unique safe names, and unique roles",
                ));
            }
            let kind_is_valid = matches!(
                (field.role, field.kind),
                (MutationFieldRole::Text, MutationFieldKind::Text)
                    | (
                        MutationFieldRole::Key | MutationFieldRole::Number,
                        MutationFieldKind::UnsignedInteger
                    )
            );
            if !kind_is_valid {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidSnapshot,
                    "mutation field kind does not match its semantic role",
                ));
            }
        }
        if roles
            != BTreeSet::from([
                MutationFieldRole::Key,
                MutationFieldRole::Number,
                MutationFieldRole::Text,
            ])
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidSnapshot,
                "mutation snapshot must contain key, number, and text roles exactly once",
            ));
        }

        Ok(())
    }

    fn required_field(&self, role: MutationFieldRole) -> Result<&MutationField, SqlGeneratorError> {
        self.field(role).ok_or_else(|| {
            SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidSnapshot,
                "validated mutation snapshot is missing a required field role",
            )
        })
    }
}

///
/// MutationRow
///
/// Canonical row shape tracked independently by the mutation state model.
/// Rows are ordered by key only when representing complete state or unordered RETURNING.
///

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutationRow {
    #[serde(with = "crate::model::tagged_u64")]
    key: u64,
    text: String,
    #[serde(with = "crate::model::tagged_u64")]
    number: u64,
}

impl MutationRow {
    /// Build one model row.
    #[must_use]
    pub fn new(key: u64, text: impl Into<String>, number: u64) -> Self {
        Self {
            key,
            text: text.into(),
            number,
        }
    }

    /// Return the primary-key value.
    #[must_use]
    pub const fn key(&self) -> u64 {
        self.key
    }

    /// Borrow the text value.
    #[must_use]
    pub const fn text(&self) -> &str {
        self.text.as_str()
    }

    /// Return the numeric value.
    #[must_use]
    pub const fn number(&self) -> u64 {
        self.number
    }
}

///
/// MutationPredicate
///
/// Typed predicate subset evaluated by the independent state model and rendered once.
/// It deliberately contains no product predicate or expression representation.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MutationPredicate {
    /// Match every row.
    All,

    /// Require both nested predicates.
    And {
        /// Left predicate.
        left: Box<Self>,

        /// Right predicate.
        right: Box<Self>,
    },

    /// Match one exact primary key.
    KeyEqual {
        /// Required key.
        #[serde(with = "crate::model::tagged_u64")]
        value: u64,
    },

    /// Match a half-open numeric range.
    NumberRange {
        /// Inclusive lower bound.
        #[serde(with = "crate::model::tagged_u64")]
        min_inclusive: u64,

        /// Exclusive upper bound.
        #[serde(with = "crate::model::tagged_u64")]
        max_exclusive: u64,
    },

    /// Match one exact text value.
    TextEqual {
        /// Required text.
        value: String,
    },
}

impl MutationPredicate {
    fn matches(&self, row: &MutationRow) -> bool {
        match self {
            Self::All => true,
            Self::And { left, right } => left.matches(row) && right.matches(row),
            Self::KeyEqual { value } => row.key == *value,
            Self::NumberRange {
                min_inclusive,
                max_exclusive,
            } => row.number >= *min_inclusive && row.number < *max_exclusive,
            Self::TextEqual { value } => row.text == *value,
        }
    }

    fn validate(&self) -> Result<(), SqlGeneratorError> {
        match self {
            Self::And { left, right } => {
                left.validate()?;
                right.validate()
            }
            Self::NumberRange {
                min_inclusive,
                max_exclusive,
            } if min_inclusive >= max_exclusive => Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "mutation numeric range must be non-empty",
            )),
            Self::All
            | Self::KeyEqual { .. }
            | Self::NumberRange { .. }
            | Self::TextEqual { .. } => Ok(()),
        }
    }

    fn render(&self, snapshot: &MutationSnapshot) -> Result<String, SqlGeneratorError> {
        match self {
            Self::All => Ok("1 = 1".to_string()),
            Self::And { left, right } => Ok(format!(
                "({}) AND ({})",
                left.render(snapshot)?,
                right.render(snapshot)?
            )),
            Self::KeyEqual { value } => Ok(format!(
                "{} = {value}",
                snapshot.required_field(MutationFieldRole::Key)?.name()
            )),
            Self::NumberRange {
                min_inclusive,
                max_exclusive,
            } => {
                let field = snapshot.required_field(MutationFieldRole::Number)?.name();
                Ok(format!(
                    "{field} >= {min_inclusive} AND {field} < {max_exclusive}"
                ))
            }
            Self::TextEqual { value } => Ok(format!(
                "{} = '{}'",
                snapshot.required_field(MutationFieldRole::Text)?.name(),
                quote_text(value)
            )),
        }
    }
}

///
/// MutationAssignment
///
/// Typed current-contract UPDATE assignment applied by the independent model.
/// Key mutation is intentionally unrepresentable in this schema family.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MutationAssignment {
    /// Replace the numeric field.
    Number {
        /// New numeric value.
        #[serde(with = "crate::model::tagged_u64")]
        value: u64,
    },

    /// Replace the text field.
    Text {
        /// New text value.
        value: String,
    },

    /// Replace both mutable fields in one statement.
    TextAndNumber {
        /// New text value.
        text: String,

        /// New numeric value.
        #[serde(with = "crate::model::tagged_u64")]
        number: u64,
    },
}

impl MutationAssignment {
    fn apply(&self, row: &mut MutationRow) {
        match self {
            Self::Number { value } => row.number = *value,
            Self::Text { value } => row.text.clone_from(value),
            Self::TextAndNumber { text, number } => {
                row.text.clone_from(text);
                row.number = *number;
            }
        }
    }

    fn render(&self, snapshot: &MutationSnapshot) -> Result<String, SqlGeneratorError> {
        let text = snapshot.required_field(MutationFieldRole::Text)?.name();
        let number = snapshot.required_field(MutationFieldRole::Number)?.name();
        Ok(match self {
            Self::Number { value } => format!("{number} = {value}"),
            Self::Text { value } => format!("{text} = '{}'", quote_text(value)),
            Self::TextAndNumber {
                text: value,
                number: number_value,
            } => format!(
                "{text} = '{}', {number} = {number_value}",
                quote_text(value)
            ),
        })
    }
}

///
/// MutationOrder
///
/// Deterministic key order used only by bounded UPDATE and DELETE selection.
/// RETURNING rows remain normalized as unordered by the mutation contract.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationOrder {
    /// Ascending primary-key order.
    KeyAscending,

    /// Descending primary-key order.
    KeyDescending,
}

///
/// MutationInsertQueryKeySource
///
/// Accepted unsigned field projected into the target key by INSERT FROM QUERY.
/// This keeps source-query typing explicit without adding computed-value coercion.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationInsertQueryKeySource {
    /// Project the source primary key.
    Key,

    /// Project the source mutable numeric field.
    Number,
}

///
/// MutationWindow
///
/// Optional deterministic candidate window for UPDATE or DELETE.
/// OFFSET is only representable with explicit key order and LIMIT.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutationWindow {
    order: MutationOrder,
    limit: u32,
    offset: u32,
}

impl MutationWindow {
    /// Build a non-zero ordered mutation window.
    ///
    /// # Errors
    ///
    /// Returns a typed case error when `limit` is zero.
    pub fn try_new(
        order: MutationOrder,
        limit: u32,
        offset: u32,
    ) -> Result<Self, SqlGeneratorError> {
        if limit == 0 {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "mutation window LIMIT must be non-zero",
            ));
        }

        Ok(Self {
            order,
            limit,
            offset,
        })
    }

    /// Return deterministic key order.
    #[must_use]
    pub const fn order(self) -> MutationOrder {
        self.order
    }

    /// Return the mutation limit.
    #[must_use]
    pub const fn limit(self) -> u32 {
        self.limit
    }

    /// Return the mutation offset.
    #[must_use]
    pub const fn offset(self) -> u32 {
        self.offset
    }
}

///
/// MutationOperation
///
/// Typed INSERT, UPDATE, or DELETE operation for one state-machine step.
/// Statement response shape is owned separately by `MutationStatement`.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "verb", rename_all = "snake_case")]
pub enum MutationOperation {
    /// Delete rows selected by a predicate and optional deterministic window.
    Delete {
        /// Typed predicate.
        predicate: MutationPredicate,

        /// Optional ordered window.
        window: Option<MutationWindow>,
    },

    /// Insert one or more complete rows atomically.
    Insert {
        /// Rows in statement value-list order.
        rows: Vec<MutationRow>,
    },

    /// Insert rows projected from the same entity with an explicit key source.
    InsertFromQuery {
        /// Source-row predicate.
        predicate: MutationPredicate,

        /// Accepted unsigned source field projected into the target key.
        key_source: MutationInsertQueryKeySource,
    },

    /// Update rows selected by a predicate and optional deterministic window.
    Update {
        /// Typed predicate.
        predicate: MutationPredicate,

        /// Typed assignment.
        assignment: MutationAssignment,

        /// Optional ordered window.
        window: Option<MutationWindow>,
    },
}

impl MutationOperation {
    fn validate(&self) -> Result<(), SqlGeneratorError> {
        match self {
            Self::Insert { rows } if rows.is_empty() => Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "mutation INSERT requires at least one row",
            )),
            Self::Insert { .. } => Ok(()),
            Self::Delete { predicate, .. }
            | Self::InsertFromQuery { predicate, .. }
            | Self::Update { predicate, .. } => predicate.validate(),
        }
    }

    /// Return whether this operation is INSERT.
    #[must_use]
    pub const fn is_insert(&self) -> bool {
        matches!(self, Self::Insert { .. } | Self::InsertFromQuery { .. })
    }

    /// Return whether this operation is UPDATE.
    #[must_use]
    pub const fn is_update(&self) -> bool {
        matches!(self, Self::Update { .. })
    }

    /// Return whether this operation is DELETE.
    #[must_use]
    pub const fn is_delete(&self) -> bool {
        matches!(self, Self::Delete { .. })
    }

    const fn window(&self) -> Option<MutationWindow> {
        match self {
            Self::Delete { window, .. } | Self::Update { window, .. } => *window,
            Self::Insert { .. } | Self::InsertFromQuery { .. } => None,
        }
    }
}

///
/// MutationStatement
///
/// One typed DML statement with an explicit full-row RETURNING contract.
/// The renderer consumes only this AST and its embedded accepted snapshot.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutationStatement {
    operation: MutationOperation,
    returning: bool,
}

impl MutationStatement {
    /// Build one typed mutation statement.
    #[must_use]
    pub const fn new(operation: MutationOperation, returning: bool) -> Self {
        Self {
            operation,
            returning,
        }
    }

    /// Borrow the operation.
    #[must_use]
    pub const fn operation(&self) -> &MutationOperation {
        &self.operation
    }

    /// Return whether the statement produces full-row RETURNING output.
    #[must_use]
    pub const fn returning(&self) -> bool {
        self.returning
    }

    /// Render current-contract SQL from accepted field names.
    ///
    /// # Errors
    ///
    /// Returns a typed rendering error when the statement violates the typed contract.
    pub fn render(&self, snapshot: &MutationSnapshot) -> Result<String, SqlGeneratorError> {
        snapshot.validate()?;
        self.operation.validate()?;
        let entity = snapshot.entity_name();
        let key = snapshot.required_field(MutationFieldRole::Key)?.name();
        let text = snapshot.required_field(MutationFieldRole::Text)?.name();
        let number = snapshot.required_field(MutationFieldRole::Number)?.name();
        let mut sql = match &self.operation {
            MutationOperation::Delete { predicate, .. } => {
                format!("DELETE FROM {entity} WHERE {}", predicate.render(snapshot)?)
            }
            MutationOperation::Insert { rows } => {
                let values = rows
                    .iter()
                    .map(|row| {
                        format!(
                            "({}, '{}', {})",
                            row.key,
                            quote_text(row.text.as_str()),
                            row.number
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("INSERT INTO {entity} ({key}, {text}, {number}) VALUES {values}")
            }
            MutationOperation::InsertFromQuery {
                predicate,
                key_source,
            } => {
                let source_key = match key_source {
                    MutationInsertQueryKeySource::Key => key,
                    MutationInsertQueryKeySource::Number => number,
                };
                format!(
                    "INSERT INTO {entity} ({key}, {text}, {number}) SELECT {source_key}, {text}, {number} FROM {entity} WHERE {} ORDER BY {key} ASC",
                    predicate.render(snapshot)?
                )
            }
            MutationOperation::Update {
                predicate,
                assignment,
                ..
            } => format!(
                "UPDATE {entity} SET {} WHERE {}",
                assignment.render(snapshot)?,
                predicate.render(snapshot)?
            ),
        };
        if let Some(window) = self.operation.window() {
            let direction = match window.order {
                MutationOrder::KeyAscending => "ASC",
                MutationOrder::KeyDescending => "DESC",
            };
            write!(
                &mut sql,
                " ORDER BY {key} {direction} LIMIT {}",
                window.limit
            )
            .map_err(|_| {
                SqlGeneratorError::new(
                    SqlGeneratorErrorKind::Rendering,
                    "failed to append the mutation window to rendered SQL",
                )
            })?;
            if window.offset > 0 {
                write!(&mut sql, " OFFSET {}", window.offset).map_err(|_| {
                    SqlGeneratorError::new(
                        SqlGeneratorErrorKind::Rendering,
                        "failed to append the mutation offset to rendered SQL",
                    )
                })?;
            }
        }
        if self.returning {
            write!(&mut sql, " RETURNING {key}, {text}, {number}").map_err(|_| {
                SqlGeneratorError::new(
                    SqlGeneratorErrorKind::Rendering,
                    "failed to append the mutation RETURNING clause to rendered SQL",
                )
            })?;
        }

        Ok(sql)
    }
}

///
/// MutationExpectedRejection
///
/// Stable model-owned rejection class for a generated invalid mutation.
/// Product and reference adapters map their typed failures to this class.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationExpectedRejection {
    /// INSERT would violate primary-key uniqueness.
    DuplicateKey,
}

impl MutationExpectedRejection {
    /// Return the stable replay identity for this modeled rejection class.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::DuplicateKey => "duplicate_key",
        }
    }
}

///
/// MutationStepOutcome
///
/// Independent model result for one atomic statement, including complete post-state.
/// RETURNING rows are canonicalized by key because the maintained contract is unordered.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum MutationStepOutcome {
    /// The statement completed and committed its full transition.
    Accepted {
        /// Number of affected rows.
        affected_rows: u32,

        /// Canonical unordered RETURNING rows, empty when RETURNING is absent.
        returned_rows: Vec<MutationRow>,

        /// Complete canonical state after the statement.
        state_after: Vec<MutationRow>,
    },

    /// The statement rejected atomically.
    Rejected {
        /// Stable rejection class.
        rejection: MutationExpectedRejection,

        /// Unchanged complete canonical state after rejection.
        state_after: Vec<MutationRow>,
    },
}

impl MutationStepOutcome {
    /// Return the complete canonical post-state.
    #[must_use]
    pub const fn state_after(&self) -> &[MutationRow] {
        match self {
            Self::Accepted { state_after, .. } | Self::Rejected { state_after, .. } => {
                state_after.as_slice()
            }
        }
    }

    /// Return the affected-row count for an accepted statement.
    #[must_use]
    pub const fn affected_rows(&self) -> Option<u32> {
        match self {
            Self::Accepted { affected_rows, .. } => Some(*affected_rows),
            Self::Rejected { .. } => None,
        }
    }

    /// Borrow canonical RETURNING rows for an accepted statement.
    #[must_use]
    pub const fn returned_rows(&self) -> Option<&[MutationRow]> {
        match self {
            Self::Accepted { returned_rows, .. } => Some(returned_rows.as_slice()),
            Self::Rejected { .. } => None,
        }
    }

    /// Return the stable rejection class, when rejected.
    #[must_use]
    pub const fn rejection(&self) -> Option<MutationExpectedRejection> {
        match self {
            Self::Accepted { .. } => None,
            Self::Rejected { rejection, .. } => Some(*rejection),
        }
    }
}

///
/// MutationSqliteExclusion
///
/// Explicit reason a generated statement does not belong to the maintained SQLite overlap.
/// Exclusions are typed evidence and never inferred from an adapter failure.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationSqliteExclusion {
    /// SQLite's maintained overlap excludes IcyDB's ordered mutation window grammar.
    WindowedMutation,
}

///
/// MutationSqliteEligibility
///
/// Per-statement secondary-provider contract decided before either provider executes.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", content = "reason", rename_all = "snake_case")]
pub enum MutationSqliteEligibility {
    /// Execute and compare against bundled SQLite.
    Eligible,

    /// Do not execute SQLite; preserve the enumerated exclusion reason.
    Excluded(MutationSqliteExclusion),
}

///
/// GeneratedMutationIdentity
///
/// Stable versioned identity for one deterministic mutation sequence.
/// Root seed and case index are retained independently for replay and sharding.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedMutationIdentity {
    id: String,
    generator_version: u32,
    family_id: String,
    #[serde(with = "crate::model::tagged_u64")]
    root_seed: u64,
    #[serde(with = "crate::model::tagged_u64")]
    sub_seed: u64,
    #[serde(with = "crate::model::tagged_u64")]
    case_index: u64,
}

impl GeneratedMutationIdentity {
    pub(crate) fn new(
        id: impl Into<String>,
        generator_version: u32,
        family_id: String,
        root_seed: u64,
        sub_seed: u64,
        case_index: u64,
    ) -> Self {
        Self {
            id: id.into(),
            generator_version,
            family_id,
            root_seed,
            sub_seed,
            case_index,
        }
    }

    /// Borrow the stable case identifier.
    #[must_use]
    pub const fn id(&self) -> &str {
        self.id.as_str()
    }

    /// Return the generator version.
    #[must_use]
    pub const fn generator_version(&self) -> u32 {
        self.generator_version
    }

    /// Borrow the independently seeded generator family identity.
    #[must_use]
    pub const fn family_id(&self) -> &str {
        self.family_id.as_str()
    }

    /// Return the root seed.
    #[must_use]
    pub const fn root_seed(&self) -> u64 {
        self.root_seed
    }

    /// Return the independently derived sub-seed.
    #[must_use]
    pub const fn sub_seed(&self) -> u64 {
        self.sub_seed
    }

    /// Return the root-local case index.
    #[must_use]
    pub const fn case_index(&self) -> u64 {
        self.case_index
    }

    fn validate(&self) -> Result<(), SqlGeneratorError> {
        crate::mutation::generator::validate_generated_mutation_identity(self)
    }
}

///
/// GeneratedMutationStep
///
/// One typed statement paired with independently derived pre-state, outcome, SQL, and provider eligibility.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedMutationStep {
    statement: MutationStatement,
    rendered_sql: String,
    sqlite_eligibility: MutationSqliteEligibility,
    state_before: Vec<MutationRow>,
    expected: MutationStepOutcome,
}

impl GeneratedMutationStep {
    /// Borrow the typed statement.
    #[must_use]
    pub const fn statement(&self) -> &MutationStatement {
        &self.statement
    }

    /// Borrow current-contract rendered SQL.
    #[must_use]
    pub const fn rendered_sql(&self) -> &str {
        self.rendered_sql.as_str()
    }

    /// Return secondary SQLite eligibility.
    #[must_use]
    pub const fn sqlite_eligibility(&self) -> MutationSqliteEligibility {
        self.sqlite_eligibility
    }

    /// Borrow complete canonical pre-state.
    #[must_use]
    pub const fn state_before(&self) -> &[MutationRow] {
        self.state_before.as_slice()
    }

    /// Borrow the independent expected outcome.
    #[must_use]
    pub const fn expected(&self) -> &MutationStepOutcome {
        &self.expected
    }
}

///
/// GeneratedMutationSequence
///
/// Bounded replayable sequence derived from one accepted snapshot and initial fixture.
/// Every expected transition is recomputed by this crate's independent row model.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedMutationSequence {
    identity: GeneratedMutationIdentity,
    snapshot: MutationSnapshot,
    initial_rows: Vec<MutationRow>,
    steps: Vec<GeneratedMutationStep>,
    budgets: MutationBudgets,
}

impl GeneratedMutationSequence {
    /// Build one sequence and derive every expected transition atomically.
    ///
    /// # Errors
    ///
    /// Returns a typed case error for invalid schema, fixture, statement, or budget facts.
    pub fn try_from_statements(
        identity: GeneratedMutationIdentity,
        snapshot: MutationSnapshot,
        initial_rows: Vec<MutationRow>,
        statements: Vec<MutationStatement>,
        budgets: MutationBudgets,
    ) -> Result<Self, SqlGeneratorError> {
        identity.validate()?;
        snapshot.validate()?;
        budgets.validate()?;
        validate_rows(initial_rows.as_slice())?;
        if initial_rows.len() > budgets.max_fixture_rows as usize
            || statements.is_empty()
            || statements.len() > budgets.max_statements as usize
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "mutation fixture or statement count violates its deterministic budget",
            ));
        }

        let mut state = canonical_rows(initial_rows.clone());
        let mut steps = Vec::with_capacity(statements.len());
        for statement in statements {
            statement.operation.validate()?;
            let state_before = state.clone();
            let expected = apply_statement(state.as_slice(), &statement)?;
            state = expected.state_after().to_vec();
            let sqlite_eligibility = if statement.operation.window().is_some() {
                MutationSqliteEligibility::Excluded(MutationSqliteExclusion::WindowedMutation)
            } else {
                MutationSqliteEligibility::Eligible
            };
            let rendered_sql = statement.render(&snapshot)?;
            steps.push(GeneratedMutationStep {
                statement,
                rendered_sql,
                sqlite_eligibility,
                state_before,
                expected,
            });
        }
        let sequence = Self {
            identity,
            snapshot,
            initial_rows: canonical_rows(initial_rows),
            steps,
            budgets,
        };
        sequence.validate()?;

        Ok(sequence)
    }

    /// Borrow the deterministic sequence identity.
    #[must_use]
    pub const fn identity(&self) -> &GeneratedMutationIdentity {
        &self.identity
    }

    /// Borrow embedded accepted-snapshot facts.
    #[must_use]
    pub const fn snapshot(&self) -> &MutationSnapshot {
        &self.snapshot
    }

    /// Borrow canonical initial fixture rows.
    #[must_use]
    pub const fn initial_rows(&self) -> &[MutationRow] {
        self.initial_rows.as_slice()
    }

    /// Borrow generated steps.
    #[must_use]
    pub const fn steps(&self) -> &[GeneratedMutationStep] {
        self.steps.as_slice()
    }

    /// Return deterministic sequence budgets.
    #[must_use]
    pub const fn budgets(&self) -> MutationBudgets {
        self.budgets
    }

    /// Borrow the final canonical modeled state.
    #[must_use]
    pub fn final_state(&self) -> &[MutationRow] {
        self.steps
            .last()
            .map_or(self.initial_rows.as_slice(), |step| {
                step.expected.state_after()
            })
    }

    /// Compute a canonical fingerprint for replay diagnostics.
    ///
    /// # Errors
    ///
    /// Returns a typed serialization error when canonical encoding fails.
    pub fn fingerprint(&self) -> Result<String, SqlGeneratorError> {
        let bytes = crate::replay::canonical_json_bytes(self)?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }

    /// Revalidate all embedded authority, rendering, state, and budget facts.
    ///
    /// # Errors
    ///
    /// Returns a typed case error at the first stale or inconsistent fact.
    pub fn validate(&self) -> Result<(), SqlGeneratorError> {
        self.identity.validate()?;
        self.snapshot.validate()?;
        self.budgets.validate()?;
        validate_rows(self.initial_rows.as_slice())?;
        if self.initial_rows.len() > self.budgets.max_fixture_rows as usize
            || self.steps.is_empty()
            || self.steps.len() > self.budgets.max_statements as usize
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "mutation sequence exceeds fixture or statement budget",
            ));
        }
        let mut state = self.initial_rows.clone();
        for step in &self.steps {
            if step.state_before != state
                || step.rendered_sql != step.statement.render(&self.snapshot)?
                || step.sqlite_eligibility
                    != if step.statement.operation.window().is_some() {
                        MutationSqliteEligibility::Excluded(
                            MutationSqliteExclusion::WindowedMutation,
                        )
                    } else {
                        MutationSqliteEligibility::Eligible
                    }
            {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidCase,
                    "mutation step embeds stale pre-state, SQL, or SQLite eligibility",
                ));
            }
            let expected = apply_statement(state.as_slice(), &step.statement)?;
            if step.expected != expected {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidCase,
                    "mutation step embeds an outcome not produced by the independent model",
                ));
            }
            state = expected.state_after().to_vec();
        }

        Ok(())
    }

    pub(crate) fn statements(&self) -> Vec<MutationStatement> {
        self.steps
            .iter()
            .map(|step| step.statement.clone())
            .collect()
    }

    pub(crate) fn rebuilt(
        &self,
        initial_rows: Vec<MutationRow>,
        statements: Vec<MutationStatement>,
    ) -> Result<Self, SqlGeneratorError> {
        Self::try_from_statements(
            self.identity.clone(),
            self.snapshot.clone(),
            initial_rows,
            statements,
            self.budgets,
        )
    }
}

fn apply_statement(
    state_before: &[MutationRow],
    statement: &MutationStatement,
) -> Result<MutationStepOutcome, SqlGeneratorError> {
    let mut state_after = state_before.to_vec();
    match &statement.operation {
        MutationOperation::Insert { rows } => {
            apply_insert_rows(state_before, rows.clone(), statement.returning)
        }
        MutationOperation::InsertFromQuery {
            predicate,
            key_source,
        } => {
            let rows = state_before
                .iter()
                .filter(|row| predicate.matches(row))
                .map(|row| {
                    let key = match key_source {
                        MutationInsertQueryKeySource::Key => row.key,
                        MutationInsertQueryKeySource::Number => row.number,
                    };
                    MutationRow::new(key, row.text.clone(), row.number)
                })
                .collect::<Vec<_>>();
            apply_insert_rows(state_before, rows, statement.returning)
        }
        MutationOperation::Update {
            predicate,
            assignment,
            window,
        } => {
            let selected = selected_indices(state_before, predicate, *window);
            let mut returned_rows = Vec::with_capacity(selected.len());
            for index in selected.iter().copied() {
                assignment.apply(&mut state_after[index]);
                if statement.returning {
                    returned_rows.push(state_after[index].clone());
                }
            }
            returned_rows = canonical_rows(returned_rows);
            state_after = canonical_rows(state_after);
            Ok(MutationStepOutcome::Accepted {
                affected_rows: row_count(selected.len())?,
                returned_rows,
                state_after,
            })
        }
        MutationOperation::Delete { predicate, window } => {
            let selected = selected_indices(state_before, predicate, *window);
            let selected_keys = selected
                .iter()
                .map(|index| state_before[*index].key)
                .collect::<BTreeSet<_>>();
            let returned_rows = if statement.returning {
                canonical_rows(
                    selected
                        .iter()
                        .map(|index| state_before[*index].clone())
                        .collect(),
                )
            } else {
                Vec::new()
            };
            state_after.retain(|row| !selected_keys.contains(&row.key));
            Ok(MutationStepOutcome::Accepted {
                affected_rows: row_count(selected.len())?,
                returned_rows,
                state_after,
            })
        }
    }
}

fn apply_insert_rows(
    state_before: &[MutationRow],
    rows: Vec<MutationRow>,
    returning: bool,
) -> Result<MutationStepOutcome, SqlGeneratorError> {
    let existing = state_before
        .iter()
        .map(MutationRow::key)
        .collect::<BTreeSet<_>>();
    let mut inserted = BTreeSet::new();
    if rows
        .iter()
        .any(|row| existing.contains(&row.key) || !inserted.insert(row.key))
    {
        return Ok(MutationStepOutcome::Rejected {
            rejection: MutationExpectedRejection::DuplicateKey,
            state_after: state_before.to_vec(),
        });
    }
    let mut state_after = state_before.to_vec();
    state_after.extend(rows.iter().cloned());
    state_after = canonical_rows(state_after);
    let returned_rows = if returning {
        canonical_rows(rows.clone())
    } else {
        Vec::new()
    };
    Ok(MutationStepOutcome::Accepted {
        affected_rows: row_count(rows.len())?,
        returned_rows,
        state_after,
    })
}

fn selected_indices(
    rows: &[MutationRow],
    predicate: &MutationPredicate,
    window: Option<MutationWindow>,
) -> Vec<usize> {
    let mut selected = rows
        .iter()
        .enumerate()
        .filter_map(|(index, row)| predicate.matches(row).then_some(index))
        .collect::<Vec<_>>();
    let Some(window) = window else {
        return selected;
    };
    selected.sort_by(|left, right| {
        let ordering = rows[*left].key.cmp(&rows[*right].key);
        match window.order {
            MutationOrder::KeyAscending => ordering,
            MutationOrder::KeyDescending => ordering.reverse(),
        }
    });
    selected
        .into_iter()
        .skip(window.offset as usize)
        .take(window.limit as usize)
        .collect()
}

fn validate_rows(rows: &[MutationRow]) -> Result<(), SqlGeneratorError> {
    let mut keys = BTreeSet::new();
    if rows.iter().any(|row| !keys.insert(row.key)) {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "mutation fixture contains duplicate primary keys",
        ));
    }

    Ok(())
}

fn canonical_rows(mut rows: Vec<MutationRow>) -> Vec<MutationRow> {
    rows.sort_by_key(MutationRow::key);
    rows
}

fn row_count(count: usize) -> Result<u32, SqlGeneratorError> {
    u32::try_from(count).map_err(|_| {
        SqlGeneratorError::new(
            SqlGeneratorErrorKind::Budget,
            "mutation affected-row count exceeds u32",
        )
    })
}

fn quote_text(value: &str) -> String {
    value.replace('\'', "''")
}

fn is_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    chars
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}
