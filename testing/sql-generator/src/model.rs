//! Module: sql_generator::model
//! Responsibility: accepted-snapshot facts, typed SELECT AST, and bounded case identity.
//! Does not own: random generation policy, SQL execution, or mismatch evaluation.
//! Boundary: makes valid scalar and aggregate query states explicit before current-contract rendering.

use crate::{GeneratedFixture, GeneratedValue, SqlGeneratorError, SqlGeneratorErrorKind};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const MAX_GENERATED_MEMBERSHIP_MEMBERS: usize = 8;

/// Required native budgets for the initial 0.204 SELECT generator lane.
pub const TIER_A_SELECT_BUDGETS: SelectBudgets = SelectBudgets::new(16, 3, 4, 3, 256, 512, 262_144);

/// Required scheduled and closeout budgets for the 0.204 SELECT generator lane.
pub const TIER_C_SELECT_BUDGETS: SelectBudgets =
    SelectBudgets::new(64, 4, 4, 3, 4_096, 8_192, 1_048_576);

/// Every maintained SELECT generator family in stable identity order.
pub const ALL_SELECT_GENERATOR_FAMILIES: &[SelectGeneratorFamily] = &[
    SelectGeneratorFamily::Distinct,
    SelectGeneratorFamily::Expression,
    SelectGeneratorFamily::GlobalAggregate,
    SelectGeneratorFamily::GroupedAggregate,
    SelectGeneratorFamily::Having,
    SelectGeneratorFamily::Predicate,
    SelectGeneratorFamily::ScalarProjection,
    SelectGeneratorFamily::Window,
];

/// Every initial classified invalid mutation in stable identity order.
pub const ALL_SELECT_VIOLATIONS: &[SelectViolation] = &[
    SelectViolation::InvalidClauseOrder,
    SelectViolation::LimitOverflow,
    SelectViolation::UnknownField,
    SelectViolation::UnsupportedFunctionSignature,
    SelectViolation::WrongOperatorType,
];

///
/// SelectValueKind
///
/// Scalar value type admitted by the initial generated SELECT overlap.
/// Owned by the generator and mapped explicitly by each execution adapter.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectValueKind {
    /// Boolean scalar values.
    Boolean,

    /// Exact decimal result values produced by numeric SQL expressions.
    Decimal,

    /// Signed 64-bit integer scalar values.
    Integer,

    /// UTF-8 text under bytewise comparison.
    Text,
}

///
/// SelectFieldKind
///
/// Accepted field-kind fact embedded in a generated snapshot. Initial query
/// generation uses boolean, integer, and text; other kinds remain explicit.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectFieldKind {
    /// Accepted blob field excluded from initial scalar expression generation.
    Blob,

    /// Accepted boolean field.
    Boolean,

    /// Accepted signed integer field represented exactly as `i64`.
    Integer,

    /// Accepted UTF-8 text field.
    Text,

    /// Accepted ULID field excluded from the initial SQLite overlap.
    Ulid,
}

impl SelectFieldKind {
    /// Return the generated scalar type when this accepted kind participates.
    #[must_use]
    pub const fn value_kind(self) -> Option<SelectValueKind> {
        match self {
            Self::Boolean => Some(SelectValueKind::Boolean),
            Self::Integer => Some(SelectValueKind::Integer),
            Self::Text => Some(SelectValueKind::Text),
            Self::Blob | Self::Ulid => None,
        }
    }

    /// Return whether fixture generation may assign this field directly.
    #[must_use]
    pub const fn is_generated_scalar(self) -> bool {
        self.value_kind().is_some()
    }
}

///
/// SelectField
///
/// Canonical accepted field fact consumed by generation. Durable identity,
/// current name, type, nullability, key role, and write ownership remain explicit.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SelectField {
    #[serde(with = "tagged_u32")]
    id: u32,
    name: String,
    kind: SelectFieldKind,
    nullable: bool,
    primary_key: bool,
    generated: bool,
}

impl SelectField {
    /// Build one accepted field fact before snapshot validation and ordering.
    #[must_use]
    pub fn new(
        id: u32,
        name: impl Into<String>,
        kind: SelectFieldKind,
        nullable: bool,
        primary_key: bool,
        generated: bool,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            kind,
            nullable,
            primary_key,
            generated,
        }
    }

    /// Return the durable accepted field identity.
    #[must_use]
    pub const fn id(&self) -> u32 {
        self.id
    }

    /// Borrow the current accepted field name.
    #[must_use]
    pub const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return the accepted generator field kind.
    #[must_use]
    pub const fn kind(&self) -> SelectFieldKind {
        self.kind
    }

    /// Return whether the accepted field permits SQL `NULL`.
    #[must_use]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Return whether the field participates in accepted primary-key identity.
    #[must_use]
    pub const fn primary_key(&self) -> bool {
        self.primary_key
    }

    /// Return whether the database generates or manages the field value.
    #[must_use]
    pub const fn generated(&self) -> bool {
        self.generated
    }
}

///
/// SelectIndex
///
/// Canonical stable index identity and direct accepted field participation.
/// Initial generation records these facts but does not force execution routes.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SelectIndex {
    #[serde(with = "tagged_u16")]
    id: u16,
    name: String,
    #[serde(with = "tagged_u32_vec")]
    field_ids: Vec<u32>,
}

impl SelectIndex {
    /// Build one accepted index fact before snapshot validation and ordering.
    #[must_use]
    pub fn new(id: u16, name: impl Into<String>, field_ids: Vec<u32>) -> Self {
        Self {
            id,
            name: name.into(),
            field_ids,
        }
    }

    /// Return the stable accepted index identity.
    #[must_use]
    pub const fn id(&self) -> u16 {
        self.id
    }

    /// Borrow the accepted index name.
    #[must_use]
    pub const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Borrow direct accepted field identities in key order.
    #[must_use]
    pub const fn field_ids(&self) -> &[u32] {
        self.field_ids.as_slice()
    }
}

///
/// SelectSnapshot
///
/// Bounded canonical accepted-snapshot material embedded in every replay.
/// Construction sorts fields and indexes by stable identity before generation.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SelectSnapshot {
    fixture_family: String,
    entity_path: String,
    entity_name: String,
    version: u32,
    fields: Vec<SelectField>,
    indexes: Vec<SelectIndex>,
}

impl SelectSnapshot {
    /// Build and canonicalize one accepted test snapshot.
    ///
    /// # Errors
    ///
    /// Returns a typed snapshot error for invalid identities, duplicate facts,
    /// unsafe current names, or indexes that reference absent fields.
    pub fn try_new(
        fixture_family: impl Into<String>,
        entity_path: impl Into<String>,
        entity_name: impl Into<String>,
        version: u32,
        mut fields: Vec<SelectField>,
        mut indexes: Vec<SelectIndex>,
    ) -> Result<Self, SqlGeneratorError> {
        let fixture_family = fixture_family.into();
        let entity_path = entity_path.into();
        let entity_name = entity_name.into();
        if fixture_family.is_empty() || entity_path.is_empty() || version == 0 {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidSnapshot,
                "SELECT snapshot needs fixture identity, entity path, and non-zero version",
            ));
        }
        validate_identifier(&entity_name, "snapshot entity")?;
        fields.sort_by(|left, right| left.id.cmp(&right.id).then(left.name.cmp(&right.name)));
        indexes.sort_by(|left, right| left.id.cmp(&right.id).then(left.name.cmp(&right.name)));
        validate_snapshot_fields(&fields)?;
        validate_snapshot_indexes(&fields, &indexes)?;

        Ok(Self {
            fixture_family,
            entity_path,
            entity_name,
            version,
            fields,
            indexes,
        })
    }

    /// Borrow the stable snapshot-fixture family identity.
    #[must_use]
    pub const fn fixture_family(&self) -> &str {
        self.fixture_family.as_str()
    }

    /// Borrow the accepted entity path.
    #[must_use]
    pub const fn entity_path(&self) -> &str {
        self.entity_path.as_str()
    }

    /// Borrow the current accepted entity name used by SQL rendering.
    #[must_use]
    pub const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Return the accepted schema version.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Borrow accepted fields in durable identity order.
    #[must_use]
    pub const fn fields(&self) -> &[SelectField] {
        self.fields.as_slice()
    }

    /// Borrow accepted indexes in stable identity order.
    #[must_use]
    pub const fn indexes(&self) -> &[SelectIndex] {
        self.indexes.as_slice()
    }

    /// Return the canonical BLAKE3 fingerprint of the embedded snapshot.
    ///
    /// # Errors
    ///
    /// Returns a typed serialization error if canonical JSON construction fails.
    pub fn fingerprint(&self) -> Result<String, SqlGeneratorError> {
        let bytes = crate::replay::canonical_json_bytes(self)?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }

    pub(crate) fn field_by_id(&self, id: u32) -> Option<&SelectField> {
        self.fields.iter().find(|field| field.id == id)
    }

    pub(crate) fn first_query_field(&self, kind: SelectFieldKind) -> Option<&SelectField> {
        self.fields
            .iter()
            .find(|field| field.kind == kind && !field.primary_key && !field.generated)
    }

    pub(crate) fn query_fields(&self, kind: SelectFieldKind) -> Vec<&SelectField> {
        self.fields
            .iter()
            .filter(|field| field.kind == kind && !field.primary_key && !field.generated)
            .collect()
    }
}

///
/// SelectGeneratorFamily
///
/// Independently seeded SELECT generation family owned by the 0.204 harness.
/// Family identity partitions deterministic streams and evidence coverage.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectGeneratorFamily {
    /// Scalar `SELECT DISTINCT` projection and ordering semantics.
    Distinct,

    /// Arithmetic, functions, null-producing expressions, and searched `CASE`.
    Expression,

    /// Whole-input aggregate projection without explicit grouping keys.
    GlobalAggregate,

    /// Explicit grouping keys paired with aggregate projections.
    GroupedAggregate,

    /// Global and grouped post-aggregate rejection predicates.
    Having,

    /// Typed WHERE comparisons, boolean combinations, text, and null predicates.
    Predicate,

    /// Plain and aliased scalar projection shapes.
    ScalarProjection,

    /// Deterministic ordering, limits, offsets, and alias order targets.
    Window,
}

impl SelectGeneratorFamily {
    /// Return the stable family identity included in BLAKE3 sub-seeds.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Distinct => "select.distinct",
            Self::Expression => "select.expression",
            Self::GlobalAggregate => "select.global_aggregate",
            Self::GroupedAggregate => "select.grouped_aggregate",
            Self::Having => "select.having",
            Self::Predicate => "select.predicate",
            Self::ScalarProjection => "select.scalar_projection",
            Self::Window => "select.window",
        }
    }

    /// Borrow contract identifiers covered by the family as a whole.
    #[must_use]
    pub const fn contract_features(self) -> &'static [&'static str] {
        match self {
            Self::Distinct => &["projection.scalar", "select.scalar_distinct"],
            Self::Expression => &[
                "expression.numeric_functions",
                "expression.searched_case",
                "expression.text_functions",
                "expression.value_selection",
                "select.computed_projection",
            ],
            Self::GlobalAggregate => &[
                "projection.aggregate",
                "select.aggregate_distinct_filter",
                "select.global_aggregate",
            ],
            Self::GroupedAggregate => &[
                "projection.aggregate",
                "projection.grouped_layout",
                "select.grouped_aggregate",
            ],
            Self::Having => &[
                "having.global_aggregate",
                "having.grouped_aggregate",
                "ordering.projection_alias",
                "projection.aggregate",
                "select.grouped_composition",
            ],
            Self::Predicate => &[
                "predicate.boolean_comparison",
                "predicate.boolean_truth",
                "predicate.casefold_prefix",
                "predicate.expression_arguments",
                "predicate.field_comparison",
                "predicate.membership",
                "predicate.null",
                "predicate.prefix_pattern",
                "predicate.range",
                "predicate.starts_with",
            ],
            Self::ScalarProjection => &[
                "projection.aliases",
                "projection.scalar",
                "select.scalar_rows",
            ],
            Self::Window => &[
                "expression.numeric_functions",
                "ordering.projection_alias",
                "pagination.scalar_limit_offset",
                "predicate.boolean_comparison",
                "projection.scalar",
                "select.computed_projection",
                "select.scalar_composition",
                "select.scalar_rows",
            ],
        }
    }
}

///
/// SelectViolation
///
/// One classified invalid mutation applied to an otherwise valid typed case.
/// Each variant owns its expected rejection before SQL is rendered.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectViolation {
    /// Place `OFFSET` before `LIMIT` contrary to the current SELECT grammar.
    InvalidClauseOrder,

    /// Render a limit one greater than the unsigned 32-bit grammar range.
    LimitOverflow,

    /// Replace a valid field reference with an absent current field identity.
    UnknownField,

    /// Apply a maintained scalar function to an unsupported argument type.
    UnsupportedFunctionSignature,

    /// Apply numeric addition to a text operand.
    WrongOperatorType,
}

impl SelectViolation {
    /// Return the stable independently seeded violation-family identity.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::InvalidClauseOrder => "invalid.clause_order",
            Self::LimitOverflow => "invalid.limit_overflow",
            Self::UnknownField => "invalid.unknown_field",
            Self::UnsupportedFunctionSignature => "invalid.function_signature",
            Self::WrongOperatorType => "invalid.operator_type",
        }
    }

    /// Return the expected typed rejection attached before rendering.
    #[must_use]
    pub const fn expected_rejection(self) -> SelectExpectedRejection {
        match self {
            Self::InvalidClauseOrder => SelectExpectedRejection::InvalidClauseOrder,
            Self::LimitOverflow => SelectExpectedRejection::LimitOverflow,
            Self::UnknownField => SelectExpectedRejection::UnknownField,
            Self::UnsupportedFunctionSignature => {
                SelectExpectedRejection::UnsupportedFunctionSignature
            }
            Self::WrongOperatorType => SelectExpectedRejection::WrongOperatorType,
        }
    }
}

///
/// SelectExpectedRejection
///
/// Stable semantic rejection expected from one invalid generated case.
/// Product adapters map these classes to their owned typed error taxonomy.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectExpectedRejection {
    /// Current SELECT clause ordering rejected the statement.
    InvalidClauseOrder,

    /// The SQL unsigned limit literal exceeded its admitted range.
    LimitOverflow,

    /// Accepted-snapshot field resolution rejected an absent field.
    UnknownField,

    /// Function signature validation rejected an unsupported argument type.
    UnsupportedFunctionSignature,

    /// Expression typing rejected an operator/operand mismatch.
    WrongOperatorType,
}

///
/// SelectExpectedOutcome
///
/// Acceptance contract attached to a generated case before SQL rendering.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", content = "reason", rename_all = "snake_case")]
pub enum SelectExpectedOutcome {
    /// The typed query belongs to the current SQL contract.
    Accepted,

    /// The classified invalid mutation must reject with its typed reason.
    Rejected(SelectExpectedRejection),
}

///
/// SelectProvider
///
/// Evidence provider required to judge one generated case.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectProvider {
    /// Typed product rejection is the required invariant.
    RejectionInvariant,

    /// Bundled SQLite supplies the independent result oracle.
    SqliteReference,
}

///
/// SelectFeature
///
/// Typed feature counter emitted from AST facts rather than rendered SQL text.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectFeature {
    /// Aggregate expression projection or predicate input.
    Aggregate,

    /// Aggregate-level duplicate elimination.
    AggregateDistinct,

    /// Aggregate-local `FILTER (WHERE ...)` predicate.
    AggregateFilter,

    /// Projection alias declaration.
    Alias,

    /// Integer arithmetic expression.
    Arithmetic,

    /// Boolean combination or truth test.
    Boolean,

    /// Scalar comparison predicate.
    Comparison,

    /// Query-level scalar duplicate elimination.
    Distinct,

    /// Maintained scalar function call.
    Function,

    /// Explicit grouping key.
    Grouping,

    /// Post-aggregate predicate attached through `HAVING`.
    Having,

    /// Explicit SQL limit.
    Limit,

    /// Membership predicate over a bounded explicit value set.
    Membership,

    /// Maintained numeric scalar function call.
    NumericFunction,

    /// Null predicate or null-producing expression.
    Null,

    /// Explicit SQL offset.
    Offset,

    /// Deterministic order term.
    Ordering,

    /// Predicate attached through `WHERE`.
    Predicate,

    /// Plain scalar projection.
    Projection,

    /// Lower-and-upper-bound predicate.
    Range,

    /// Searched `CASE` expression.
    SearchedCase,

    /// Text transformation or prefix semantics.
    Text,
}

///
/// SelectOrderDirection
///
/// Explicit SQL ordering direction for one typed order target.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SelectOrderDirection {
    /// Ascending canonical value order.
    Ascending,

    /// Descending canonical value order.
    Descending,
}

///
/// SelectResultOrder
///
/// Whether row position participates in generated result comparison.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectResultOrder {
    /// Query order is semantically defined by explicit order terms.
    Ordered,

    /// Rows compare as a canonical typed multiset.
    Unordered,
}

///
/// SelectBudgets
///
/// Deterministic generation, shrink, evaluation, and artifact bounds carried
/// by every generated case and replay record.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[expect(
    clippy::struct_field_names,
    reason = "each field is a distinct hard ceiling and the max prefix is contractual"
)]
pub struct SelectBudgets {
    max_fixture_rows: u32,
    max_expression_depth: u8,
    max_projections: u8,
    max_order_terms: u8,
    max_shrink_candidates: u32,
    max_evaluations: u32,
    max_artifact_bytes: u32,
}

impl SelectBudgets {
    /// Build one explicit bounded generator profile.
    #[must_use]
    pub const fn new(
        max_fixture_rows: u32,
        max_expression_depth: u8,
        max_projections: u8,
        max_order_terms: u8,
        max_shrink_candidates: u32,
        max_evaluations: u32,
        max_artifact_bytes: u32,
    ) -> Self {
        Self {
            max_fixture_rows,
            max_expression_depth,
            max_projections,
            max_order_terms,
            max_shrink_candidates,
            max_evaluations,
            max_artifact_bytes,
        }
    }

    /// Return the fixture-row bound.
    #[must_use]
    pub const fn max_fixture_rows(self) -> u32 {
        self.max_fixture_rows
    }

    /// Return the typed expression-depth bound.
    #[must_use]
    pub const fn max_expression_depth(self) -> u8 {
        self.max_expression_depth
    }

    /// Return the projection-count bound.
    #[must_use]
    pub const fn max_projections(self) -> u8 {
        self.max_projections
    }

    /// Return the order-term bound.
    #[must_use]
    pub const fn max_order_terms(self) -> u8 {
        self.max_order_terms
    }

    /// Return the shrink-candidate attempt bound.
    #[must_use]
    pub const fn max_shrink_candidates(self) -> u32 {
        self.max_shrink_candidates
    }

    /// Return the complete scenario-evaluation bound.
    #[must_use]
    pub const fn max_evaluations(self) -> u32 {
        self.max_evaluations
    }

    /// Return the canonical replay byte bound.
    #[must_use]
    pub const fn max_artifact_bytes(self) -> u32 {
        self.max_artifact_bytes
    }
}

///
/// GeneratedSelectIdentity
///
/// Stable generator version, family, root, case, and derived independent stream.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedSelectIdentity {
    id: String,
    generator_version: u32,
    family_id: String,
    #[serde(with = "tagged_u64")]
    root_seed: u64,
    #[serde(with = "tagged_u64")]
    sub_seed: u64,
    #[serde(with = "tagged_u64")]
    case_index: u64,
}

impl GeneratedSelectIdentity {
    pub(crate) const fn new(
        id: String,
        generator_version: u32,
        family_id: String,
        root_seed: u64,
        sub_seed: u64,
        case_index: u64,
    ) -> Self {
        Self {
            id,
            generator_version,
            family_id,
            root_seed,
            sub_seed,
            case_index,
        }
    }

    /// Borrow the complete stable generated scenario identity.
    #[must_use]
    pub const fn id(&self) -> &str {
        self.id.as_str()
    }

    /// Return the current hard-cut generator version.
    #[must_use]
    pub const fn generator_version(&self) -> u32 {
        self.generator_version
    }

    /// Borrow the independently seeded generator or violation family identity.
    #[must_use]
    pub const fn family_id(&self) -> &str {
        self.family_id.as_str()
    }

    /// Return the configured root seed.
    #[must_use]
    pub const fn root_seed(&self) -> u64 {
        self.root_seed
    }

    /// Return the BLAKE3-derived family/case sub-seed.
    #[must_use]
    pub const fn sub_seed(&self) -> u64 {
        self.sub_seed
    }

    /// Return the case index inside its independent family stream.
    #[must_use]
    pub const fn case_index(&self) -> u64 {
        self.case_index
    }
}

///
/// SelectQueryShape
///
/// Semantic SELECT result family derived from the typed query tree. Execution
/// adapters use this fact without classifying rendered SQL text.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectQueryShape {
    /// Aggregate projection over the whole admitted input.
    GlobalAggregate,

    /// Explicit grouping keys followed by aggregate projections.
    GroupedAggregate,

    /// Non-aggregate scalar row projection, with optional distinctness.
    Scalar,
}

///
/// SelectQuery
///
/// Typed current-contract SELECT tree. Fields remain private so callers cannot
/// bypass type, grouping, depth, alias, or window validation.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SelectQuery {
    distinct: bool,
    projections: Vec<SelectProjection>,
    predicate: Option<SelectPredicate>,
    group_by: Vec<SelectExpression>,
    having: Option<SelectPredicate>,
    order: Vec<SelectOrderTerm>,
    limit: Option<u32>,
    offset: Option<u32>,
}

impl SelectQuery {
    pub(crate) const fn new(
        projections: Vec<SelectProjection>,
        predicate: Option<SelectPredicate>,
        order: Vec<SelectOrderTerm>,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Self {
        Self {
            distinct: false,
            projections,
            predicate,
            group_by: Vec::new(),
            having: None,
            order,
            limit,
            offset,
        }
    }

    pub(crate) const fn distinct(
        projections: Vec<SelectProjection>,
        predicate: Option<SelectPredicate>,
        order: Vec<SelectOrderTerm>,
        limit: Option<u32>,
    ) -> Self {
        Self {
            distinct: true,
            projections,
            predicate,
            group_by: Vec::new(),
            having: None,
            order,
            limit,
            offset: None,
        }
    }

    pub(crate) const fn global_aggregate(
        projections: Vec<SelectProjection>,
        predicate: Option<SelectPredicate>,
        having: Option<SelectPredicate>,
    ) -> Self {
        Self {
            distinct: false,
            projections,
            predicate,
            group_by: Vec::new(),
            having,
            order: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    pub(crate) const fn grouped_aggregate(
        projections: Vec<SelectProjection>,
        predicate: Option<SelectPredicate>,
        group_by: Vec<SelectExpression>,
        having: Option<SelectPredicate>,
        order: Vec<SelectOrderTerm>,
        limit: u32,
    ) -> Self {
        Self {
            distinct: false,
            projections,
            predicate,
            group_by,
            having,
            order,
            limit: Some(limit),
            offset: None,
        }
    }

    /// Return result scalar kinds in projection order.
    ///
    /// # Errors
    ///
    /// Returns a typed case error if the query references inconsistent snapshot facts.
    pub fn projection_kinds(
        &self,
        snapshot: &SelectSnapshot,
    ) -> Result<Vec<SelectValueKind>, SqlGeneratorError> {
        self.projections
            .iter()
            .map(|projection| projection.expression.value_kind(snapshot))
            .collect()
    }

    /// Return the deepest typed expression or predicate node in this query.
    ///
    /// Alias-only order targets contribute no independent depth because their
    /// source projection is already included. Valid queries always return at
    /// least one because projection lists cannot be empty.
    #[must_use]
    pub fn max_expression_depth(&self) -> u8 {
        let projection_depth = self
            .projections
            .iter()
            .map(|projection| projection.expression.depth());
        let predicate_depth = self.predicate.iter().map(SelectPredicate::depth);
        let grouping_depth = self.group_by.iter().map(SelectExpression::depth);
        let having_depth = self.having.iter().map(SelectPredicate::depth);
        let order_depth = self.order.iter().filter_map(|term| match &term.target {
            SelectOrderTarget::Alias(_) => None,
            SelectOrderTarget::Expression(expression) => Some(expression.depth()),
        });

        projection_depth
            .chain(predicate_depth)
            .chain(grouping_depth)
            .chain(having_depth)
            .chain(order_depth)
            .max()
            .unwrap_or_default()
    }

    /// Return the semantic result family derived from grouping and aggregates.
    #[must_use]
    pub fn shape(&self) -> SelectQueryShape {
        if !self.group_by.is_empty() {
            SelectQueryShape::GroupedAggregate
        } else if self.has_aggregate() {
            SelectQueryShape::GlobalAggregate
        } else {
            SelectQueryShape::Scalar
        }
    }

    /// Return whether generated result comparison preserves row position.
    #[must_use]
    pub const fn result_order(&self) -> SelectResultOrder {
        if self.order.is_empty() {
            SelectResultOrder::Unordered
        } else {
            SelectResultOrder::Ordered
        }
    }

    /// Return the explicit row limit, when present.
    #[must_use]
    pub const fn limit(&self) -> Option<u32> {
        self.limit
    }

    /// Return the explicit row offset, when present.
    #[must_use]
    pub const fn offset(&self) -> Option<u32> {
        self.offset
    }

    /// Return whether this query carries a generated predicate.
    #[must_use]
    pub const fn has_predicate(&self) -> bool {
        self.predicate.is_some()
    }

    /// Return the number of explicit order terms.
    #[must_use]
    pub const fn order_term_count(&self) -> usize {
        self.order.len()
    }

    /// Return the number of projected result cells.
    #[must_use]
    pub const fn projection_count(&self) -> usize {
        self.projections.len()
    }

    /// Return whether query-level duplicate elimination is enabled.
    #[must_use]
    pub const fn is_distinct(&self) -> bool {
        self.distinct
    }

    /// Return the number of explicit grouping keys.
    #[must_use]
    pub const fn group_key_count(&self) -> usize {
        self.group_by.len()
    }

    /// Return whether a post-aggregate predicate is present.
    #[must_use]
    pub const fn has_having(&self) -> bool {
        self.having.is_some()
    }

    pub(crate) fn validate(
        &self,
        snapshot: &SelectSnapshot,
        budgets: SelectBudgets,
    ) -> Result<(), SqlGeneratorError> {
        self.validate_bounds(budgets)?;

        let mut aliases = BTreeSet::new();
        for projection in &self.projections {
            projection.expression.validate(snapshot)?;
            if projection.expression.depth() > budgets.max_expression_depth {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::Budget,
                    "generated projection expression exceeds its depth budget",
                ));
            }
            if let Some(alias) = &projection.alias {
                validate_identifier(alias, "projection alias")?;
                if !aliases.insert(alias.as_str()) {
                    return Err(SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        format!("duplicate generated projection alias {alias:?}"),
                    ));
                }
            }
        }
        if let Some(predicate) = &self.predicate {
            predicate.validate(snapshot)?;
            if predicate.contains_aggregate() {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidCase,
                    "generated WHERE predicate cannot contain aggregate expressions",
                ));
            }
            if predicate.depth() > budgets.max_expression_depth {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::Budget,
                    "generated predicate exceeds its depth budget",
                ));
            }
        }
        self.validate_order(snapshot, budgets, &aliases)?;

        for expression in &self.group_by {
            expression.validate(snapshot)?;
            if expression.contains_aggregate() {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidCase,
                    "generated GROUP BY key cannot contain an aggregate expression",
                ));
            }
            if expression.depth() > budgets.max_expression_depth {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::Budget,
                    "generated grouping expression exceeds its depth budget",
                ));
            }
        }

        if let Some(having) = &self.having {
            having.validate(snapshot)?;
            if having.depth() > budgets.max_expression_depth {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::Budget,
                    "generated HAVING predicate exceeds its depth budget",
                ));
            }
        }

        self.validate_aggregate_scope()?;

        Ok(())
    }

    // Keep structural ceilings separate from semantic AST validation so one
    // query cannot consume unbounded test resources before type checks run.
    fn validate_bounds(&self, budgets: SelectBudgets) -> Result<(), SqlGeneratorError> {
        if self.projections.is_empty()
            || self.projections.len() > usize::from(budgets.max_projections)
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "generated SELECT projection count is empty or over budget",
            ));
        }
        if self.order.len() > usize::from(budgets.max_order_terms) {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "generated SELECT order-term count exceeds its budget",
            ));
        }
        if self.group_by.len() > usize::from(budgets.max_projections) {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "generated SELECT grouping-key count exceeds its projection budget",
            ));
        }
        if self.offset.is_some() && self.limit.is_none() {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "valid generated SELECT offset requires a limit",
            ));
        }
        if self.limit == Some(0) {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "valid generated SELECT limit must be positive",
            ));
        }

        Ok(())
    }

    fn validate_order(
        &self,
        snapshot: &SelectSnapshot,
        budgets: SelectBudgets,
        aliases: &BTreeSet<&str>,
    ) -> Result<(), SqlGeneratorError> {
        for term in &self.order {
            match &term.target {
                SelectOrderTarget::Alias(alias) if aliases.contains(alias.as_str()) => {}
                SelectOrderTarget::Alias(alias) => {
                    return Err(SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        format!("generated order target references unknown alias {alias:?}"),
                    ));
                }
                SelectOrderTarget::Expression(expression) => {
                    expression.validate(snapshot)?;
                    if expression.depth() > budgets.max_expression_depth {
                        return Err(SqlGeneratorError::new(
                            SqlGeneratorErrorKind::Budget,
                            "generated order expression exceeds its depth budget",
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_aggregate_scope(&self) -> Result<(), SqlGeneratorError> {
        let shape = self.shape();
        if self.distinct && shape != SelectQueryShape::Scalar {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "generated query-level DISTINCT is limited to scalar SELECT shapes",
            ));
        }
        if shape == SelectQueryShape::Scalar {
            if self.having.is_some() {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidCase,
                    "generated scalar SELECT cannot carry HAVING",
                ));
            }
            return Ok(());
        }

        if shape == SelectQueryShape::GlobalAggregate
            && self
                .projections
                .iter()
                .any(|projection| !projection.expression.contains_aggregate())
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "generated global aggregate projections must be aggregate expressions",
            ));
        } else if shape == SelectQueryShape::GroupedAggregate
            && (self.projections.len() <= self.group_by.len()
                || self
                    .projections
                    .iter()
                    .take(self.group_by.len())
                    .map(SelectProjection::expression)
                    .ne(self.group_by.iter())
                || self
                    .projections
                    .iter()
                    .skip(self.group_by.len())
                    .any(|projection| !projection.expression.contains_aggregate()))
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "generated grouped projection must contain every grouping key first and at least one aggregate",
            ));
        }

        for projection in &self.projections {
            if !projection
                .expression
                .respects_group_scope(self.group_by.as_slice(), false)
            {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidCase,
                    "generated aggregate projection references an ungrouped field",
                ));
            }
        }
        if let Some(having) = &self.having
            && !having.respects_group_scope(self.group_by.as_slice())
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "generated HAVING predicate references an ungrouped field",
            ));
        }
        for term in &self.order {
            if let SelectOrderTarget::Expression(expression) = &term.target
                && !expression.respects_group_scope(self.group_by.as_slice(), false)
            {
                return Err(SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidCase,
                    "generated aggregate ORDER BY expression references an ungrouped field",
                ));
            }
        }

        Ok(())
    }

    pub(crate) const fn projections(&self) -> &[SelectProjection] {
        self.projections.as_slice()
    }

    pub(crate) const fn predicate(&self) -> Option<&SelectPredicate> {
        self.predicate.as_ref()
    }

    pub(crate) const fn group_by(&self) -> &[SelectExpression] {
        self.group_by.as_slice()
    }

    pub(crate) const fn having(&self) -> Option<&SelectPredicate> {
        self.having.as_ref()
    }

    pub(crate) const fn order(&self) -> &[SelectOrderTerm] {
        self.order.as_slice()
    }

    fn has_aggregate(&self) -> bool {
        self.projections
            .iter()
            .any(|projection| projection.expression.contains_aggregate())
            || self
                .having
                .as_ref()
                .is_some_and(SelectPredicate::contains_aggregate)
            || self.order.iter().any(|term| match &term.target {
                SelectOrderTarget::Alias(_) => false,
                SelectOrderTarget::Expression(expression) => expression.contains_aggregate(),
            })
    }

    fn without_fixture_independent_projection(&self, index: usize) -> Option<Self> {
        if self.projections.len() <= 1 || index >= self.projections.len() {
            return None;
        }
        let mut query = self.clone();
        let removed_alias = query.projections[index].alias.clone();
        if removed_alias.as_ref().is_some_and(|alias| {
            query.order.iter().any(
                |term| matches!(&term.target, SelectOrderTarget::Alias(target) if target == alias),
            )
        }) {
            return None;
        }
        query.projections.remove(index);
        Some(query)
    }

    fn without_predicate(&self) -> Option<Self> {
        self.predicate.as_ref()?;
        let mut query = self.clone();
        query.predicate = None;
        Some(query)
    }

    fn without_distinct(&self) -> Option<Self> {
        if !self.distinct {
            return None;
        }
        let mut query = self.clone();
        query.distinct = false;
        Some(query)
    }

    fn without_having(&self) -> Option<Self> {
        self.having.as_ref()?;
        let mut query = self.clone();
        query.having = None;
        Some(query)
    }

    fn without_order_term(&self, index: usize) -> Option<Self> {
        if index >= self.order.len() {
            return None;
        }
        let mut query = self.clone();
        query.order.remove(index);
        Some(query)
    }

    fn without_window(&self) -> Option<Self> {
        if self.limit.is_none() && self.offset.is_none() {
            return None;
        }
        let mut query = self.clone();
        query.limit = None;
        query.offset = None;
        Some(query)
    }

    pub(crate) fn shrink_candidates(&self) -> Vec<Self> {
        let mut candidates = Vec::new();
        for index in 0..self.projections.len() {
            if let Some(candidate) = self.without_fixture_independent_projection(index) {
                candidates.push(candidate);
            }
        }
        if let Some(candidate) = self.without_predicate() {
            candidates.push(candidate);
        }
        if let Some(candidate) = self.without_distinct() {
            candidates.push(candidate);
        }
        if let Some(candidate) = self.without_having() {
            candidates.push(candidate);
        }
        for index in 0..self.order.len() {
            if let Some(candidate) = self.without_order_term(index) {
                candidates.push(candidate);
            }
        }
        if let Some(candidate) = self.without_window() {
            candidates.push(candidate);
        }
        for (index, projection) in self.projections.iter().enumerate() {
            for expression in projection.expression.shrink_candidates() {
                let mut candidate = self.clone();
                candidate.projections[index].expression = expression;
                candidates.push(candidate);
            }
        }
        if let Some(predicate) = &self.predicate {
            for predicate in predicate.shrink_candidates() {
                let mut candidate = self.clone();
                candidate.predicate = Some(predicate);
                candidates.push(candidate);
            }
        }
        if let Some(having) = &self.having {
            for predicate in having.shrink_candidates() {
                let mut candidate = self.clone();
                candidate.having = Some(predicate);
                candidates.push(candidate);
            }
        }
        for (index, term) in self.order.iter().enumerate() {
            if let SelectOrderTarget::Expression(expression) = &term.target {
                for expression in expression.shrink_candidates() {
                    let mut candidate = self.clone();
                    candidate.order[index].target = SelectOrderTarget::Expression(expression);
                    candidates.push(candidate);
                }
            }
        }

        candidates
    }
}

///
/// GeneratedSelectCase
///
/// Complete bounded query, accepted snapshot, fixture, evidence intent, and
/// rendered SQL needed for deterministic execution and replay.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedSelectCase {
    identity: GeneratedSelectIdentity,
    family: SelectGeneratorFamily,
    violation: Option<SelectViolation>,
    snapshot: SelectSnapshot,
    fixture: GeneratedFixture,
    query: SelectQuery,
    rendered_sql: String,
    expected: SelectExpectedOutcome,
    provider: SelectProvider,
    features: BTreeSet<SelectFeature>,
    budgets: SelectBudgets,
}

impl GeneratedSelectCase {
    #[expect(
        clippy::too_many_arguments,
        reason = "the private constructor makes every complete replay fact explicit"
    )]
    pub(crate) const fn new(
        identity: GeneratedSelectIdentity,
        family: SelectGeneratorFamily,
        violation: Option<SelectViolation>,
        snapshot: SelectSnapshot,
        fixture: GeneratedFixture,
        query: SelectQuery,
        rendered_sql: String,
        expected: SelectExpectedOutcome,
        provider: SelectProvider,
        features: BTreeSet<SelectFeature>,
        budgets: SelectBudgets,
    ) -> Self {
        Self {
            identity,
            family,
            violation,
            snapshot,
            fixture,
            query,
            rendered_sql,
            expected,
            provider,
            features,
            budgets,
        }
    }

    /// Borrow stable generated case identity and seed material.
    #[must_use]
    pub const fn identity(&self) -> &GeneratedSelectIdentity {
        &self.identity
    }

    /// Return the valid SELECT family from which this case was generated.
    #[must_use]
    pub const fn family(&self) -> SelectGeneratorFamily {
        self.family
    }

    /// Return the classified invalid mutation, when present.
    #[must_use]
    pub const fn violation(&self) -> Option<SelectViolation> {
        self.violation
    }

    /// Borrow embedded canonical accepted-snapshot material.
    #[must_use]
    pub const fn snapshot(&self) -> &SelectSnapshot {
        &self.snapshot
    }

    /// Borrow the bounded generated fixture.
    #[must_use]
    pub const fn fixture(&self) -> &GeneratedFixture {
        &self.fixture
    }

    /// Borrow the typed valid base query.
    #[must_use]
    pub const fn query(&self) -> &SelectQuery {
        &self.query
    }

    /// Borrow current-contract SQL or the single classified invalid mutation.
    #[must_use]
    pub const fn rendered_sql(&self) -> &str {
        self.rendered_sql.as_str()
    }

    /// Return the acceptance contract attached before rendering.
    #[must_use]
    pub const fn expected(&self) -> SelectExpectedOutcome {
        self.expected
    }

    /// Return the provider required to judge the case.
    #[must_use]
    pub const fn provider(&self) -> SelectProvider {
        self.provider
    }

    /// Borrow typed AST feature counters in stable order.
    #[must_use]
    pub const fn features(&self) -> &BTreeSet<SelectFeature> {
        &self.features
    }

    /// Return the deterministic budgets attached to this case.
    #[must_use]
    pub const fn budgets(&self) -> SelectBudgets {
        self.budgets
    }

    /// Revalidate identity, snapshot, fixture, AST, rendering, and bounds.
    ///
    /// # Errors
    ///
    /// Returns a typed generator error if any embedded replay fact is stale or
    /// inconsistent with the current hard-cut generator version.
    pub fn validate(&self) -> Result<(), SqlGeneratorError> {
        crate::generator::validate_generated_select_case(self)
    }

    pub(crate) fn with_fixture(&self, fixture: GeneratedFixture) -> Self {
        let mut case = self.clone();
        case.fixture = fixture;
        case
    }

    pub(crate) fn with_query(&self, query: SelectQuery) -> Result<Self, SqlGeneratorError> {
        let mut case = self.clone();
        case.query = query;
        case.features = crate::generator::collect_select_features(&case.query);
        case.rendered_sql = crate::generator::render_generated_select_case(
            &case.snapshot,
            &case.query,
            case.violation,
            case.budgets,
        )?;
        case.validate()?;
        Ok(case)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "node", rename_all = "snake_case")]
pub(crate) enum SelectExpression {
    Arithmetic {
        operator: SelectArithmeticOperator,
        left: Box<Self>,
        right: Box<Self>,
    },
    Case {
        condition: Box<SelectPredicate>,
        then_expression: Box<Self>,
        else_expression: Box<Self>,
    },
    /// `COUNT(*)`, `COUNT(expression)`, or `COUNT(DISTINCT expression)`, with
    /// an optional aggregate-local filter.
    Count {
        argument: Option<Box<Self>>,
        distinct: bool,
        filter: Option<Box<SelectPredicate>>,
    },
    Field {
        #[serde(with = "tagged_u32")]
        field_id: u32,
    },
    Function {
        function: SelectFunction,
        arguments: Vec<Self>,
    },
    Literal {
        value: GeneratedValue,
    },
}

impl SelectExpression {
    pub(crate) const fn field(field_id: u32) -> Self {
        Self::Field { field_id }
    }

    pub(crate) const fn literal(value: GeneratedValue) -> Self {
        Self::Literal { value }
    }

    pub(crate) fn value_kind(
        &self,
        snapshot: &SelectSnapshot,
    ) -> Result<SelectValueKind, SqlGeneratorError> {
        match self {
            Self::Field { field_id } => snapshot
                .field_by_id(*field_id)
                .and_then(|field| field.kind.value_kind())
                .ok_or_else(|| {
                    SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        format!("generated expression references ineligible field {field_id}"),
                    )
                }),
            Self::Literal { value } => Ok(value.value_kind()),
            Self::Count {
                argument,
                distinct,
                filter,
            } => {
                if *distinct && argument.is_none() {
                    return Err(SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        "generated COUNT(DISTINCT *) is outside the maintained overlap",
                    ));
                }
                if let Some(argument) = argument {
                    if argument.contains_aggregate() {
                        return Err(SqlGeneratorError::new(
                            SqlGeneratorErrorKind::InvalidCase,
                            "generated aggregate expressions cannot be nested",
                        ));
                    }
                    argument.validate(snapshot)?;
                }
                if let Some(filter) = filter {
                    filter.validate(snapshot)?;
                    if filter.contains_aggregate() {
                        return Err(SqlGeneratorError::new(
                            SqlGeneratorErrorKind::InvalidCase,
                            "generated aggregate FILTER cannot contain aggregate expressions",
                        ));
                    }
                }
                Ok(SelectValueKind::Integer)
            }
            Self::Arithmetic { left, right, .. } => {
                require_expression_kind(left, snapshot, SelectValueKind::Integer)?;
                require_expression_kind(right, snapshot, SelectValueKind::Integer)?;
                Ok(SelectValueKind::Decimal)
            }
            Self::Function {
                function,
                arguments,
            } => function.result_kind(arguments, snapshot),
            Self::Case {
                condition,
                then_expression,
                else_expression,
            } => {
                condition.validate(snapshot)?;
                let then_kind = then_expression.value_kind(snapshot)?;
                let else_kind = else_expression.value_kind(snapshot)?;
                if then_kind != else_kind {
                    return Err(SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        "generated CASE branches have different scalar types",
                    ));
                }
                Ok(then_kind)
            }
        }
    }

    pub(crate) fn validate(&self, snapshot: &SelectSnapshot) -> Result<(), SqlGeneratorError> {
        self.value_kind(snapshot).map(|_| ())
    }

    pub(crate) fn depth(&self) -> u8 {
        match self {
            Self::Field { .. } | Self::Literal { .. } => 1,
            Self::Count {
                argument, filter, ..
            } => 1_u8.saturating_add(
                argument
                    .as_deref()
                    .map_or(0, Self::depth)
                    .max(filter.as_deref().map_or(0, SelectPredicate::depth)),
            ),
            Self::Arithmetic { left, right, .. } => {
                1_u8.saturating_add(left.depth().max(right.depth()))
            }
            Self::Function { arguments, .. } => {
                1_u8.saturating_add(arguments.iter().map(Self::depth).max().unwrap_or_default())
            }
            Self::Case {
                condition,
                then_expression,
                else_expression,
            } => 1_u8.saturating_add(
                condition
                    .depth()
                    .max(then_expression.depth())
                    .max(else_expression.depth()),
            ),
        }
    }

    fn contains_aggregate(&self) -> bool {
        match self {
            Self::Count { .. } => true,
            Self::Arithmetic { left, right, .. } => {
                left.contains_aggregate() || right.contains_aggregate()
            }
            Self::Case {
                condition,
                then_expression,
                else_expression,
            } => {
                condition.contains_aggregate()
                    || then_expression.contains_aggregate()
                    || else_expression.contains_aggregate()
            }
            Self::Function { arguments, .. } => arguments.iter().any(Self::contains_aggregate),
            Self::Field { .. } | Self::Literal { .. } => false,
        }
    }

    fn respects_group_scope(&self, group_by: &[Self], inside_aggregate: bool) -> bool {
        match self {
            Self::Count { .. } => !inside_aggregate,
            Self::Field { .. } => inside_aggregate || group_by.contains(self),
            Self::Literal { .. } => true,
            Self::Arithmetic { left, right, .. } => {
                left.respects_group_scope(group_by, inside_aggregate)
                    && right.respects_group_scope(group_by, inside_aggregate)
            }
            Self::Function { arguments, .. } => arguments
                .iter()
                .all(|argument| argument.respects_group_scope(group_by, inside_aggregate)),
            Self::Case {
                condition,
                then_expression,
                else_expression,
            } => {
                condition.respects_group_scope(group_by)
                    && then_expression.respects_group_scope(group_by, inside_aggregate)
                    && else_expression.respects_group_scope(group_by, inside_aggregate)
            }
        }
    }

    fn shrink_candidates(&self) -> Vec<Self> {
        let mut candidates = Vec::new();
        match self {
            Self::Arithmetic {
                operator,
                left,
                right,
            } => {
                candidates.push((**left).clone());
                candidates.push((**right).clone());
                for candidate in left.shrink_candidates() {
                    candidates.push(Self::Arithmetic {
                        operator: *operator,
                        left: Box::new(candidate),
                        right: right.clone(),
                    });
                }
                for candidate in right.shrink_candidates() {
                    candidates.push(Self::Arithmetic {
                        operator: *operator,
                        left: left.clone(),
                        right: Box::new(candidate),
                    });
                }
            }
            Self::Function {
                function,
                arguments,
            } => {
                candidates.extend(arguments.iter().cloned());
                for (index, argument) in arguments.iter().enumerate() {
                    for candidate in argument.shrink_candidates() {
                        let mut simplified_arguments = arguments.clone();
                        simplified_arguments[index] = candidate;
                        candidates.push(Self::Function {
                            function: *function,
                            arguments: simplified_arguments,
                        });
                    }
                }
            }
            Self::Count {
                argument,
                distinct,
                filter,
            } => {
                if let Some(argument) = argument {
                    for candidate in argument.shrink_candidates() {
                        candidates.push(Self::Count {
                            argument: Some(Box::new(candidate)),
                            distinct: *distinct,
                            filter: filter.clone(),
                        });
                    }
                    if *distinct {
                        candidates.push(Self::Count {
                            argument: argument.clone().into(),
                            distinct: false,
                            filter: filter.clone(),
                        });
                    }
                }
                if let Some(filter) = filter {
                    candidates.push(Self::Count {
                        argument: argument.clone(),
                        distinct: *distinct,
                        filter: None,
                    });
                    for candidate in filter.shrink_candidates() {
                        candidates.push(Self::Count {
                            argument: argument.clone(),
                            distinct: *distinct,
                            filter: Some(Box::new(candidate)),
                        });
                    }
                }
            }
            Self::Case {
                condition,
                then_expression,
                else_expression,
            } => {
                candidates.push((**then_expression).clone());
                candidates.push((**else_expression).clone());
                for candidate in condition.shrink_candidates() {
                    candidates.push(Self::Case {
                        condition: Box::new(candidate),
                        then_expression: then_expression.clone(),
                        else_expression: else_expression.clone(),
                    });
                }
            }
            Self::Literal { value } => {
                candidates.extend(value.shrink_candidates().into_iter().map(Self::literal));
            }
            Self::Field { .. } => {}
        }

        candidates
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SelectArithmeticOperator {
    Add,
    Subtract,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SelectFunction {
    Abs,
    Coalesce,
    Length,
    Lower,
    NullIf,
    Upper,
}

impl SelectFunction {
    fn result_kind(
        self,
        arguments: &[SelectExpression],
        snapshot: &SelectSnapshot,
    ) -> Result<SelectValueKind, SqlGeneratorError> {
        match self {
            Self::Abs => {
                require_arity(arguments, 1, self)?;
                let kind = arguments[0].value_kind(snapshot)?;
                if !matches!(kind, SelectValueKind::Decimal | SelectValueKind::Integer) {
                    return Err(function_error(self, "requires one numeric argument"));
                }
                Ok(SelectValueKind::Decimal)
            }
            Self::Lower | Self::Upper => {
                require_arity(arguments, 1, self)?;
                require_expression_kind(&arguments[0], snapshot, SelectValueKind::Text)?;
                Ok(SelectValueKind::Text)
            }
            Self::Length => {
                require_arity(arguments, 1, self)?;
                require_expression_kind(&arguments[0], snapshot, SelectValueKind::Text)?;
                Ok(SelectValueKind::Integer)
            }
            Self::NullIf => {
                require_arity(arguments, 2, self)?;
                let left = arguments[0].value_kind(snapshot)?;
                let right = arguments[1].value_kind(snapshot)?;
                if left != right {
                    return Err(function_error(self, "arguments must share one scalar type"));
                }
                Ok(left)
            }
            Self::Coalesce => {
                if arguments.len() < 2 {
                    return Err(function_error(self, "requires at least two arguments"));
                }
                let kind = arguments[0].value_kind(snapshot)?;
                for argument in &arguments[1..] {
                    if argument.value_kind(snapshot)? != kind {
                        return Err(function_error(self, "arguments must share one scalar type"));
                    }
                }
                Ok(kind)
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "node", rename_all = "snake_case")]
pub(crate) enum SelectPredicate {
    And {
        left: Box<Self>,
        right: Box<Self>,
    },
    Between {
        expression: SelectExpression,
        lower: SelectExpression,
        upper: SelectExpression,
        negated: bool,
    },
    Comparison {
        operator: SelectComparisonOperator,
        left: SelectExpression,
        right: SelectExpression,
    },
    InList {
        expression: SelectExpression,
        members: Vec<SelectExpression>,
        negated: bool,
    },
    IsNull {
        expression: SelectExpression,
        negated: bool,
    },
    IsTruth {
        expression: SelectExpression,
        expected: bool,
        negated: bool,
    },
    Not {
        predicate: Box<Self>,
    },
    Or {
        left: Box<Self>,
        right: Box<Self>,
    },
    PrefixLike {
        expression: SelectExpression,
        prefix: String,
        case_insensitive: bool,
        negated: bool,
    },
    StartsWith {
        value: SelectExpression,
        prefix: SelectExpression,
    },
}

impl SelectPredicate {
    pub(crate) fn validate(&self, snapshot: &SelectSnapshot) -> Result<(), SqlGeneratorError> {
        match self {
            Self::And { left, right } | Self::Or { left, right } => {
                left.validate(snapshot)?;
                right.validate(snapshot)
            }
            Self::Between {
                expression,
                lower,
                upper,
                ..
            } => {
                let expression_kind = expression.value_kind(snapshot)?;
                let lower_kind = lower.value_kind(snapshot)?;
                let upper_kind = upper.value_kind(snapshot)?;
                if expression_kind != lower_kind || expression_kind != upper_kind {
                    return Err(SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        "generated BETWEEN operands have different scalar types",
                    ));
                }
                Ok(())
            }
            Self::Not { predicate } => predicate.validate(snapshot),
            Self::Comparison { left, right, .. } => {
                let left_kind = left.value_kind(snapshot)?;
                let right_kind = right.value_kind(snapshot)?;
                if left_kind != right_kind {
                    return Err(SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        "generated comparison operands have different scalar types",
                    ));
                }
                Ok(())
            }
            Self::InList {
                expression,
                members,
                ..
            } => {
                if members.is_empty() || members.len() > MAX_GENERATED_MEMBERSHIP_MEMBERS {
                    return Err(SqlGeneratorError::new(
                        SqlGeneratorErrorKind::Budget,
                        "generated membership list is empty or over budget",
                    ));
                }
                let expression_kind = expression.value_kind(snapshot)?;
                for member in members {
                    if member.value_kind(snapshot)? != expression_kind {
                        return Err(SqlGeneratorError::new(
                            SqlGeneratorErrorKind::InvalidCase,
                            "generated membership operands have different scalar types",
                        ));
                    }
                }
                Ok(())
            }
            Self::IsNull { expression, .. } => expression.validate(snapshot),
            Self::IsTruth { expression, .. } => {
                require_expression_kind(expression, snapshot, SelectValueKind::Boolean)
            }
            Self::PrefixLike { expression, .. } => {
                require_expression_kind(expression, snapshot, SelectValueKind::Text)
            }
            Self::StartsWith { value, prefix } => {
                require_expression_kind(value, snapshot, SelectValueKind::Text)?;
                require_expression_kind(prefix, snapshot, SelectValueKind::Text)
            }
        }
    }

    pub(crate) fn depth(&self) -> u8 {
        match self {
            Self::And { left, right } | Self::Or { left, right } => {
                1_u8.saturating_add(left.depth().max(right.depth()))
            }
            Self::Between {
                expression,
                lower,
                upper,
                ..
            } => 1_u8.saturating_add(expression.depth().max(lower.depth()).max(upper.depth())),
            Self::Not { predicate } => 1_u8.saturating_add(predicate.depth()),
            Self::Comparison { left, right, .. } => {
                1_u8.saturating_add(left.depth().max(right.depth()))
            }
            Self::InList {
                expression,
                members,
                ..
            } => 1_u8.saturating_add(
                expression.depth().max(
                    members
                        .iter()
                        .map(SelectExpression::depth)
                        .max()
                        .unwrap_or_default(),
                ),
            ),
            Self::IsNull { expression, .. }
            | Self::IsTruth { expression, .. }
            | Self::PrefixLike { expression, .. } => 1_u8.saturating_add(expression.depth()),
            Self::StartsWith { value, prefix } => {
                1_u8.saturating_add(value.depth().max(prefix.depth()))
            }
        }
    }

    fn contains_aggregate(&self) -> bool {
        match self {
            Self::And { left, right } | Self::Or { left, right } => {
                left.contains_aggregate() || right.contains_aggregate()
            }
            Self::Between {
                expression,
                lower,
                upper,
                ..
            } => {
                expression.contains_aggregate()
                    || lower.contains_aggregate()
                    || upper.contains_aggregate()
            }
            Self::Not { predicate } => predicate.contains_aggregate(),
            Self::Comparison { left, right, .. } => {
                left.contains_aggregate() || right.contains_aggregate()
            }
            Self::InList {
                expression,
                members,
                ..
            } => {
                expression.contains_aggregate()
                    || members.iter().any(SelectExpression::contains_aggregate)
            }
            Self::IsNull { expression, .. }
            | Self::IsTruth { expression, .. }
            | Self::PrefixLike { expression, .. } => expression.contains_aggregate(),
            Self::StartsWith { value, prefix } => {
                value.contains_aggregate() || prefix.contains_aggregate()
            }
        }
    }

    fn respects_group_scope(&self, group_by: &[SelectExpression]) -> bool {
        match self {
            Self::And { left, right } | Self::Or { left, right } => {
                left.respects_group_scope(group_by) && right.respects_group_scope(group_by)
            }
            Self::Between {
                expression,
                lower,
                upper,
                ..
            } => {
                expression.respects_group_scope(group_by, false)
                    && lower.respects_group_scope(group_by, false)
                    && upper.respects_group_scope(group_by, false)
            }
            Self::Not { predicate } => predicate.respects_group_scope(group_by),
            Self::Comparison { left, right, .. } => {
                left.respects_group_scope(group_by, false)
                    && right.respects_group_scope(group_by, false)
            }
            Self::InList {
                expression,
                members,
                ..
            } => {
                expression.respects_group_scope(group_by, false)
                    && members
                        .iter()
                        .all(|member| member.respects_group_scope(group_by, false))
            }
            Self::IsNull { expression, .. }
            | Self::IsTruth { expression, .. }
            | Self::PrefixLike { expression, .. } => {
                expression.respects_group_scope(group_by, false)
            }
            Self::StartsWith { value, prefix } => {
                value.respects_group_scope(group_by, false)
                    && prefix.respects_group_scope(group_by, false)
            }
        }
    }

    // Keep shrink behavior exhaustive beside the closed predicate AST so new
    // variants cannot silently miss minimization.
    #[expect(
        clippy::too_many_lines,
        reason = "closed predicate AST shrink policy is intentionally exhaustive"
    )]
    fn shrink_candidates(&self) -> Vec<Self> {
        let mut candidates = Vec::new();
        match self {
            Self::And { left, right } | Self::Or { left, right } => {
                candidates.push((**left).clone());
                candidates.push((**right).clone());
            }
            Self::Between {
                expression,
                lower,
                upper,
                negated,
            } => {
                for candidate in expression.shrink_candidates() {
                    candidates.push(Self::Between {
                        expression: candidate,
                        lower: lower.clone(),
                        upper: upper.clone(),
                        negated: *negated,
                    });
                }
                for candidate in lower.shrink_candidates() {
                    candidates.push(Self::Between {
                        expression: expression.clone(),
                        lower: candidate,
                        upper: upper.clone(),
                        negated: *negated,
                    });
                }
                for candidate in upper.shrink_candidates() {
                    candidates.push(Self::Between {
                        expression: expression.clone(),
                        lower: lower.clone(),
                        upper: candidate,
                        negated: *negated,
                    });
                }
                if *negated {
                    candidates.push(Self::Between {
                        expression: expression.clone(),
                        lower: lower.clone(),
                        upper: upper.clone(),
                        negated: false,
                    });
                }
            }
            Self::Not { predicate } => candidates.push((**predicate).clone()),
            Self::Comparison {
                operator,
                left,
                right,
            } => {
                for candidate in left.shrink_candidates() {
                    candidates.push(Self::Comparison {
                        operator: *operator,
                        left: candidate,
                        right: right.clone(),
                    });
                }
                for candidate in right.shrink_candidates() {
                    candidates.push(Self::Comparison {
                        operator: *operator,
                        left: left.clone(),
                        right: candidate,
                    });
                }
            }
            Self::IsNull {
                expression,
                negated,
            } => {
                for candidate in expression.shrink_candidates() {
                    candidates.push(Self::IsNull {
                        expression: candidate,
                        negated: *negated,
                    });
                }
            }
            Self::InList {
                expression,
                members,
                negated,
            } => {
                for candidate in expression.shrink_candidates() {
                    candidates.push(Self::InList {
                        expression: candidate,
                        members: members.clone(),
                        negated: *negated,
                    });
                }
                for (index, member) in members.iter().enumerate() {
                    for candidate in member.shrink_candidates() {
                        let mut simplified_members = members.clone();
                        simplified_members[index] = candidate;
                        candidates.push(Self::InList {
                            expression: expression.clone(),
                            members: simplified_members,
                            negated: *negated,
                        });
                    }
                }
                if members.len() > 1 {
                    candidates.push(Self::InList {
                        expression: expression.clone(),
                        members: members[..members.len() - 1].to_vec(),
                        negated: *negated,
                    });
                }
                if *negated {
                    candidates.push(Self::InList {
                        expression: expression.clone(),
                        members: members.clone(),
                        negated: false,
                    });
                }
            }
            Self::IsTruth {
                expression,
                expected,
                negated,
            } => {
                for candidate in expression.shrink_candidates() {
                    candidates.push(Self::IsTruth {
                        expression: candidate,
                        expected: *expected,
                        negated: *negated,
                    });
                }
            }
            Self::PrefixLike {
                expression,
                prefix,
                case_insensitive,
                negated,
            } => {
                for candidate in expression.shrink_candidates() {
                    candidates.push(Self::PrefixLike {
                        expression: candidate,
                        prefix: prefix.clone(),
                        case_insensitive: *case_insensitive,
                        negated: *negated,
                    });
                }
                if !prefix.is_empty() {
                    candidates.push(Self::PrefixLike {
                        expression: expression.clone(),
                        prefix: String::new(),
                        case_insensitive: *case_insensitive,
                        negated: *negated,
                    });
                }
            }
            Self::StartsWith { value, prefix } => {
                for candidate in value.shrink_candidates() {
                    candidates.push(Self::StartsWith {
                        value: candidate,
                        prefix: prefix.clone(),
                    });
                }
                for candidate in prefix.shrink_candidates() {
                    candidates.push(Self::StartsWith {
                        value: value.clone(),
                        prefix: candidate,
                    });
                }
            }
        }

        candidates
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SelectComparisonOperator {
    Equal,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
    NotEqual,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SelectProjection {
    expression: SelectExpression,
    alias: Option<String>,
}

impl SelectProjection {
    pub(crate) fn new(expression: SelectExpression, alias: Option<&str>) -> Self {
        Self {
            expression,
            alias: alias.map(str::to_string),
        }
    }

    pub(crate) const fn expression(&self) -> &SelectExpression {
        &self.expression
    }

    pub(crate) fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SelectOrderTerm {
    target: SelectOrderTarget,
    direction: SelectOrderDirection,
}

impl SelectOrderTerm {
    pub(crate) const fn expression(
        expression: SelectExpression,
        direction: SelectOrderDirection,
    ) -> Self {
        Self {
            target: SelectOrderTarget::Expression(expression),
            direction,
        }
    }

    pub(crate) fn alias(alias: &str, direction: SelectOrderDirection) -> Self {
        Self {
            target: SelectOrderTarget::Alias(alias.to_string()),
            direction,
        }
    }

    pub(crate) const fn target(&self) -> &SelectOrderTarget {
        &self.target
    }

    pub(crate) const fn direction(&self) -> SelectOrderDirection {
        self.direction
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub(crate) enum SelectOrderTarget {
    Alias(String),
    Expression(SelectExpression),
}

fn validate_snapshot_fields(fields: &[SelectField]) -> Result<(), SqlGeneratorError> {
    if fields.is_empty() || !fields.iter().any(SelectField::primary_key) {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidSnapshot,
            "SELECT snapshot needs fields and accepted primary-key identity",
        ));
    }
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    for field in fields {
        if field.id == 0 || !ids.insert(field.id) {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidSnapshot,
                format!("duplicate or zero accepted field identity {}", field.id),
            ));
        }
        validate_identifier(&field.name, "snapshot field")?;
        if !names.insert(field.name.as_str()) {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidSnapshot,
                format!("duplicate accepted field name {:?}", field.name),
            ));
        }
    }
    for required in [
        SelectFieldKind::Boolean,
        SelectFieldKind::Integer,
        SelectFieldKind::Text,
    ] {
        if !fields
            .iter()
            .any(|field| field.kind == required && !field.primary_key && !field.generated)
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidSnapshot,
                format!("SELECT snapshot lacks a generated-query {required:?} field"),
            ));
        }
    }
    if fields
        .iter()
        .filter(|field| {
            field.kind == SelectFieldKind::Integer && !field.primary_key && !field.generated
        })
        .count()
        < 2
    {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidSnapshot,
            "SELECT snapshot needs two non-key integer fields for field comparisons",
        ));
    }

    Ok(())
}

fn validate_snapshot_indexes(
    fields: &[SelectField],
    indexes: &[SelectIndex],
) -> Result<(), SqlGeneratorError> {
    let field_ids = fields.iter().map(|field| field.id).collect::<BTreeSet<_>>();
    let mut index_ids = BTreeSet::new();
    let mut index_names = BTreeSet::new();
    for index in indexes {
        if index.id == 0 || !index_ids.insert(index.id) || index.field_ids.is_empty() {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidSnapshot,
                format!("invalid or duplicate accepted index identity {}", index.id),
            ));
        }
        validate_identifier(&index.name, "snapshot index")?;
        if !index_names.insert(index.name.as_str())
            || index
                .field_ids
                .iter()
                .any(|field_id| !field_ids.contains(field_id))
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidSnapshot,
                format!(
                    "accepted index {:?} has duplicate name or unknown field",
                    index.name
                ),
            ));
        }
    }

    Ok(())
}

pub(crate) fn validate_identifier(
    identifier: &str,
    context: &str,
) -> Result<(), SqlGeneratorError> {
    let mut bytes = identifier.bytes();
    let Some(first) = bytes.next() else {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidSnapshot,
            format!("{context} identifier is empty"),
        ));
    };
    if !(first.is_ascii_alphabetic() || first == b'_')
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidSnapshot,
            format!("{context} identifier {identifier:?} is outside the current SQL grammar"),
        ));
    }

    Ok(())
}

fn require_arity(
    arguments: &[SelectExpression],
    expected: usize,
    function: SelectFunction,
) -> Result<(), SqlGeneratorError> {
    if arguments.len() == expected {
        Ok(())
    } else {
        Err(function_error(
            function,
            &format!("requires {expected} arguments"),
        ))
    }
}

fn require_expression_kind(
    expression: &SelectExpression,
    snapshot: &SelectSnapshot,
    expected: SelectValueKind,
) -> Result<(), SqlGeneratorError> {
    let actual = expression.value_kind(snapshot)?;
    if actual == expected {
        Ok(())
    } else {
        Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            format!("generated expression has {actual:?}, expected {expected:?}"),
        ))
    }
}

fn function_error(function: SelectFunction, detail: &str) -> SqlGeneratorError {
    SqlGeneratorError::new(
        SqlGeneratorErrorKind::InvalidCase,
        format!("generated {function:?} function {detail}"),
    )
}

pub(crate) mod tagged_u32 {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    #[expect(
        clippy::trivially_copy_pass_by_ref,
        reason = "Serde with-module serializers receive borrowed field values"
    )]
    pub(crate) fn serialize<S>(value: &u32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("field:{value:08x}"))
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<u32, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tagged = String::deserialize(deserializer)?;
        u32::from_str_radix(
            tagged
                .strip_prefix("field:")
                .ok_or_else(|| D::Error::custom("expected field: tagged identity"))?,
            16,
        )
        .map_err(D::Error::custom)
    }
}

mod tagged_u16 {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    #[expect(
        clippy::trivially_copy_pass_by_ref,
        reason = "Serde with-module serializers receive borrowed field values"
    )]
    pub(super) fn serialize<S>(value: &u16, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("index:{value:04x}"))
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<u16, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tagged = String::deserialize(deserializer)?;
        u16::from_str_radix(
            tagged
                .strip_prefix("index:")
                .ok_or_else(|| D::Error::custom("expected index: tagged identity"))?,
            16,
        )
        .map_err(D::Error::custom)
    }
}

mod tagged_u32_vec {
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

    pub(super) fn serialize<S>(values: &[u32], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        values
            .iter()
            .map(|value| format!("field:{value:08x}"))
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<String>::deserialize(deserializer)?
            .into_iter()
            .map(|tagged| {
                u32::from_str_radix(
                    tagged
                        .strip_prefix("field:")
                        .ok_or_else(|| D::Error::custom("expected field: tagged identity"))?,
                    16,
                )
                .map_err(D::Error::custom)
            })
            .collect()
    }
}

pub(crate) mod tagged_u64 {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    #[expect(
        clippy::trivially_copy_pass_by_ref,
        reason = "Serde with-module serializers receive borrowed field values"
    )]
    pub(crate) fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("u64:{value:016x}"))
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tagged = String::deserialize(deserializer)?;
        u64::from_str_radix(
            tagged
                .strip_prefix("u64:")
                .ok_or_else(|| D::Error::custom("expected u64: tagged integer"))?,
            16,
        )
        .map_err(D::Error::custom)
    }
}
