//! Module: db::query::plan::expr::projection
//! Defines the planner-owned projection selection and projection field shapes
//! that flow into structural execution.

use crate::{
    db::{
        query::plan::expr::ast::{Alias, BinaryOp, Expr, FieldId},
        schema::SchemaInfo,
    },
    error::InternalError,
    model::{entity::EntityModel, field::FieldModel},
    value::Value,
};

///
/// ProjectionSelection
///
/// Planner-owned projection selection contract for scalar query shapes.
/// `All` projects the full entity model field list.
/// `Fields` projects one explicit field subset in declaration order.
/// Invariant: projection order is planner-authoritative and must remain stable
/// through executor/materialization boundaries.
///
#[cfg_attr(
    all(not(test), not(feature = "sql")),
    expect(
        dead_code,
        reason = "SQL lowering constructs explicit field and expression projections; no-default fluent queries use full-model projection"
    )
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ProjectionSelection {
    All,
    Fields(Vec<FieldId>),
    Exprs(Vec<ProjectionField>),
}

///
/// ProjectionField
///
/// One canonical projection output field in declaration order.
/// This remains planner-owned semantic shape and is executor-agnostic.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ProjectionField {
    Scalar { expr: Expr, alias: Option<Alias> },
}

///
/// ProjectionSpec
///
/// Canonical projection semantic contract emitted by planner.
/// Construction remains planner-only; consumers borrow read-only views.
/// Invariant: `fields` order is canonical output order and must not be
/// reordered by executor/output layers.
///

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct ProjectionSpec {
    fields: Vec<ProjectionField>,
}

impl ProjectionSpec {
    /// Build one projection semantic contract from planner-lowered fields.
    #[must_use]
    pub(in crate::db::query::plan) const fn new(fields: Vec<ProjectionField>) -> Self {
        Self { fields }
    }

    /// Build one projection semantic contract for tests outside planner modules.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn from_fields_for_test(fields: Vec<ProjectionField>) -> Self {
        Self::new(fields)
    }

    /// Return the declared output field count.
    #[must_use]
    pub(in crate::db) const fn len(&self) -> usize {
        self.fields.len()
    }

    /// Borrow declared projection fields in canonical order.
    pub(in crate::db) fn fields(&self) -> std::slice::Iter<'_, ProjectionField> {
        self.fields.iter()
    }

    /// Return whether this projection preserves the model's canonical identity
    /// field order without aliases or computed expressions.
    #[must_use]
    pub(in crate::db) fn is_model_identity_for(&self, model: &EntityModel) -> bool {
        if self.len() != model.fields().len() {
            return false;
        }

        for (field_model, projected_field) in model.fields().iter().zip(self.fields()) {
            if !projected_field.is_identity_field_projection(field_model) {
                return false;
            }
        }

        true
    }

    /// Return referenced slots using the caller-selected schema authority.
    pub(in crate::db) fn referenced_slots_for_schema(
        &self,
        _model: &EntityModel,
        schema: &SchemaInfo,
    ) -> Result<Vec<usize>, InternalError> {
        let mut referenced = Vec::new();

        for field in self.fields() {
            mark_projection_expr_slots(schema, field.expr(), &mut referenced)?;
        }

        referenced.sort_unstable();

        Ok(referenced)
    }
}

impl ProjectionField {
    /// Borrow the canonical expression owned by this projection field.
    #[must_use]
    pub(in crate::db) const fn expr(&self) -> &Expr {
        match self {
            Self::Scalar { expr, .. } => expr,
        }
    }

    /// Return one direct projected field name when this output stays on one
    /// field leaf under optional alias wrappers.
    #[must_use]
    pub(in crate::db) fn direct_field_name(&self) -> Option<&str> {
        direct_projection_expr_field_name(self.expr())
    }

    // Identity projection stays on the direct field leaf with no alias or
    // computed wrapper, and must preserve the model-declared field name.
    fn is_identity_field_projection(&self, field_model: &FieldModel) -> bool {
        match self {
            Self::Scalar {
                expr: Expr::Field(field_id),
                alias: None,
            } => field_id.as_str() == field_model.name(),
            Self::Scalar { .. } => false,
        }
    }
}

// Walk one canonical projection expression and mark every referenced field slot
// against the resolved model layout. This stays on the projection boundary so
// static-planning consumers do not open-code expression slot scans locally.
fn mark_projection_expr_slots(
    schema: &SchemaInfo,
    expr: &Expr,
    referenced: &mut Vec<usize>,
) -> Result<(), InternalError> {
    expr.try_for_each_tree_expr(&mut |node| match node {
        Expr::Field(field_id) => {
            let field_name = field_id.as_str();
            let slot = schema
                .field_slot_index(field_name)
                .ok_or_else(InternalError::query_invalid_logical_plan)?;
            if !referenced.contains(&slot) {
                referenced.push(slot);
            }
            Ok(())
        }
        Expr::FieldPath(path) => {
            let field_name = path.root().as_str();
            let slot = schema
                .field_slot_index(field_name)
                .ok_or_else(InternalError::query_invalid_logical_plan)?;
            if !referenced.contains(&slot) {
                referenced.push(slot);
            }
            Ok(())
        }
        _ => Ok(()),
    })
}

/// Return one direct field name when the expression is only a field leaf plus
/// optional alias wrappers.
#[must_use]
#[cfg_attr(
    not(test),
    expect(
        clippy::missing_const_for_fn,
        reason = "test-only alias traversal keeps the shared helper non-const across the full target matrix"
    )
)]
pub(in crate::db) fn direct_projection_expr_field_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Field(field) => Some(field.as_str()),
        #[cfg(test)]
        Expr::Alias { expr, .. } => direct_projection_expr_field_name(expr.as_ref()),
        Expr::Unary { .. } => None,
        Expr::FieldPath(_)
        | Expr::Literal(_)
        | Expr::FunctionCall { .. }
        | Expr::Aggregate(_)
        | Expr::Case { .. }
        | Expr::Binary { .. } => None,
    }
}

/// Resolve one unique direct field-slot layout using explicit schema authority.
#[must_use]
pub(in crate::db::query) fn collect_unique_direct_projection_slots_with_schema<'a>(
    schema: &SchemaInfo,
    field_names: impl IntoIterator<Item = &'a str>,
) -> Option<Vec<usize>> {
    let mut field_slots = Vec::new();

    for field_name in field_names {
        let slot = schema.field_slot_index(field_name)?;
        if field_slots.contains(&slot) {
            return None;
        }

        field_slots.push(slot);
    }

    Some(field_slots)
}

///
/// GroupedOrderExprClass
///
/// Classifies the small grouped `ORDER BY` expression family that the planner
/// can prove preserves canonical grouped-key order in the current grouped
/// execution model. This stays intentionally narrower than the broader scalar
/// computed-order surface because grouped pagination still resumes on grouped
/// keys rather than on arbitrary computed order values.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum GroupedOrderExprClass {
    CanonicalGroupField,
    GroupFieldPlusConstant,
    GroupFieldMinusConstant,
}

///
/// GroupedOrderTermAdmissibility
///
/// One planner-local admission result for a grouped `ORDER BY` term against
/// one expected grouped key. The grouped cursor validator uses this to keep
/// plain prefix mismatch separate from expressions that parse and evaluate but
/// still are not order-admissible under the grouped boundedness contract.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum GroupedOrderTermAdmissibility {
    Preserves(GroupedOrderExprClass),
    PrefixMismatch,
    UnsupportedExpression,
}

///
/// GroupedTopKOrderTermAdmissibility
///
/// Planner-local grouped Top-K admission result for one `ORDER BY` term.
/// This keeps the `0.88` aggregate-order lane explicit without widening the
/// narrow canonical grouped-key proof helper into a catch-all classifier.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum GroupedTopKOrderTermAdmissibility {
    Admissible,
    NonGroupFieldReference,
    UnsupportedExpression,
}

///
/// GroupedCanonicalOrderShape
///
/// One local grouped canonical-order proof shape for one already-parsed
/// expression.
/// This exists so canonical grouped-key prefix proof and broader grouped Top-K
/// admission can read one shared analysis result instead of reclassifying the
/// same expression separately.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupedCanonicalOrderShape {
    CanonicalGroupField,
    GroupFieldPlusConstant,
    GroupFieldMinusConstant,
    OtherField,
    OtherFieldOffset,
    Unsupported,
}

///
/// GroupedOrderExprAnalysis
///
/// One shared grouped-order proof summary for one already-parsed planner
/// expression.
/// This exists so canonical grouped-key validation, grouped Top-K admission,
/// and grouped heap selection all read one recursive ownership seam.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GroupedOrderExprAnalysis {
    canonical_shape: GroupedCanonicalOrderShape,
    flags: GroupedOrderExprFlags,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GroupedOrderExprFlags {
    bits: u8,
}

impl GroupedOrderExprFlags {
    const REFERENCES_ONLY_GROUP_FIELDS: u8 = 1 << 0;
    const CONTAINS_AGGREGATE: u8 = 1 << 1;
    const CONTAINS_CASE: u8 = 1 << 2;
    const CONTAINS_NON_AGGREGATE_WRAPPER_FN: u8 = 1 << 3;

    const fn field_reference(is_group_field: bool) -> Self {
        if is_group_field {
            Self::group_field_only()
        } else {
            Self::empty()
        }
    }

    const fn empty() -> Self {
        Self { bits: 0 }
    }

    const fn group_field_only() -> Self {
        Self {
            bits: Self::REFERENCES_ONLY_GROUP_FIELDS,
        }
    }

    const fn aggregate() -> Self {
        Self {
            bits: Self::REFERENCES_ONLY_GROUP_FIELDS | Self::CONTAINS_AGGREGATE,
        }
    }

    const fn with_case(self) -> Self {
        Self {
            bits: self.bits | Self::CONTAINS_CASE,
        }
    }

    const fn with_non_aggregate_wrapper_fn(self) -> Self {
        Self {
            bits: self.bits | Self::CONTAINS_NON_AGGREGATE_WRAPPER_FN,
        }
    }

    const fn merge_with(self, other: Self) -> Self {
        let mut bits = (self.bits | other.bits) & !Self::REFERENCES_ONLY_GROUP_FIELDS;
        if self.references_only_group_fields() && other.references_only_group_fields() {
            bits |= Self::REFERENCES_ONLY_GROUP_FIELDS;
        }

        Self { bits }
    }

    const fn references_only_group_fields(self) -> bool {
        self.bits & Self::REFERENCES_ONLY_GROUP_FIELDS != 0
    }

    const fn contains_aggregate(self) -> bool {
        self.bits & Self::CONTAINS_AGGREGATE != 0
    }

    const fn contains_case(self) -> bool {
        self.bits & Self::CONTAINS_CASE != 0
    }

    const fn contains_non_aggregate_wrapper_fn(self) -> bool {
        self.bits & Self::CONTAINS_NON_AGGREGATE_WRAPPER_FN != 0
    }
}

impl GroupedOrderExprAnalysis {
    // Build the shared grouped-order proof summary for one expression tree
    // while keeping canonical grouped-key proof and broader Top-K admission on
    // the same recursive owner.
    fn from_expr(expr: &Expr, group_fields: &[&str], expected_group_field: Option<&str>) -> Self {
        match expr {
            Expr::Field(field) => Self {
                canonical_shape: expected_group_field.map_or(
                    GroupedCanonicalOrderShape::Unsupported,
                    |expected_group_field| {
                        if field.as_str() == expected_group_field {
                            GroupedCanonicalOrderShape::CanonicalGroupField
                        } else {
                            GroupedCanonicalOrderShape::OtherField
                        }
                    },
                ),
                flags: GroupedOrderExprFlags::field_reference(
                    group_fields
                        .iter()
                        .any(|allowed| *allowed == field.as_str()),
                ),
            },
            Expr::Aggregate(_) => Self {
                canonical_shape: GroupedCanonicalOrderShape::Unsupported,
                flags: GroupedOrderExprFlags::aggregate(),
            },
            Expr::FieldPath(_) => Self {
                canonical_shape: GroupedCanonicalOrderShape::Unsupported,
                flags: GroupedOrderExprFlags::empty(),
            },
            Expr::Literal(_) => Self {
                canonical_shape: GroupedCanonicalOrderShape::Unsupported,
                flags: GroupedOrderExprFlags::group_field_only(),
            },
            Expr::FunctionCall { args, .. } => {
                let child = args.iter().fold(
                    Self {
                        canonical_shape: GroupedCanonicalOrderShape::Unsupported,
                        flags: GroupedOrderExprFlags::group_field_only(),
                    },
                    |current, arg| current.merge_with(Self::from_expr(arg, group_fields, None)),
                );

                Self {
                    canonical_shape: GroupedCanonicalOrderShape::Unsupported,
                    flags: if !child.flags.contains_aggregate()
                        || child.flags.contains_non_aggregate_wrapper_fn()
                    {
                        child.flags.with_non_aggregate_wrapper_fn()
                    } else {
                        child.flags
                    },
                }
            }
            Expr::Case {
                when_then_arms,
                else_expr,
            } => {
                let child = when_then_arms.iter().fold(
                    Self::from_expr(else_expr.as_ref(), group_fields, None),
                    |current, arm| {
                        current
                            .merge_with(Self::from_expr(arm.condition(), group_fields, None))
                            .merge_with(Self::from_expr(arm.result(), group_fields, None))
                    },
                );

                Self {
                    flags: child.flags.with_case(),
                    ..child
                }
            }
            Expr::Binary { op, left, right } => {
                let left_expr = left.as_ref();
                let right_expr = right.as_ref();
                let left = Self::from_expr(left_expr, group_fields, None);
                let right = Self::from_expr(right_expr, group_fields, None);

                Self {
                    canonical_shape: classify_grouped_canonical_order_shape(
                        *op,
                        left_expr,
                        right_expr,
                        expected_group_field,
                    ),
                    ..left.merge_with(right)
                }
            }
            #[cfg(test)]
            Expr::Alias { expr, .. } => Self {
                canonical_shape: GroupedCanonicalOrderShape::Unsupported,
                ..Self::from_expr(expr.as_ref(), group_fields, None)
            },
            Expr::Unary { expr, .. } => Self {
                canonical_shape: GroupedCanonicalOrderShape::Unsupported,
                ..Self::from_expr(expr.as_ref(), group_fields, None)
            },
        }
    }

    // Merge one child analysis into the current grouped-order proof summary
    // without widening canonical grouped-key proof beyond the parent node.
    const fn merge_with(self, other: Self) -> Self {
        Self {
            canonical_shape: GroupedCanonicalOrderShape::Unsupported,
            flags: self.flags.merge_with(other.flags),
        }
    }

    // Convert the shared canonical grouped-key proof shape into the
    // caller-facing admissibility contract used by grouped validation.
    const fn canonical_admissibility(self) -> GroupedOrderTermAdmissibility {
        match self.canonical_shape {
            GroupedCanonicalOrderShape::CanonicalGroupField => {
                GroupedOrderTermAdmissibility::Preserves(GroupedOrderExprClass::CanonicalGroupField)
            }
            GroupedCanonicalOrderShape::GroupFieldPlusConstant => {
                GroupedOrderTermAdmissibility::Preserves(
                    GroupedOrderExprClass::GroupFieldPlusConstant,
                )
            }
            GroupedCanonicalOrderShape::GroupFieldMinusConstant => {
                GroupedOrderTermAdmissibility::Preserves(
                    GroupedOrderExprClass::GroupFieldMinusConstant,
                )
            }
            GroupedCanonicalOrderShape::OtherField
            | GroupedCanonicalOrderShape::OtherFieldOffset => {
                GroupedOrderTermAdmissibility::PrefixMismatch
            }
            GroupedCanonicalOrderShape::Unsupported => {
                GroupedOrderTermAdmissibility::UnsupportedExpression
            }
        }
    }
}

// Keep canonical grouped-key proof intentionally syntactic and fail closed so
// only the admitted field-preserving offset family can reuse the resumable
// grouped-order lane.
fn classify_grouped_canonical_order_shape(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    expected_group_field: Option<&str>,
) -> GroupedCanonicalOrderShape {
    let Some(expected_group_field) = expected_group_field else {
        return GroupedCanonicalOrderShape::Unsupported;
    };

    match (op, left, right) {
        (BinaryOp::Add, Expr::Field(field), right)
            if field.as_str() == expected_group_field && is_numeric_order_offset_literal(right) =>
        {
            GroupedCanonicalOrderShape::GroupFieldPlusConstant
        }
        (BinaryOp::Sub, Expr::Field(field), right)
            if field.as_str() == expected_group_field && is_numeric_order_offset_literal(right) =>
        {
            GroupedCanonicalOrderShape::GroupFieldMinusConstant
        }
        (BinaryOp::Add | BinaryOp::Sub, Expr::Field(_), right)
            if is_numeric_order_offset_literal(right) =>
        {
            GroupedCanonicalOrderShape::OtherFieldOffset
        }
        _ => GroupedCanonicalOrderShape::Unsupported,
    }
}

// Classify one grouped ORDER BY term against one expected grouped key field
// so grouped validation can distinguish prefix mismatch from unsupported-but-
// evaluable grouped order expressions.
#[must_use]
pub(in crate::db) fn classify_grouped_order_term_for_field(
    expr: &Expr,
    expected_group_field: &str,
) -> GroupedOrderTermAdmissibility {
    GroupedOrderExprAnalysis::from_expr(expr, &[], Some(expected_group_field))
        .canonical_admissibility()
}

// Additive constant offsets preserve both ascending and descending order for
// the underlying grouped key while avoiding the tie/collapse behavior of the
// broader computed-order family.
const fn is_numeric_order_offset_literal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(
            Value::Int64(_)
                | Value::Int128(_)
                | Value::IntBig(_)
                | Value::Nat64(_)
                | Value::Nat128(_)
                | Value::NatBig(_)
                | Value::Decimal(_)
                | Value::Float32(_)
                | Value::Float64(_)
        )
    )
}

/// Return true when one grouped `ORDER BY` term is admissible for the
/// aggregate/post-aggregate Top-K lane over the declared grouped key set.
#[must_use]
pub(in crate::db) fn classify_grouped_top_k_order_term(
    expr: &Expr,
    group_fields: &[&str],
) -> GroupedTopKOrderTermAdmissibility {
    let analysis = GroupedOrderExprAnalysis::from_expr(expr, group_fields, None);

    if analysis.flags.references_only_group_fields() {
        if !analysis.flags.contains_aggregate()
            && analysis.flags.contains_non_aggregate_wrapper_fn()
        {
            return GroupedTopKOrderTermAdmissibility::UnsupportedExpression;
        }

        return GroupedTopKOrderTermAdmissibility::Admissible;
    }

    GroupedTopKOrderTermAdmissibility::NonGroupFieldReference
}

/// Return true when one grouped post-aggregate order expression must leave the
/// canonical grouped-key ordered lane for bounded Top-K finalization.
#[must_use]
pub(in crate::db) fn grouped_top_k_order_term_requires_heap(expr: &Expr) -> bool {
    let analysis = GroupedOrderExprAnalysis::from_expr(expr, &[], None);
    analysis.flags.contains_aggregate() || analysis.flags.contains_case()
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            query::plan::expr::{
                Expr, FieldPath, ProjectionField, ProjectionSpec,
                collect_unique_direct_projection_slots_with_schema,
            },
            schema::{
                AcceptedFieldKind, AcceptedSchemaSnapshot, FieldId, PersistedFieldSnapshot,
                PersistedSchemaSnapshot, SchemaFieldDefault, SchemaFieldSlot, SchemaInfo,
                SchemaRowLayout, SchemaVersion,
            },
        },
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec},
            index::IndexModel,
        },
        testing::entity_model_from_static,
    };

    static FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated(
            "profile",
            FieldKind::empty_test_composite("query::expr::projection::tests::Profile"),
        ),
    ];
    static INDEXES: [&IndexModel; 0] = [];
    static MODEL: EntityModel = entity_model_from_static(
        "query::plan::expr::projection::tests::Entity",
        "Entity",
        &FIELDS[0],
        0,
        &FIELDS,
        &INDEXES,
    );

    // Build one accepted schema with a deliberately divergent top-level slot
    // for `profile`. The unchecked accepted wrapper is test-only, letting this
    // module prove projection metadata follows accepted schema authority.
    fn accepted_schema_with_profile_slot(slot: SchemaFieldSlot) -> SchemaInfo {
        let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "query::plan::expr::projection::tests::Entity".to_string(),
            "Entity".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), slot),
                ],
            ),
            vec![
                PersistedFieldSnapshot::new(
                    FieldId::new(1),
                    "id".to_string(),
                    SchemaFieldSlot::new(0),
                    AcceptedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Structural,
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "profile".to_string(),
                    SchemaFieldSlot::new(1),
                    AcceptedFieldKind::test_composite(),
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::CatalogValue,
                    LeafCodec::Structural,
                ),
            ],
        ));

        SchemaInfo::from_snapshot_with_generated_model_for_test(&MODEL, &snapshot)
    }

    #[test]
    fn projection_referenced_slots_use_schema_slot_authority() {
        let schema = accepted_schema_with_profile_slot(SchemaFieldSlot::new(7));
        let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::FieldPath(FieldPath::new("profile", vec!["rank".to_string()])),
            alias: None,
        }]);

        let slots = projection
            .referenced_slots_for_schema(&MODEL, &schema)
            .expect("field-path projection should resolve through accepted schema");

        assert_eq!(slots, vec![7]);
    }

    #[test]
    fn direct_projection_slots_use_schema_slot_authority() {
        let schema = accepted_schema_with_profile_slot(SchemaFieldSlot::new(5));
        let slots = collect_unique_direct_projection_slots_with_schema(&schema, ["profile"])
            .expect("direct projection field should resolve through accepted schema");

        assert_eq!(slots, vec![5]);
    }
}
