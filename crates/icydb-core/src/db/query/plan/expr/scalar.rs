//! Module: query::plan::expr::scalar
//! Responsibility: planner-owned scalar projection program lowering.
//! Does not own: runtime projection evaluation or grouped projection lowering.
//! Boundary: freezes slot-resolved scalar projection programs before execution.

use crate::db::query::plan::expr::{PathSpec, UnaryOp};
#[cfg(test)]
use crate::db::scalar_expr::{compile_scalar_literal_expr_value, scalar_expr_value_into_value};
use crate::db::{
    query::plan::expr::{BinaryOp, CompiledPath, Expr, FieldPath, ProjectionField, ProjectionSpec},
    schema::SchemaInfo,
};
#[cfg(test)]
use crate::model::entity::EntityModel;
use crate::value::Value;

///
/// ScalarProjectionExpr
///
/// ScalarProjectionExpr is the planner-owned compiled scalar projection tree
/// carried into execution for scalar projection materialization.
/// Field slots are resolved once and scalar literals are prebuilt into runtime
/// `Value`s so executor consumers no longer rediscover projection structure or
/// re-materialize literals per row from `EntityModel`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ScalarProjectionExpr {
    Field(ScalarProjectionField),
    FieldPath(ScalarProjectionFieldPath),
    Literal(Value),
    FunctionCall {
        function: crate::db::query::plan::expr::Function,
        args: Vec<Self>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    Case {
        when_then_arms: Vec<ScalarProjectionCaseArm>,
        else_expr: Box<Self>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
}

///
/// ScalarProjectionFieldPath
///
/// Compiled nested field-path projection rooted at a resolved top-level slot.
/// The executor uses the slot to borrow the persisted root field bytes, then
/// walks the stored value payload without materializing intermediate maps.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ScalarProjectionFieldPath {
    path: PathSpec,
    compiled_path: CompiledPath,
    root_slot: usize,
}

impl ScalarProjectionFieldPath {
    /// Borrow the top-level field name used as the path root.
    #[must_use]
    pub(in crate::db) const fn root(&self) -> &str {
        self.path.root().as_str()
    }

    /// Borrow the resolved top-level field slot used by execution.
    #[must_use]
    pub(in crate::db) const fn root_slot(&self) -> usize {
        self.root_slot
    }

    /// Borrow the nested map-key path below the root field.
    #[must_use]
    pub(in crate::db) const fn segments(&self) -> &[String] {
        self.compiled_path.segments()
    }
}

///
/// ScalarProjectionCaseArm
///
/// Compiled scalar searched-CASE arm carried into executor evaluation.
/// Conditions and results are independently compiled onto the scalar seam so
/// runtime can evaluate only the selected branch without rediscovering slots.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ScalarProjectionCaseArm {
    condition: ScalarProjectionExpr,
    result: ScalarProjectionExpr,
}

impl ScalarProjectionCaseArm {
    /// Build one compiled scalar CASE arm.
    #[must_use]
    pub(in crate::db) const fn new(
        condition: ScalarProjectionExpr,
        result: ScalarProjectionExpr,
    ) -> Self {
        Self { condition, result }
    }

    /// Borrow the compiled condition expression.
    #[must_use]
    pub(in crate::db) const fn condition(&self) -> &ScalarProjectionExpr {
        &self.condition
    }

    /// Borrow the compiled result expression.
    #[must_use]
    pub(in crate::db) const fn result(&self) -> &ScalarProjectionExpr {
        &self.result
    }
}

///
/// ScalarProjectionField
///
/// ScalarProjectionField is one resolved scalar field reference inside a
/// planner-owned compiled projection expression.
/// It preserves field-name diagnostics while turning field access into one
/// direct slot lookup for executor consumers.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ScalarProjectionField {
    field: String,
    slot: usize,
}

impl ScalarProjectionField {
    /// Borrow the declared field name for diagnostics.
    #[must_use]
    pub(in crate::db) const fn field(&self) -> &str {
        self.field.as_str()
    }

    /// Borrow the resolved slot index used by executor readers.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> usize {
        self.slot
    }
}

/// Compile one model-only scalar projection expression into a planner-owned
/// slot-resolved program when it stays entirely on the scalar seam.
#[cfg(test)]
#[must_use]
pub(in crate::db) fn compile_scalar_projection_expr_for_model_only(
    model: &EntityModel,
    expr: &Expr,
) -> Option<ScalarProjectionExpr> {
    compile_scalar_projection_expr_with_schema(
        SchemaInfo::cached_for_generated_entity_model(model),
        expr,
    )
}

/// Compile one scalar projection expression using an explicit schema authority.
///
/// Accepted-schema planning paths use this helper so field and field-path root
/// slots come from `SchemaInfo` instead of directly re-reading generated model
/// slot order.
#[must_use]
pub(in crate::db) fn compile_scalar_projection_expr_with_schema(
    schema: &SchemaInfo,
    expr: &Expr,
) -> Option<ScalarProjectionExpr> {
    compile_scalar_projection_expr_with_schema_authority(schema, expr)
}

/// Compile one scalar projection expression using only schema authority.
///
/// Runtime consumers use this explicit entrypoint when the surrounding call
/// site wants to document that generated model metadata is outside the
/// projection compiler boundary.
#[must_use]
pub(in crate::db) fn compile_scalar_projection_expr_from_schema(
    schema: &SchemaInfo,
    expr: &Expr,
) -> Option<ScalarProjectionExpr> {
    compile_scalar_projection_expr_with_schema_authority(schema, expr)
}

fn compile_scalar_projection_expr_with_schema_authority(
    schema: &SchemaInfo,
    expr: &Expr,
) -> Option<ScalarProjectionExpr> {
    match expr {
        Expr::Field(field_id) => compile_scalar_field_reference(schema, field_id.as_str()),
        Expr::FieldPath(path) => compile_scalar_field_path_reference(schema, path),
        Expr::Literal(value) => Some(compile_scalar_literal(value)),
        Expr::FunctionCall { function, args } => {
            let args = args
                .iter()
                .map(|arg| compile_scalar_projection_expr_with_schema_authority(schema, arg))
                .collect::<Option<Vec<_>>>()?;

            Some(ScalarProjectionExpr::FunctionCall {
                function: *function,
                args,
            })
        }
        Expr::Unary { op, expr } => {
            compile_scalar_projection_expr_with_schema_authority(schema, expr.as_ref()).map(
                |expr| ScalarProjectionExpr::Unary {
                    op: *op,
                    expr: Box::new(expr),
                },
            )
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            let when_then_arms = when_then_arms
                .iter()
                .map(|arm| {
                    Some(ScalarProjectionCaseArm::new(
                        compile_scalar_projection_expr_with_schema_authority(
                            schema,
                            arm.condition(),
                        )?,
                        compile_scalar_projection_expr_with_schema_authority(schema, arm.result())?,
                    ))
                })
                .collect::<Option<Vec<_>>>()?;
            let else_expr =
                compile_scalar_projection_expr_with_schema_authority(schema, else_expr.as_ref())?;

            Some(ScalarProjectionExpr::Case {
                when_then_arms,
                else_expr: Box::new(else_expr),
            })
        }
        Expr::Binary { op, left, right } => {
            let left = compile_scalar_projection_expr_with_schema_authority(schema, left.as_ref())?;
            let right =
                compile_scalar_projection_expr_with_schema_authority(schema, right.as_ref())?;

            Some(ScalarProjectionExpr::Binary {
                op: *op,
                left: Box::new(left),
                right: Box::new(right),
            })
        }
        Expr::Aggregate(_) => None,
        #[cfg(test)]
        Expr::Alias { expr, .. } => {
            compile_scalar_projection_expr_with_schema_authority(schema, expr.as_ref())
        }
    }
}

/// Compile one scalar projection spec using an explicit schema authority.
///
/// This freezes row-slot contracts from the schema view chosen by planning so
/// prepared projection execution does not re-resolve slots from generated
/// model order.
#[must_use]
pub(in crate::db) fn compile_scalar_projection_plan_with_schema(
    schema: &SchemaInfo,
    projection: &ProjectionSpec,
) -> Option<Vec<ScalarProjectionExpr>> {
    let mut compiled_fields = Vec::with_capacity(projection.len());

    for field in projection.fields() {
        compiled_fields.push(compile_scalar_projection_field(schema, field)?);
    }

    Some(compiled_fields)
}

// Field paths resolve their root slot through the caller-provided schema view;
// only the nested tail is deferred to executor value-storage traversal.
fn compile_scalar_field_path_reference(
    schema: &SchemaInfo,
    path: &FieldPath,
) -> Option<ScalarProjectionExpr> {
    debug_assert!(path.path_spec().is_scalar_leaf());
    let path_spec = path.path_spec().clone();
    let compiled_path = CompiledPath::new(path_spec.segments().to_vec());

    Some(ScalarProjectionExpr::FieldPath(ScalarProjectionFieldPath {
        path: path_spec,
        compiled_path,
        root_slot: schema.field_slot_index(path.root().as_str())?,
    }))
}

// Field references are the only scalar projection leaves that need schema slot
// resolution before recursion continues.
fn compile_scalar_field_reference(
    schema: &SchemaInfo,
    field_name: &str,
) -> Option<ScalarProjectionExpr> {
    let slot = schema.field_slot_index(field_name)?;

    Some(ScalarProjectionExpr::Field(ScalarProjectionField {
        field: field_name.to_string(),
        slot,
    }))
}

// Literal lowering stays owner-local here so the expression compiler can keep
// the recursive shape match focused on planner expression structure.
fn compile_scalar_literal(value: &Value) -> ScalarProjectionExpr {
    #[cfg(test)]
    {
        if let Some(compiled) = compile_scalar_literal_expr_value(value) {
            return ScalarProjectionExpr::Literal(scalar_expr_value_into_value(compiled));
        }

        // Decimal and other non-shared-scalar test literals still remain valid
        // runtime projection leaves even when the shared scalar test helper does
        // not model them directly.
        ScalarProjectionExpr::Literal(value.clone())
    }

    #[cfg(not(test))]
    {
        ScalarProjectionExpr::Literal(value.clone())
    }
}

// Projection-plan compilation only admits scalar projection fields at this
// boundary, so the field wrapper is lowered through one shared helper.
fn compile_scalar_projection_field(
    schema: &SchemaInfo,
    field: &ProjectionField,
) -> Option<ScalarProjectionExpr> {
    compile_scalar_projection_expr_with_schema(schema, field.expr())
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            query::plan::expr::{
                Expr, FieldId as ExprFieldId, FieldPath, ScalarProjectionExpr,
                compile_scalar_projection_expr_with_schema,
            },
            schema::{
                AcceptedSchemaSnapshot, FieldId, PersistedFieldKind, PersistedFieldSnapshot,
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
        FieldModel::generated("profile", FieldKind::Structured { queryable: true }),
    ];
    static INDEXES: [&IndexModel; 0] = [];
    static MODEL: EntityModel = entity_model_from_static(
        "query::plan::expr::scalar::tests::Entity",
        "Entity",
        &FIELDS[0],
        0,
        &FIELDS,
        &INDEXES,
    );

    // Build one accepted schema with a deliberately different row-layout slot
    // for `profile`. The unchecked accepted wrapper is test-only, and lets this
    // module prove compilation follows `SchemaInfo` rather than generated order.
    fn accepted_schema_with_profile_slot(slot: SchemaFieldSlot) -> SchemaInfo {
        let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "query::plan::expr::scalar::tests::Entity".to_string(),
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
                    PersistedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::StructuralFallback,
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "profile".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Structured { queryable: true },
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::Value,
                    LeafCodec::StructuralFallback,
                ),
            ],
        ));

        SchemaInfo::from_accepted_snapshot_for_model(&MODEL, &snapshot)
    }

    #[test]
    fn scalar_field_compilation_uses_schema_slot_authority() {
        let schema = accepted_schema_with_profile_slot(SchemaFieldSlot::new(9));
        let expr = Expr::Field(ExprFieldId::new("profile"));
        let compiled = compile_scalar_projection_expr_with_schema(&schema, &expr)
            .expect("accepted schema field slot should compile");

        let ScalarProjectionExpr::Field(field) = compiled else {
            panic!("field expression should compile as direct field");
        };
        assert_eq!(field.slot(), 9);
    }

    #[test]
    fn scalar_field_path_compilation_uses_schema_root_slot_authority() {
        let schema = accepted_schema_with_profile_slot(SchemaFieldSlot::new(7));
        let expr = Expr::FieldPath(FieldPath::new("profile", vec!["rank".to_string()]));
        let compiled = compile_scalar_projection_expr_with_schema(&schema, &expr)
            .expect("accepted schema field-path root slot should compile");

        let ScalarProjectionExpr::FieldPath(path) = compiled else {
            panic!("field-path expression should compile as field path");
        };
        assert_eq!(path.root_slot(), 7);
        assert_eq!(path.segments(), ["rank".to_string()].as_slice());
    }
}
