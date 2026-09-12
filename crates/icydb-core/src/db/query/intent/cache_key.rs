//! Module: query::intent::cache_key
//! Responsibility: canonical shared-cache identity normalization for structural queries.
//! Does not own: planner validation, executor runtime behavior, or SQL surface routing.
//! Boundary: turns semantic query intent into one explicit derived-hash cache key.

use crate::{
    db::{
        QueryError,
        predicate::MissingRowPolicy,
        query::{
            builder::aggregate::AggregateExpr,
            intent::{model::QueryModel, state::GroupedIntent},
            plan::{
                AggregateSemanticKeyRef, OrderDirection, OrderSpec, PreparedQueryParameterContract,
                QueryMode,
                expr::{Expr, Function, ProjectionField, ProjectionSelection},
            },
            preparation::PreparationWork,
        },
    },
    value::{Value, hash_value},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::rc::Rc;

///
/// StructuralQueryCacheKey
///
/// Canonical semantic identity for the shared structural query-plan cache.
/// This key is intentionally explicit: normalization owns semantic equivalence,
/// while `Hash` ownership stays mechanical at the map boundary.
/// Clones share immutable content; query edits replace the memoized key rather
/// than mutating it. Accepted runtime authority stays in the session key shell.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct StructuralQueryCacheKey(Rc<StructuralQueryCacheKeyData>);

// One completed payload per construction, shared by memo and cache key copies.
// Keep it private and non-Clone so the handle owns all key sharing.
#[derive(Debug, Eq, Hash, PartialEq)]
struct StructuralQueryCacheKeyData {
    mode: QueryModeCacheKey,
    predicate: Option<[u8; 32]>,
    parameter_contract: Option<PreparedQueryParameterContract>,
    filter_expr: Option<ProjectionExprCacheKey>,
    order: Option<Vec<OrderTermCacheKey>>,
    distinct: bool,
    projection: ProjectionCacheKey,
    grouping: Option<GroupingCacheKey>,
    consistency: ConsistencyCacheKey,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum QueryModeCacheKey {
    Load { limit: Option<u32>, offset: u32 },
    Delete { limit: Option<u32>, offset: u32 },
}

// Hash failures reject key construction; errors are never reusable identities.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ValueCacheKey {
    Canonical([u8; 16]),
}

///
/// OrderTermCacheKey
///
/// Canonical representation of one `ORDER BY` term in the structural query
/// cache key.
/// Typed expression identity, rather than its display label, keeps cache hits
/// from crossing different operand domains or sort layouts.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct OrderTermCacheKey {
    expr: ProjectionExprCacheKey,
    direction: OrderDirectionCacheKey,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum OrderDirectionCacheKey {
    Asc,
    Desc,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ProjectionCacheKey {
    All,
    Fields(Vec<String>),
    // Cached projections retain output aliases as well as evaluation semantics.
    Exprs(Vec<(ProjectionExprCacheKey, Option<String>)>),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ProjectionExprCacheKey {
    Field(String),
    FieldPath {
        root: String,
        segments: Vec<String>,
    },
    Literal(ValueCacheKey),
    FunctionCall {
        function: Function,
        args: Vec<Self>,
    },
    Unary {
        op: UnaryOpCacheKey,
        expr: Box<Self>,
    },
    Case {
        when_then_arms: Vec<CaseWhenArmCacheKey>,
        else_expr: Box<Self>,
    },
    Binary {
        op: BinaryOpCacheKey,
        left: Box<Self>,
        right: Box<Self>,
    },
    Aggregate(AggregateCacheKey),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CaseWhenArmCacheKey {
    condition: ProjectionExprCacheKey,
    result: ProjectionExprCacheKey,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum BinaryOpCacheKey {
    Or,
    And,
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum UnaryOpCacheKey {
    Not,
}

///
/// AggregateCacheKey
///
/// Canonical aggregate identity shared by projected and grouped aggregate
/// cache entries. It records only the semantic pieces that affect planner
/// reuse.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct AggregateCacheKey {
    kind_tag: u8,
    input_expr: Option<Box<ProjectionExprCacheKey>>,
    filter_expr: Option<Box<ProjectionExprCacheKey>>,
    distinct: bool,
}

///
/// GroupingCacheKey
///
/// Canonical identity for the grouped-query portion of a structural cache key.
/// This captures grouping fields, aggregate slots, grouped `HAVING`
/// expressions, and the configured grouping limits so grouped plans only reuse
/// compatible shapes.
/// This is a canonicalized grouped structural/cache identity surface;
/// prepared/template identity remains outside this key and stays syntax-bound.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct GroupingCacheKey {
    // The enclosing query-plan key already carries accepted schema identity,
    // so canonical field/path identity is sufficient and cannot cross a slot remap.
    group_fields: Vec<ProjectionExprCacheKey>,
    aggregates: Vec<AggregateCacheKey>,
    having_expr: Option<ProjectionExprCacheKey>,
    max_groups: u64,
    max_group_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ConsistencyCacheKey {
    Ignore,
    Error,
}

impl StructuralQueryCacheKey {
    pub(in crate::db::query) fn from_query_model_with_normalized_predicate_fingerprint(
        model: &QueryModel,
        predicate_fingerprint: Option<[u8; 32]>,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        Self::from_query_model_with_optional_predicate_key(model, predicate_fingerprint, None, work)
    }

    pub(in crate::db::query) fn from_query_model_with_parameter_contract(
        model: &QueryModel,
        parameter_contract: PreparedQueryParameterContract,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        Self::from_query_model_with_optional_predicate_key(
            model,
            None,
            Some(parameter_contract),
            work,
        )
    }

    // Build the shared structural cache key from one optional predicate-key
    // fragment so callers that already computed canonical predicate identity
    // do not walk the same normalized tree twice.
    fn from_query_model_with_optional_predicate_key(
        model: &QueryModel,
        predicate: Option<[u8; 32]>,
        parameter_contract: Option<PreparedQueryParameterContract>,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        work.charge(
            Resource::TemporaryBytes,
            (size_of::<StructuralQueryCacheKeyData>() + 2 * size_of::<usize>()) as u64,
        )?;
        let scalar = model.scalar_intent_for_cache_key();
        // The fully-covered parameter contract is the semantic filter
        // authority. Equivalent IN filters may lower to different-width OR
        // expression trees, so authored expression arity must not enter the
        // reusable template identity.
        let filter_expr = if parameter_contract.is_some() {
            None
        } else {
            scalar
                .filter
                .as_ref()
                .and_then(|filter| filter.logical_filter_expr())
                .map(|expr| ProjectionExprCacheKey::from_expr(expr, work))
                .transpose()?
        };
        Ok(Self(Rc::new(StructuralQueryCacheKeyData {
            mode: QueryModeCacheKey::from_query_mode(model.mode()),
            // Canonical scalar `filter_expr` owns semantic filter identity when
            // present. The derived predicate key remains only for plans that
            // still have no planner-owned semantic filter expression.
            predicate: if filter_expr.is_some() {
                None
            } else {
                predicate
            },
            parameter_contract,
            filter_expr,
            order: scalar
                .order
                .as_ref()
                .map(|order| OrderTermCacheKey::from_order_spec(order, work))
                .transpose()?,
            distinct: scalar.distinct,
            projection: ProjectionCacheKey::from_projection_selection(
                &scalar.projection_selection,
                work,
            )?,
            grouping: model
                .grouped_intent_for_cache_key()
                .map(|grouped| GroupingCacheKey::from_grouped_intent(grouped, work))
                .transpose()?,
            consistency: ConsistencyCacheKey::from_missing_row_policy(
                model.consistency_for_cache_key(),
            ),
        })))
    }
}

impl QueryModeCacheKey {
    const fn from_query_mode(mode: QueryMode) -> Self {
        match mode {
            QueryMode::Load(spec) => Self::Load {
                limit: spec.limit(),
                offset: spec.offset(),
            },
            QueryMode::Delete(spec) => Self::Delete {
                limit: spec.limit(),
                offset: spec.offset(),
            },
        }
    }
}

impl ValueCacheKey {
    fn from_value(value: &Value) -> Result<Self, QueryError> {
        hash_value(value)
            .map(Self::Canonical)
            .map_err(QueryError::execute)
    }
}

impl OrderTermCacheKey {
    fn from_order_spec(
        order: &OrderSpec,
        work: &PreparationWork<'_>,
    ) -> Result<Vec<Self>, QueryError> {
        work.copy_slice(&order.fields, |term| {
            Ok(Self {
                expr: ProjectionExprCacheKey::from_expr(term.expr(), work)?,
                direction: OrderDirectionCacheKey::from_order_direction(term.direction()),
            })
        })
    }
}

impl OrderDirectionCacheKey {
    const fn from_order_direction(direction: OrderDirection) -> Self {
        match direction {
            OrderDirection::Asc => Self::Asc,
            OrderDirection::Desc => Self::Desc,
        }
    }
}

impl ProjectionCacheKey {
    fn from_projection_selection(
        projection: &ProjectionSelection,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        Ok(match projection {
            ProjectionSelection::All => Self::All,
            ProjectionSelection::Fields(fields) => {
                Self::Fields(work.copy_slice(fields, |field| work.copy_text(field.as_str()))?)
            }
            ProjectionSelection::Exprs(fields) => Self::Exprs(work.copy_slice(
                fields,
                |ProjectionField::Scalar { expr, alias }| {
                    Ok((
                        ProjectionExprCacheKey::from_expr(expr, work)?,
                        alias
                            .as_ref()
                            .map(|alias| work.copy_text(alias.as_str()))
                            .transpose()?,
                    ))
                },
            )?),
        })
    }
}

impl ProjectionExprCacheKey {
    fn from_expr(expr: &Expr, work: &PreparationWork<'_>) -> Result<Self, QueryError> {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        Ok(match expr {
            Expr::Field(field) => Self::Field(work.copy_text(field.as_str())?),
            Expr::FieldPath(path) => Self::FieldPath {
                root: work.copy_text(path.root().as_str())?,
                segments: work.copy_slice(path.segments(), |segment| work.copy_text(segment))?,
            },
            Expr::Literal(value) => Self::Literal(ValueCacheKey::from_value(value)?),
            Expr::FunctionCall { function, args } => Self::FunctionCall {
                function: *function,
                args: work.copy_slice(args, |expr| Self::from_expr(expr, work))?,
            },
            Expr::Unary { op, expr } => Self::Unary {
                op: UnaryOpCacheKey::from_unary_op(*op),
                expr: Self::boxed_from_expr(expr, work)?,
            },
            Expr::Case {
                when_then_arms,
                else_expr,
            } => Self::Case {
                when_then_arms: work.copy_slice(when_then_arms, |arm| {
                    CaseWhenArmCacheKey::from_arm(arm, work)
                })?,
                else_expr: Self::boxed_from_expr(else_expr, work)?,
            },
            Expr::Binary { op, left, right } => Self::Binary {
                op: BinaryOpCacheKey::from_binary_op(*op),
                left: Self::boxed_from_expr(left, work)?,
                right: Self::boxed_from_expr(right, work)?,
            },
            Expr::Aggregate(aggregate) => {
                Self::Aggregate(AggregateCacheKey::from_aggregate_expr(aggregate, work)?)
            }
            #[cfg(test)]
            Expr::Alias { expr, name: _ } => Self::from_expr(expr.as_ref(), work)?,
        })
    }

    // Admit backing before recursively constructing a child; failed partial
    // keys remain local and add no nesting beyond the borrowed source tree.
    fn boxed_from_expr(expr: &Expr, work: &PreparationWork<'_>) -> Result<Box<Self>, QueryError> {
        work.charge(Resource::TemporaryBytes, size_of::<Self>() as u64)?;
        Ok(Box::new(Self::from_expr(expr, work)?))
    }

    fn from_group_field(
        field: crate::db::query::plan::GroupFieldRef<'_>,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        Ok(match field.as_scalar_path() {
            Some(path) => Self::FieldPath {
                root: work.copy_text(path.path().root().as_str())?,
                segments: work
                    .copy_slice(path.path().segments(), |segment| work.copy_text(segment))?,
            },
            None => Self::Field(work.copy_text(field.field())?),
        })
    }
}

impl BinaryOpCacheKey {
    const fn from_binary_op(op: crate::db::query::plan::expr::BinaryOp) -> Self {
        match op {
            crate::db::query::plan::expr::BinaryOp::Or => Self::Or,
            crate::db::query::plan::expr::BinaryOp::And => Self::And,
            crate::db::query::plan::expr::BinaryOp::Eq => Self::Eq,
            crate::db::query::plan::expr::BinaryOp::Ne => Self::Ne,
            crate::db::query::plan::expr::BinaryOp::Lt => Self::Lt,
            crate::db::query::plan::expr::BinaryOp::Lte => Self::Lte,
            crate::db::query::plan::expr::BinaryOp::Gt => Self::Gt,
            crate::db::query::plan::expr::BinaryOp::Gte => Self::Gte,
            crate::db::query::plan::expr::BinaryOp::Add => Self::Add,
            crate::db::query::plan::expr::BinaryOp::Sub => Self::Sub,
            crate::db::query::plan::expr::BinaryOp::Mul => Self::Mul,
            crate::db::query::plan::expr::BinaryOp::Div => Self::Div,
        }
    }
}

impl UnaryOpCacheKey {
    const fn from_unary_op(op: crate::db::query::plan::expr::UnaryOp) -> Self {
        match op {
            crate::db::query::plan::expr::UnaryOp::Not => Self::Not,
        }
    }
}

impl CaseWhenArmCacheKey {
    fn from_arm(
        arm: &crate::db::query::plan::expr::CaseWhenArm,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        Ok(Self {
            condition: ProjectionExprCacheKey::from_expr(arm.condition(), work)?,
            result: ProjectionExprCacheKey::from_expr(arm.result(), work)?,
        })
    }
}

impl AggregateCacheKey {
    fn from_aggregate_expr(
        aggregate: &AggregateExpr,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        Self::from_semantic_key(
            AggregateSemanticKeyRef::from_aggregate_expr(aggregate),
            work,
        )
    }

    fn from_group_aggregate_spec(
        aggregate: &crate::db::query::plan::GroupAggregateSpec,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        Self::from_semantic_key(aggregate.semantic_key(), work)
    }

    // Labels are presentation, not identity: Decimal(1) and U256(1) both render
    // as "1" but have different query semantics. Reuse structural expression
    // keys after the shared aggregate owner decides COUNT/DISTINCT equivalence.
    fn from_semantic_key(
        identity: AggregateSemanticKeyRef<'_>,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        Ok(Self {
            kind_tag: identity.kind().fingerprint_tag(),
            input_expr: identity
                .input_expr()
                .map(|expr| ProjectionExprCacheKey::boxed_from_expr(expr, work))
                .transpose()?,
            filter_expr: identity
                .filter_expr()
                .map(|expr| ProjectionExprCacheKey::boxed_from_expr(expr, work))
                .transpose()?,
            distinct: identity.distinct(),
        })
    }
}

impl GroupingCacheKey {
    fn from_grouped_intent(
        grouped: &GroupedIntent,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        let mut group_fields = work.vec_with_capacity(grouped.group.group_fields.len())?;
        for field in grouped.group.group_fields.iter() {
            group_fields.push(ProjectionExprCacheKey::from_group_field(field, work)?);
        }
        Ok(Self {
            group_fields,
            aggregates: work.copy_slice(&grouped.group.aggregates, |aggregate| {
                AggregateCacheKey::from_group_aggregate_spec(aggregate, work)
            })?,
            having_expr: grouped
                .having_expr
                .as_ref()
                .map(|expr| ProjectionExprCacheKey::from_expr(expr, work))
                .transpose()?,
            max_groups: grouped.group.execution.max_groups,
            max_group_bytes: grouped.group.execution.max_group_bytes,
        })
    }
}

impl ConsistencyCacheKey {
    const fn from_missing_row_policy(policy: MissingRowPolicy) -> Self {
        match policy {
            MissingRowPolicy::Ignore => Self::Ignore,
            MissingRowPolicy::Error => Self::Error,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            predicate::MissingRowPolicy,
            query::{
                builder::aggregate,
                intent::StructuralQuery,
                plan::{
                    AggregateKind, GroupAggregateSpec, OrderDirection, OrderSpec, OrderTerm,
                    expr::{Alias, Expr, ProjectionField, ProjectionSelection},
                },
            },
        },
        retained::RetainedBytes,
        types::{Decimal, U256},
        value::Value,
    };

    use super::{AggregateCacheKey, OrderTermCacheKey, ProjectionCacheKey};
    use crate::db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            plan::expr::{BinaryOp, CaseWhenArm, FieldPath, Function, UnaryOp},
            preparation::{PreparationWork, with_preparation_work},
        },
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
    };
    use std::{
        hash::{BuildHasher, BuildHasherDefault, DefaultHasher},
        rc::Rc,
    };

    #[test]
    fn key_construction_admits_backing_and_retries_without_partial_memos() {
        let make_query = || {
            let expr = Expr::Case {
                when_then_arms: vec![CaseWhenArm::new(
                    Expr::Binary {
                        op: BinaryOp::Eq,
                        left: Box::new(Expr::FieldPath(FieldPath::new(
                            "record",
                            vec!["inner".into()],
                        ))),
                        right: Box::new(Expr::Unary {
                            op: UnaryOp::Not,
                            expr: Box::new(Expr::Literal(Value::Null)),
                        }),
                    },
                    Expr::Aggregate(
                        aggregate::sum("amount").with_filter_expr(Expr::Field("flag".into())),
                    ),
                )],
                else_expr: Box::new(Expr::FunctionCall {
                    function: Function::Coalesce,
                    args: vec![Expr::Field("fallback".into())],
                }),
            };
            StructuralQuery::new(MissingRowPolicy::Ignore)
                .order_spec(OrderSpec {
                    fields: vec![OrderTerm::new(expr.clone(), OrderDirection::Asc)],
                })
                .projection_selection(ProjectionSelection::Exprs(vec![ProjectionField::Scalar {
                    expr,
                    alias: Some(Alias::new("output")),
                }]))
        };
        let request = |resource, limit| {
            RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(resource, limit),
            )
        };
        let build = |query: &StructuralQuery, root: &RequestExecutionRoot| {
            PreparationWork::run(&root.scope(), DiagnosticExecutionLane::Diagnostic, |work| {
                query.structural_cache_key_with_normalized_predicate_fingerprint(None, work)
            })
        };
        let measured = request(Resource::TemporaryBytes, 16_000_000);
        let expected = build(&make_query(), &measured).unwrap();
        // Independent retained traversal counts exactly the new backing for this
        // key: the handle itself is returned inline, not allocated separately.
        assert_eq!(
            measured.observed(Resource::TemporaryBytes),
            (RetainedBytes::measure(&expected, usize::MAX).unwrap() - size_of_val(&expected))
                as u64
        );
        for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
            let exact = measured.observed(resource);
            let query = make_query();
            let denied = request(resource, exact - 1);
            for _ in 0..2 {
                let error: QueryError = build(&query, &denied).unwrap_err();
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                );
            }
            let admitted = request(resource, exact);
            assert_eq!(build(&query, &admitted).unwrap(), expected);
            assert_eq!(admitted.observed(resource), exact);
            // A completed memo can be shared without allocating/copying again.
            assert_eq!(build(&query, &denied).unwrap(), expected);
            assert_eq!(build(&query, &admitted).unwrap(), expected);
            assert_eq!(admitted.observed(resource), exact);
        }
    }

    #[test]
    fn shared_structural_key_preserves_content_identity_and_retention() {
        with_preparation_work(|work| {
            let make_key = || {
                StructuralQuery::new(MissingRowPolicy::Ignore)
                    .projection_selection(ProjectionSelection::Exprs(vec![
                        ProjectionField::Scalar {
                            expr: Expr::Aggregate(aggregate::sum("amount".repeat(100))),
                            alias: Some(Alias::new("output".repeat(100))),
                        },
                    ]))
                    .structural_cache_key_with_normalized_predicate_fingerprint(None, work)
                    .unwrap()
            };
            let key = make_key();
            let cloned = key.clone();
            // Sharing is the resource contract of key cloning, not pointer-based identity.
            assert!(Rc::ptr_eq(&key.0, &cloned.0));
            let independent = make_key();
            assert!(!Rc::ptr_eq(&key.0, &independent.0));
            assert_eq!(key, independent);
            let hash = BuildHasherDefault::<DefaultHasher>::default();
            assert_eq!(hash.hash_one(&key), hash.hash_one(&independent));
            // The handle must preserve the payload's existing content hash.
            assert_eq!(hash.hash_one(&key), hash.hash_one(key.0.as_ref()));

            let bytes = RetainedBytes::measure(&key, usize::MAX).unwrap();
            let payload_bytes = RetainedBytes::measure(key.0.as_ref(), usize::MAX).unwrap();
            assert!(bytes >= size_of_val(&key) + 2 * size_of::<usize>() + payload_bytes);
            assert_eq!(RetainedBytes::measure(&key, bytes), Some(bytes));
            assert!(RetainedBytes::measure(&key, bytes - 1).is_none());
            drop(key);
            assert_eq!(cloned, independent);
            assert_eq!(RetainedBytes::measure(&cloned, usize::MAX), Some(bytes));
        });
    }

    #[test]
    fn projection_cache_keys_preserve_optional_alias_text() {
        with_preparation_work(|work| {
            let key = |alias: Option<&str>| {
                ProjectionCacheKey::from_projection_selection(
                    &ProjectionSelection::Exprs(vec![ProjectionField::Scalar {
                        expr: Expr::Field("label".into()),
                        alias: alias.map(Alias::new),
                    }]),
                    work,
                )
                .unwrap()
            };
            let aliases = [None, Some(""), Some("first"), Some("second"), Some("FIRST")];
            for left in aliases {
                for right in aliases {
                    assert_eq!(key(left) == key(right), left == right);
                }
            }
        });
    }

    #[test]
    fn projection_cache_retention_includes_alias_backing() {
        with_preparation_work(|work| {
            let alias = "output".repeat(100);
            let key = ProjectionCacheKey::from_projection_selection(
                &ProjectionSelection::Exprs(vec![ProjectionField::Scalar {
                    expr: Expr::Field("label".into()),
                    alias: Some(Alias::new(alias.as_str())),
                }]),
                work,
            )
            .unwrap();
            let bytes = RetainedBytes::measure(&key, usize::MAX).unwrap();
            assert!(
                bytes
                    >= size_of::<ProjectionCacheKey>()
                        + size_of::<(super::ProjectionExprCacheKey, Option<String>)>()
                        + "label".len()
                        + alias.len()
            );
            assert_eq!(RetainedBytes::measure(&key, bytes), Some(bytes));
            assert!(RetainedBytes::measure(&key, bytes - 1).is_none());
            let cloned = key.clone();
            assert_eq!(RetainedBytes::measure(&cloned, usize::MAX), Some(bytes));
            assert_eq!(key, cloned);
        });
    }

    #[test]
    fn order_cache_keys_preserve_typed_operands_and_direction() {
        with_preparation_work(|work| {
            let key = |expr, direction| {
                OrderTermCacheKey::from_order_spec(
                    &OrderSpec {
                        fields: vec![OrderTerm::new(expr, direction)],
                    },
                    work,
                )
                .unwrap()
            };
            let decimal = Expr::Literal(Value::Decimal(Decimal::from(1_u64)));
            let wide = Expr::Literal(Value::U256(U256::from(1_u64)));
            for (left, right) in [
                (decimal.clone(), wide.clone()),
                (
                    Expr::Aggregate(aggregate::AggregateExpr::from_expression_input(
                        AggregateKind::Sum,
                        decimal,
                    )),
                    Expr::Aggregate(aggregate::AggregateExpr::from_expression_input(
                        AggregateKind::Sum,
                        wide,
                    )),
                ),
            ] {
                assert_ne!(
                    key(left, OrderDirection::Asc),
                    key(right, OrderDirection::Asc)
                );
            }
            let field = Expr::Field("label".into());
            assert_ne!(
                key(field.clone(), OrderDirection::Asc),
                key(field, OrderDirection::Desc),
            );
            assert_eq!(
                key(Expr::Aggregate(aggregate::count()), OrderDirection::Asc),
                key(
                    Expr::Aggregate(aggregate::AggregateExpr::from_expression_input(
                        AggregateKind::Count,
                        Expr::Literal(Value::Nat64(1)),
                    )),
                    OrderDirection::Asc,
                ),
            );
        });
    }

    #[test]
    fn order_cache_retention_includes_nested_operands() {
        with_preparation_work(|work| {
            let field = "amount".repeat(100);
            let filter = "filter".repeat(100);
            let order = OrderSpec {
                fields: vec![OrderTerm::new(
                    Expr::Aggregate(
                        aggregate::sum(field.clone())
                            .with_filter_expr(Expr::Field(filter.clone().into())),
                    ),
                    OrderDirection::Desc,
                )],
            };
            let key = OrderTermCacheKey::from_order_spec(&order, work).unwrap();
            let bytes = RetainedBytes::measure(&key, usize::MAX).unwrap();
            assert!(
                bytes
                    >= size_of_val(&key)
                        + size_of::<OrderTermCacheKey>()
                        + 2 * size_of::<super::ProjectionExprCacheKey>()
                        + field.len()
                        + filter.len()
            );
            assert_eq!(RetainedBytes::measure(&key, bytes), Some(bytes));
            assert!(RetainedBytes::measure(&key, bytes - 1).is_none());
        });
    }

    #[test]
    fn aggregate_cache_keys_preserve_typed_inputs_and_canonical_equivalence() {
        with_preparation_work(|work| {
            let from_input = |kind, value| {
                aggregate::AggregateExpr::from_expression_input(kind, Expr::Literal(value))
            };
            for kind in [AggregateKind::Sum, AggregateKind::Min, AggregateKind::Max] {
                let decimal = from_input(kind, Value::Decimal(Decimal::from(1_u64)));
                let wide = from_input(kind, Value::U256(U256::from(1_u64)));
                assert_ne!(
                    AggregateCacheKey::from_aggregate_expr(&decimal, work).unwrap(),
                    AggregateCacheKey::from_aggregate_expr(&wide, work).unwrap(),
                );
                for aggregate in [decimal, wide] {
                    assert_eq!(
                        AggregateCacheKey::from_aggregate_expr(&aggregate, work).unwrap(),
                        AggregateCacheKey::from_group_aggregate_spec(
                            &GroupAggregateSpec::from_aggregate_expr(aggregate.clone()),
                            work
                        )
                        .unwrap(),
                    );
                }
            }
            for value in [Value::Nat64(1), Value::U256(U256::from(1_u64))] {
                assert_eq!(
                    AggregateCacheKey::from_aggregate_expr(&aggregate::count(), work).unwrap(),
                    AggregateCacheKey::from_aggregate_expr(
                        &from_input(AggregateKind::Count, value),
                        work
                    )
                    .unwrap(),
                );
            }
            assert_ne!(
                AggregateCacheKey::from_aggregate_expr(
                    &from_input(AggregateKind::Count, Value::Nat64(1)).distinct(),
                    work
                )
                .unwrap(),
                AggregateCacheKey::from_aggregate_expr(
                    &from_input(AggregateKind::Count, Value::U256(U256::from(1_u64))).distinct(),
                    work
                )
                .unwrap(),
            );
            for aggregate in [aggregate::min_by("amount"), aggregate::max_by("amount")] {
                assert_eq!(
                    AggregateCacheKey::from_aggregate_expr(&aggregate, work).unwrap(),
                    AggregateCacheKey::from_aggregate_expr(&aggregate.clone().distinct(), work)
                        .unwrap(),
                );
            }
            assert_eq!(
                AggregateCacheKey::from_aggregate_expr(
                    &from_input(AggregateKind::Sum, Value::Nat64(1)),
                    work
                )
                .unwrap(),
                AggregateCacheKey::from_aggregate_expr(
                    &from_input(AggregateKind::Sum, Value::Decimal(Decimal::from(1_u64)),),
                    work
                )
                .unwrap(),
            );
        });
    }

    #[test]
    fn aggregate_cache_retention_includes_both_structural_operands() {
        with_preparation_work(|work| {
            let field = "input".repeat(100);
            let filter = "filter".repeat(100);
            let aggregate =
                aggregate::sum(field.clone()).with_filter_expr(Expr::Field(filter.clone().into()));
            let key = AggregateCacheKey::from_aggregate_expr(&aggregate, work).unwrap();
            let bytes = RetainedBytes::measure(&key, usize::MAX).unwrap();
            let minimum = size_of::<AggregateCacheKey>()
                + 2 * size_of::<super::ProjectionExprCacheKey>()
                + field.len()
                + filter.len();
            assert!(bytes >= minimum);
            assert_eq!(RetainedBytes::measure(&key, bytes), Some(bytes));
            assert!(RetainedBytes::measure(&key, bytes - 1).is_none());
            let cloned = key.clone();
            assert_eq!(RetainedBytes::measure(&cloned, usize::MAX), Some(bytes));
            assert_eq!(key, cloned);
        });
    }

    #[test]
    fn scalar_and_grouped_aggregate_cache_identity_stays_shared() {
        with_preparation_work(|work| {
            let aggregate = aggregate::sum("amount")
                .with_filter_expr(Expr::Literal(Value::Bool(true)))
                .distinct();
            let grouped = GroupAggregateSpec::from_aggregate_expr(aggregate.clone());

            assert_eq!(
                AggregateCacheKey::from_aggregate_expr(&aggregate, work).unwrap(),
                AggregateCacheKey::from_group_aggregate_spec(&grouped, work).unwrap(),
            );

            let different_filter = aggregate::sum("amount")
                .with_filter_expr(Expr::Literal(Value::Bool(false)))
                .distinct();
            assert_ne!(
                AggregateCacheKey::from_aggregate_expr(&aggregate, work).unwrap(),
                AggregateCacheKey::from_aggregate_expr(&different_filter, work).unwrap(),
            );
            assert_ne!(
                AggregateCacheKey::from_aggregate_expr(&aggregate, work).unwrap(),
                AggregateCacheKey::from_aggregate_expr(
                    &aggregate::sum("other_amount").distinct(),
                    work
                )
                .unwrap(),
            );
            assert_ne!(
                AggregateCacheKey::from_aggregate_expr(&aggregate, work).unwrap(),
                AggregateCacheKey::from_aggregate_expr(&aggregate::sum("amount"), work).unwrap(),
            );
        });
    }
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(AggregateCacheKey {
Self{kind_tag,input_expr,filter_expr,distinct} => [kind_tag,input_expr,filter_expr,distinct],
});
crate::retained::retained_copy!(BinaryOpCacheKey);
crate::retained::retained_fields!(CaseWhenArmCacheKey {
Self{condition,result} => [condition,result],
});
crate::retained::retained_copy!(ConsistencyCacheKey);
crate::retained::retained_fields!(GroupingCacheKey {
Self{group_fields,aggregates,having_expr,max_groups,max_group_bytes} => [group_fields,aggregates,having_expr,max_groups,max_group_bytes],
});
crate::retained::retained_copy!(OrderDirectionCacheKey);
crate::retained::retained_fields!(OrderTermCacheKey {
Self{expr,direction} => [expr,direction],
});
crate::retained::retained_fields!(ProjectionCacheKey {
Self::All => [],
Self::Fields(field_0) => [field_0],
Self::Exprs(field_0) => [field_0],
});
crate::retained::retained_fields!(ProjectionExprCacheKey {
Self::Field(field_0) => [field_0],
Self::FieldPath{root,segments} => [root,segments],
Self::Literal(field_0) => [field_0],
Self::FunctionCall{function,args} => [function,args],
Self::Unary{op,expr} => [op,expr],
Self::Case{when_then_arms,else_expr} => [when_then_arms,else_expr],
Self::Binary{op,left,right} => [op,left,right],
Self::Aggregate(field_0) => [field_0],
});
crate::retained::retained_fields!(QueryModeCacheKey {
Self::Load{limit,offset} => [limit,offset],
Self::Delete{limit,offset} => [limit,offset],
});
crate::retained::retained_fields!(StructuralQueryCacheKey {
Self(data) => [data],
});
crate::retained::retained_fields!(StructuralQueryCacheKeyData {
Self{mode,predicate,parameter_contract,filter_expr,order,distinct,projection,grouping,consistency} => [mode,predicate,parameter_contract,filter_expr,order,distinct,projection,grouping,consistency],
});
crate::retained::retained_copy!(UnaryOpCacheKey);
crate::retained::retained_fields!(ValueCacheKey {
Self::Canonical(field_0) => [field_0],
});
