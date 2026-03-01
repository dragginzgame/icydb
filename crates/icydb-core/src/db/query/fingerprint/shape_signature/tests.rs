//! Module: query::fingerprint::continuation_signature
//! Responsibility: deterministic continuation-signature derivation from explain plans.
//! Does not own: continuation token decoding/validation.
//! Boundary: query-plan shape signature surface used by cursor token checks.

///
/// TESTS
///
use crate::{
    db::{
        access::AccessPath,
        contracts::{CompareOp, MissingRowPolicy, Predicate},
        cursor::{
            ContinuationSignature, ContinuationToken, CursorBoundary, CursorBoundarySlot,
            IndexRangeCursorAnchor, TokenWireError,
        },
        direction::Direction,
        query::{
            builder::field::FieldRef,
            intent::{KeyAccess, LoadSpec, QueryMode, access_plan_from_keys_value},
            plan::OrderDirection,
            plan::{
                AccessPlannedQuery, FieldSlot, GroupAggregateKind, GroupAggregateSpec,
                GroupHavingClause, GroupHavingSpec, GroupHavingSymbol, GroupSpec,
                GroupedExecutionConfig, LogicalPlan, OrderSpec, PageSpec,
            },
        },
    },
    model::index::IndexModel,
    types::Ulid,
    value::Value,
};
use std::fmt::Write;

fn signature_hex(signature: ContinuationSignature) -> String {
    let mut hex = String::with_capacity(64);
    for byte in signature.into_bytes() {
        let _ = write!(&mut hex, "{byte:02x}");
    }

    hex
}

#[test]
fn signature_is_deterministic_for_equivalent_predicates() {
    let id = Ulid::default();

    let predicate_a = Predicate::And(vec![
        FieldRef::new("id").eq(id),
        FieldRef::new("other").eq(Value::Text("x".to_string())),
    ]);
    let predicate_b = Predicate::And(vec![
        FieldRef::new("other").eq(Value::Text("x".to_string())),
        FieldRef::new("id").eq(id),
    ]);

    let mut plan_a: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan_a.scalar_plan_mut().predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan_b.scalar_plan_mut().predicate = Some(predicate_b);

    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_is_deterministic_for_by_keys() {
    let a = Ulid::from_u128(1);
    let b = Ulid::from_u128(2);

    let access_a = access_plan_from_keys_value(&KeyAccess::Many(vec![a, b, a]));
    let access_b = access_plan_from_keys_value(&KeyAccess::Many(vec![b, a]));

    let plan_a: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: access_a,
    };
    let plan_b: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: access_b,
    };

    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_excludes_pagination_window_state() {
    let mut plan_a: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    let mut plan_b: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);

    plan_a.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(10),
        offset: 0,
    });
    plan_b.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(10),
        offset: 999,
    });

    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_order_changes() {
    let mut plan_a: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    let mut plan_b: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);

    plan_a.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("name".to_string(), OrderDirection::Asc)],
    });
    plan_b.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("name".to_string(), OrderDirection::Desc)],
    });

    assert_ne!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_order_field_set_changes() {
    let mut plan_a: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    let mut plan_b: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);

    plan_a.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("name".to_string(), OrderDirection::Asc)],
    });
    plan_b.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("rank".to_string(), OrderDirection::Asc)],
    });

    assert_ne!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_distinct_flag_changes() {
    let plan_a: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    let mut plan_b: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan_b.scalar_plan_mut().distinct = true;

    assert_ne!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_with_entity_path() {
    let plan: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);

    assert_ne!(
        plan.continuation_signature("tests::EntityA"),
        plan.continuation_signature("tests::EntityB")
    );
}

#[test]
fn signature_changes_when_group_fields_change() {
    let grouped_a: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![
                    FieldSlot::from_parts_for_test(1, "tenant"),
                    FieldSlot::from_parts_for_test(2, "phase"),
                ],
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });
    let grouped_b: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![
                    FieldSlot::from_parts_for_test(1, "tenant"),
                    FieldSlot::from_parts_for_test(2, "region"),
                ],
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });

    assert_ne!(
        grouped_a.continuation_signature("tests::Entity"),
        grouped_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_group_aggregate_spec_changes() {
    let grouped_count: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });
    let grouped_max_rank: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Max,
                    target_field: Some("rank".to_string()),
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });

    assert_ne!(
        grouped_count.continuation_signature("tests::Entity"),
        grouped_max_rank.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_group_aggregate_target_field_changes() {
    let grouped_max_rank: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Max,
                    target_field: Some("rank".to_string()),
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });
    let grouped_max_score: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Max,
                    target_field: Some("score".to_string()),
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });

    assert_ne!(
        grouped_max_rank.continuation_signature("tests::Entity"),
        grouped_max_score.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_group_aggregate_distinct_changes() {
    let grouped_count: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });
    let grouped_count_distinct: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Count,
                    target_field: None,
                    distinct: true,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });

    assert_ne!(
        grouped_count.continuation_signature("tests::Entity"),
        grouped_count_distinct.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_between_sum_and_sum_distinct_grouped_shapes() {
    let grouped_sum: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: Vec::new(),
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Sum,
                    target_field: Some("rank".to_string()),
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(1, 1024),
            });
    let grouped_sum_distinct: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: Vec::new(),
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Sum,
                    target_field: Some("rank".to_string()),
                    distinct: true,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(1, 1024),
            });

    assert_ne!(
        grouped_sum.continuation_signature("tests::Entity"),
        grouped_sum_distinct.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_group_field_order_changes() {
    let grouped_ab: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![
                    FieldSlot::from_parts_for_test(1, "tenant"),
                    FieldSlot::from_parts_for_test(2, "phase"),
                ],
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });
    let grouped_ba: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![
                    FieldSlot::from_parts_for_test(2, "phase"),
                    FieldSlot::from_parts_for_test(1, "tenant"),
                ],
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });

    assert_ne!(
        grouped_ab.continuation_signature("tests::Entity"),
        grouped_ba.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_group_aggregate_order_changes() {
    let grouped_count_then_max: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![
                    GroupAggregateSpec {
                        kind: GroupAggregateKind::Count,
                        target_field: None,
                        distinct: false,
                    },
                    GroupAggregateSpec {
                        kind: GroupAggregateKind::Max,
                        target_field: Some("rank".to_string()),
                        distinct: false,
                    },
                ],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });
    let grouped_max_then_count: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![
                    GroupAggregateSpec {
                        kind: GroupAggregateKind::Max,
                        target_field: Some("rank".to_string()),
                        distinct: false,
                    },
                    GroupAggregateSpec {
                        kind: GroupAggregateKind::Count,
                        target_field: None,
                        distinct: false,
                    },
                ],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });

    assert_ne!(
        grouped_count_then_max.continuation_signature("tests::Entity"),
        grouped_max_then_count.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_between_scalar_and_grouped_shape() {
    let scalar: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    let grouped: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });

    assert_ne!(
        scalar.continuation_signature("tests::Entity"),
        grouped.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_grouped_limits_change() {
    let grouped_a: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });
    let grouped_b: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(128, 4096),
            });

    assert_ne!(
        grouped_a.continuation_signature("tests::Entity"),
        grouped_b.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_changes_when_grouped_having_changes() {
    let grouped_having_gt: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped_with_having(
                GroupSpec {
                    group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                    aggregates: vec![GroupAggregateSpec {
                        kind: GroupAggregateKind::Count,
                        target_field: None,
                        distinct: false,
                    }],
                    execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
                },
                Some(GroupHavingSpec {
                    clauses: vec![GroupHavingClause {
                        symbol: GroupHavingSymbol::AggregateIndex(0),
                        op: CompareOp::Gt,
                        value: Value::Uint(1),
                    }],
                }),
            );
    let grouped_having_gte: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped_with_having(
                GroupSpec {
                    group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                    aggregates: vec![GroupAggregateSpec {
                        kind: GroupAggregateKind::Count,
                        target_field: None,
                        distinct: false,
                    }],
                    execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
                },
                Some(GroupHavingSpec {
                    clauses: vec![GroupHavingClause {
                        symbol: GroupHavingSymbol::AggregateIndex(0),
                        op: CompareOp::Gte,
                        value: Value::Uint(1),
                    }],
                }),
            );

    assert_ne!(
        grouped_having_gt.continuation_signature("tests::Entity"),
        grouped_having_gte.continuation_signature("tests::Entity")
    );
}

#[test]
fn signature_snapshot_grouped_having_shape_is_stable() {
    let grouped_having: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped_with_having(
                GroupSpec {
                    group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                    aggregates: vec![GroupAggregateSpec {
                        kind: GroupAggregateKind::Count,
                        target_field: None,
                        distinct: false,
                    }],
                    execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
                },
                Some(GroupHavingSpec {
                    clauses: vec![GroupHavingClause {
                        symbol: GroupHavingSymbol::AggregateIndex(0),
                        op: CompareOp::Gt,
                        value: Value::Uint(1),
                    }],
                }),
            );
    let signature = signature_hex(grouped_having.continuation_signature("tests::Entity"));
    let expected = "e028bd8e57cc2a17014c91da49c72ff2a83942d231c5ef9502dc32a5ee940651".to_string();

    assert_eq!(
        signature, expected,
        "grouped+having signature snapshot drifted: actual={signature}",
    );
}

#[test]
fn signature_snapshot_grouped_distinct_shape_is_stable() {
    let grouped_distinct: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Count,
                    target_field: None,
                    distinct: true,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
            });
    let signature = signature_hex(grouped_distinct.continuation_signature("tests::Entity"));
    let expected = "37b689432bb60afc74c388e1a60cb9c142990cc55f448e5988850c7f7b20680d".to_string();

    assert_eq!(
        signature, expected,
        "grouped+distinct signature snapshot drifted: actual={signature}",
    );
}

#[test]
fn signature_snapshot_global_distinct_sum_shape_is_stable() {
    let global_distinct_sum: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: Vec::new(),
                aggregates: vec![GroupAggregateSpec {
                    kind: GroupAggregateKind::Sum,
                    target_field: Some("rank".to_string()),
                    distinct: true,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(1, 1024),
            });
    let signature = signature_hex(global_distinct_sum.continuation_signature("tests::Entity"));
    let expected = "1db8287da47b5b6d2042d1e413eca43b448a7d6a5222c9612372d430cf871429".to_string();

    assert_eq!(
        signature, expected,
        "global distinct sum signature snapshot drifted: actual={signature}",
    );
}

#[test]
fn signature_snapshot_ordered_group_hint_shape_is_stable() {
    let grouped_ordered: AccessPlannedQuery<Value> = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: IndexModel::new("idx_tenant", "tests", &["tenant"], false),
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    )
    .into_grouped(GroupSpec {
        group_fields: vec![FieldSlot::from_parts_for_test(1, "tenant")],
        aggregates: vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
    });
    let signature = signature_hex(grouped_ordered.continuation_signature("tests::Entity"));
    let expected = "0ca59a1431dc0d0f45a7549a7fb8004696f74c5f3591916589f564c82896e875".to_string();

    assert_eq!(
        signature, expected,
        "ordered-hint grouped signature snapshot drifted: actual={signature}",
    );
}

#[test]
fn continuation_token_round_trips_index_range_anchor() {
    let raw_key = vec![0xAA, 0xBB, 0xCC];
    let boundary = CursorBoundary {
        slots: vec![CursorBoundarySlot::Present(Value::Uint(42))],
    };
    let signature = ContinuationSignature::from_bytes([7u8; 32]);

    let token = ContinuationToken::new_index_range_with_direction(
        signature,
        boundary.clone(),
        IndexRangeCursorAnchor::new(raw_key.clone()),
        Direction::Asc,
        3,
    );

    let encoded = token
        .encode()
        .expect("token with index-range anchor encodes");
    let decoded =
        ContinuationToken::decode(&encoded).expect("token with index-range anchor decodes");

    assert_eq!(decoded.signature(), signature);
    assert_eq!(decoded.boundary(), &boundary);
    assert_eq!(decoded.initial_offset(), 3);
    let decoded_anchor = decoded
        .index_range_anchor()
        .expect("decoded token should include index-range anchor");
    assert_eq!(decoded_anchor.last_raw_key(), raw_key.as_slice());
}

#[test]
fn continuation_token_decode_rejects_unknown_version() {
    let boundary = CursorBoundary {
        slots: vec![CursorBoundarySlot::Present(Value::Uint(1))],
    };
    let signature = ContinuationSignature::from_bytes([3u8; 32]);
    let token = ContinuationToken::new_with_direction(signature, boundary, Direction::Asc, 9);
    let encoded = token
        .encode_with_version_for_test(99)
        .expect("unknown-version wire token should encode");

    let err = ContinuationToken::decode(&encoded).expect_err("unknown version must fail");
    assert_eq!(err, TokenWireError::UnsupportedVersion { version: 99 });
}

#[test]
fn continuation_token_v1_decodes_initial_offset_as_zero() {
    let boundary = CursorBoundary {
        slots: vec![CursorBoundarySlot::Present(Value::Uint(1))],
    };
    let signature = ContinuationSignature::from_bytes([4u8; 32]);
    let token = ContinuationToken::new_with_direction(signature, boundary, Direction::Desc, 11);
    let encoded = token
        .encode_with_version_for_test(1)
        .expect("v1 wire token should encode");

    let decoded = ContinuationToken::decode(&encoded).expect("v1 wire token should decode");
    assert_eq!(
        decoded.initial_offset(),
        0,
        "v1 must decode with zero offset"
    );
    assert_eq!(decoded.direction(), Direction::Desc);
}
