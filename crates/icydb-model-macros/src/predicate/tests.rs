use super::{CompareOp, CompareOperand, Literal, Predicate, parse, referenced_fields};

#[test]
fn parses_current_generated_predicate_forms() {
    let predicate =
        parse("active = true AND balance BETWEEN 0 AND 10 OR tier IN ('Bronze', 'Gold')")
            .expect("current generated predicate forms should parse");

    assert_eq!(referenced_fields(&predicate), ["active", "balance", "tier"]);
}

#[test]
fn normalizes_literal_leading_order() {
    assert_eq!(
        parse("5 < age").expect("literal-leading comparison should parse"),
        Predicate::Compare {
            field: "age".to_string(),
            op: CompareOp::Gt,
            operand: CompareOperand::Literal(Literal::Number("5".to_string())),
        }
    );
}

#[test]
fn rejects_unbounded_or_unsupported_syntax() {
    assert!(parse("").is_err());
    assert!(parse("LOWER(name) = 'a'").is_err());
    assert!(parse("id IN ()").is_err());
    assert!(parse("value = NULL").is_err());
}

#[test]
fn boolean_tests_count_implicit_nodes_at_the_depth_boundary() {
    for suffix in ["IS TRUE", "IS FALSE", "IS NOT TRUE", "IS NOT FALSE"] {
        let implicit_levels = if suffix.starts_with("IS NOT") { 2 } else { 1 };
        let admitted_nots = super::MAX_PREDICATE_DEPTH - 1 - implicit_levels;
        assert!(parse(&format!("{}active {suffix}", "NOT ".repeat(admitted_nots))).is_ok());
        assert!(
            parse(&format!(
                "{}active {suffix}",
                "NOT ".repeat(admitted_nots + 1)
            ))
            .is_err()
        );
    }
    // NULL tests are leaf nodes, unlike Boolean tests' explicit null guard.
    for suffix in ["IS NULL", "IS NOT NULL", "= true"] {
        assert!(
            parse(&format!(
                "{}active {suffix}",
                "NOT ".repeat(super::MAX_PREDICATE_DEPTH - 1)
            ))
            .is_ok()
        );
    }
}

#[test]
fn lowered_boolean_and_membership_groups_count_toward_depth() {
    for join in ["AND", "OR"] {
        for suffix in [
            "active = true",
            "id IN (1)",
            "id IN (1, 2)",
            "id NOT IN (1, 2)",
        ] {
            let implicit_levels = match suffix {
                "id IN (1, 2)" => 1,
                "id NOT IN (1, 2)" => 2,
                _ => 0,
            };
            let admitted_nots = super::MAX_PREDICATE_DEPTH - 2 - implicit_levels;
            let admitted = format!("id = 0 {join} {}{suffix}", "NOT ".repeat(admitted_nots));
            let rejected = format!("id = 0 {join} {}{suffix}", "NOT ".repeat(admitted_nots + 1));
            assert!(parse(&admitted).is_ok(), "{admitted}");
            assert!(parse(&rejected).is_err(), "{rejected}");
        }
    }
    // Membership lowers to a flattened comparison group, not a binary
    // chain whose depth grows with the number of items.
    let values = (0..64).map(|n| n.to_string()).collect::<Vec<_>>().join(",");
    assert!(parse(&format!("id IN ({values})")).is_ok());
}

#[test]
fn membership_negation_counts_its_implicit_node_at_the_depth_boundary() {
    let admitted_nots = super::MAX_PREDICATE_DEPTH - 2;
    assert!(parse(&format!("{}id NOT IN (1)", "NOT ".repeat(admitted_nots))).is_ok());
    assert!(
        parse(&format!(
            "{}id NOT IN (1)",
            "NOT ".repeat(admitted_nots + 1)
        ))
        .is_err()
    );
    assert!(parse(&format!("{}id IN (1)", "NOT ".repeat(admitted_nots + 1))).is_ok());
}
