use super::{BinaryOp, CaseWhenArm, Expr, FieldId, Function, PathSpec, UnaryOp};

fn field(name: &str) -> Expr {
    Expr::Field(FieldId::new(name))
}

fn label(expr: &Expr) -> &str {
    match expr {
        Expr::Field(field) => field.as_str(),
        Expr::Case { .. } => "case",
        Expr::Binary { .. } => "binary",
        Expr::FunctionCall { .. } => "function",
        Expr::Unary { .. } => "unary",
        _ => "leaf",
    }
}

#[test]
fn preorder_visitors_share_child_order_and_stop_before_later_children() {
    let tree = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(field("a")),
                right: Box::new(field("b")),
            },
            Expr::FunctionCall {
                function: Function::Lower,
                args: vec![
                    Expr::Unary {
                        op: UnaryOp::Not,
                        expr: Box::new(field("c")),
                    },
                    field("d"),
                ],
            },
        )],
        else_expr: Box::new(field("else")),
    };
    let mut infallible = Vec::new();
    tree.for_each_tree_expr(&mut |node| infallible.push(label(node).to_owned()));
    assert_eq!(
        infallible,
        [
            "case", "binary", "a", "b", "function", "unary", "c", "d", "else"
        ]
    );
    let mut fallible = Vec::new();
    tree.try_for_each_tree_expr(&mut |node| {
        fallible.push(label(node).to_owned());
        Ok::<_, u8>(())
    })
    .unwrap();
    assert_eq!(fallible, infallible);
    let mut stopped = Vec::new();
    assert!(
        !tree
            .try_all_tree_expr(&mut |node| {
                stopped.push(label(node).to_owned());
                Ok::<_, u8>(label(node) != "b")
            })
            .unwrap()
    );
    assert_eq!(stopped, ["case", "binary", "a", "b"]);
    let mut failed = Vec::new();
    assert_eq!(
        tree.try_for_each_tree_expr(&mut |node| {
            failed.push(label(node).to_owned());
            if label(node) == "b" { Err(7u8) } else { Ok(()) }
        }),
        Err(7)
    );
    assert_eq!(failed, stopped);
}

#[test]
fn compare_index_visitor_remains_postorder_for_every_comparison() {
    for op in [
        BinaryOp::Eq,
        BinaryOp::Ne,
        BinaryOp::Lt,
        BinaryOp::Lte,
        BinaryOp::Gt,
        BinaryOp::Gte,
    ] {
        let tree = Expr::Binary {
            op,
            left: Box::new(field("a")),
            right: Box::new(field("b")),
        };
        let mut next = 0;
        let mut visited = Vec::new();
        tree.try_for_each_tree_expr_with_compare_index(&mut next, &mut |index, node| {
            visited.push((index, label(node).to_owned()));
            Ok::<_, u8>(())
        })
        .unwrap();
        assert_eq!(
            visited,
            vec![(0, "a".into()), (0, "b".into()), (0, "binary".into())]
        );
        assert_eq!(next, 1);
    }
}

#[test]
fn path_spec_renders_one_canonical_dotted_label() {
    let path = PathSpec::new(
        "profile",
        vec!["location".to_string(), "country".to_string()],
    );

    assert_eq!(path.dotted_label(), "profile.location.country");
}

#[cfg(feature = "sql")]
#[test]
fn path_label_comparison_matches_rendering_without_normalizing_components() {
    for (root, segments) in [
        ("profile", vec!["location", "country"]),
        ("profile.location", vec!["country"]),
        ("profile", vec!["location.country"]),
        ("", vec!["", "name"]),
        ("profile", vec![""]),
        ("é", vec!["名", "🌍"]),
    ] {
        let path = PathSpec::new(root, segments.into_iter().map(str::to_string).collect());
        let rendered = path.dotted_label();
        for label in [
            rendered.clone(),
            format!("{rendered}."),
            format!("x{rendered}"),
            rendered.trim_end_matches('.').to_string(),
            "profile.location.country".to_string(),
            String::new(),
        ] {
            assert_eq!(path.matches_dotted_label(&label), rendered == label);
        }
        // Check every valid UTF-8 truncation, including component interiors.
        for (end, _) in rendered.char_indices() {
            assert!(!path.matches_dotted_label(&rendered[..end]));
        }
    }
}
