//! Resolve complete wrapper graphs without cycle-tracking allocations.

use super::{
    AcceptedCompositeShape, AcceptedFieldKind, CompositeTypeId, catalog, composite, newtype,
};

// A tiny independent graph oracle reports the terminal definition's index.
// The graph has at most four definitions; terminal choices are scalar, list,
// record and missing definition, in that order after the edge choices.
fn terminal(graph: &[usize], mut node: usize) -> Option<usize> {
    let mut seen = [false; 4];
    loop {
        if seen[node] {
            return None;
        }
        seen[node] = true;
        let choice = graph[node];
        if choice >= graph.len() {
            return (choice < graph.len() + 2).then_some(node);
        }
        node = choice;
    }
}

#[test]
fn resolution_matches_every_small_wrapper_graph_and_borrows_its_terminal_kind() {
    for count in 1_usize..=4 {
        let choices = count + 4;
        for mut encoding in 0..choices.pow(u32::try_from(count).unwrap()) {
            let graph = (0..count)
                .map(|_| {
                    let choice = encoding % choices;
                    encoding /= choices;
                    choice
                })
                .collect::<Vec<_>>();
            let shapes = graph
                .iter()
                .map(|&choice| match choice.checked_sub(count) {
                    None => newtype(composite(u32::try_from(choice + 1).unwrap())),
                    Some(0) => newtype(AcceptedFieldKind::Nat64),
                    Some(1) => newtype(AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64))),
                    Some(2) => AcceptedCompositeShape::Record(Vec::new()),
                    Some(_) => newtype(composite(99)),
                })
                .collect();
            let catalog = catalog(shapes);
            for root in 0..count {
                let input = composite(u32::try_from(root + 1).unwrap());
                let actual = catalog.resolve_newtype_value_kind(&input);
                if let Some(index) = terminal(&graph, root) {
                    let definition = catalog
                        .composite_type(
                            CompositeTypeId::new(u32::try_from(index + 1).unwrap()).unwrap(),
                        )
                        .unwrap();
                    let AcceptedCompositeShape::Newtype(inner) = definition.shape() else {
                        panic!("oracle terminal must be a scalar or list wrapper");
                    };
                    assert!(
                        std::ptr::eq(actual.unwrap(), inner.kind()),
                        "{graph:?}: {root}"
                    );
                } else {
                    assert!(actual.is_none(), "{graph:?}: {root}");
                }
            }
        }
    }
}

#[test]
fn resolution_keeps_empty_catalog_and_complete_chain_boundaries_without_a_depth_ceiling() {
    let empty = catalog(Vec::new());
    let direct = AcceptedFieldKind::Nat64;
    assert!(std::ptr::eq(
        empty.resolve_newtype_value_kind(&direct).unwrap(),
        &raw const direct,
    ));
    assert!(empty.resolve_newtype_value_kind(&composite(1)).is_none());

    // This exceeds the separate recursive-value limit: newtype resolution has
    // always followed the finite catalog rather than imposing that other limit.
    let count = u32::try_from(crate::db::schema::MAX_ACCEPTED_RECURSIVE_DEPTH + 1).unwrap();
    let mut shapes = (1..=count)
        .map(|id| {
            newtype(if id == count {
                AcceptedFieldKind::Text { max_len: Some(64) }
            } else {
                composite(id + 1)
            })
        })
        .collect::<Vec<_>>();
    let complete = catalog(shapes.clone());
    for root in 1..=count {
        assert_eq!(
            complete.resolve_newtype_value_kind(&composite(root)),
            Some(&AcceptedFieldKind::Text { max_len: Some(64) }),
        );
    }
    *shapes.last_mut().unwrap() = newtype(composite(count / 2));
    let cyclic = catalog(shapes);
    for root in 1..=count {
        assert!(
            cyclic
                .resolve_newtype_value_kind(&composite(root))
                .is_none()
        );
    }
}
