use super::*;

#[test]
fn execute_sql_projection_filtered_equivalent_strict_prefix_forms_match_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered-index dataset where only the
    // guarded active rows should participate in the bounded `br...` window.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "bristle", true, 30),
            (9_204, "broom", false, 40),
            (9_205, "charlie", true, 50),
        ],
    );

    // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
    // ranges to return the same guarded filtered-index rows.
    let like_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 2",
    )
    .expect("filtered strict LIKE prefix projection should execute");
    let starts_with_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name ASC, id ASC LIMIT 2",
    )
    .expect("filtered direct STARTS_WITH projection should execute");
    let range_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name ASC, id ASC LIMIT 2",
    )
    .expect("filtered strict text-range projection should execute");

    let expected_rows = vec![
        vec![Value::Text("bravo".to_string())],
        vec![Value::Text("bristle".to_string())],
    ];

    assert_eq!(
        like_rows, expected_rows,
        "filtered strict LIKE prefix projection must return the guarded bounded rows",
    );
    assert_eq!(
        starts_with_rows, like_rows,
        "filtered direct STARTS_WITH projection must preserve row parity with strict LIKE",
    );
    assert_eq!(
        range_rows, like_rows,
        "filtered strict text-range projection must preserve row parity with strict LIKE",
    );
}

#[test]
fn execute_sql_projection_filtered_equivalent_desc_strict_prefix_forms_match_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered-index dataset where the same
    // guarded `br...` window can prove descending parity across the accepted
    // bounded text spellings.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "bristle", true, 30),
            (9_204, "broom", false, 40),
            (9_205, "charlie", true, 50),
        ],
    );

    // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
    // ranges to return the same reverse guarded filtered-index window.
    let like_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered strict LIKE prefix projection should execute");
    let starts_with_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered direct STARTS_WITH projection should execute");
    let range_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered strict text-range projection should execute");

    let expected_rows = vec![
        vec![Value::Text("bristle".to_string())],
        vec![Value::Text("bravo".to_string())],
    ];

    assert_eq!(
        like_rows, expected_rows,
        "descending filtered strict LIKE prefix projection must return the guarded reverse bounded rows",
    );
    assert_eq!(
        starts_with_rows, like_rows,
        "descending filtered direct STARTS_WITH projection must preserve row parity with strict LIKE",
    );
    assert_eq!(
        range_rows, like_rows,
        "descending filtered strict text-range projection must preserve row parity with strict LIKE",
    );
}

#[test]
fn session_explain_execution_filtered_equivalent_strict_prefix_forms_preserve_covering_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered-index dataset so the guarded
    // bounded text forms all target the same filtered secondary index window.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "bristle", true, 30),
            (9_204, "broom", false, 40),
            (9_205, "charlie", true, 50),
        ],
    );

    // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
    // ranges to preserve the same covering filtered index-range route.
    let queries = [
        (
            "filtered strict LIKE prefix",
            "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 2",
        ),
        (
            "filtered direct STARTS_WITH",
            "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name ASC, id ASC LIMIT 2",
        ),
        (
            "filtered strict text range",
            "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name ASC, id ASC LIMIT 2",
        ),
    ];

    for (context, sql) in queries {
        let descriptor = session
            .query_from_sql::<FilteredIndexedSessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"))
            .explain_execution()
            .unwrap_or_else(|err| {
                panic!("{context} SQL explain_execution should succeed: {err:?}")
            });

        assert_eq!(
            descriptor.node_type(),
            ExplainExecutionNodeType::IndexRangeScan,
            "{context} queries should stay on the shared index-range root",
        );
        assert_eq!(
            descriptor.covering_scan(),
            Some(true),
            "{context} projections should keep the explicit covering-read route",
        );
        assert_eq!(
            descriptor.node_properties().get("cov_read_route"),
            Some(&Value::Text("covering_read".to_string())),
            "{context} explain roots should expose the covering-read route label",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::SecondaryOrderPushdown
            )
            .is_some(),
            "{context} roots should report secondary order pushdown",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::OrderByAccessSatisfied
            )
            .is_some(),
            "{context} roots should report access-satisfied ordering",
        );
    }
}

#[test]
fn session_explain_execution_filtered_equivalent_desc_strict_prefix_forms_preserve_covering_route()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered-index dataset so the guarded
    // bounded text forms all target the same reverse filtered secondary index
    // window.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "bristle", true, 30),
            (9_204, "broom", false, 40),
            (9_205, "charlie", true, 50),
        ],
    );

    // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
    // ranges to preserve the same reverse covering filtered index-range route.
    let queries = [
        (
            "descending filtered strict LIKE prefix",
            "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 2",
        ),
        (
            "descending filtered direct STARTS_WITH",
            "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name DESC, id DESC LIMIT 2",
        ),
        (
            "descending filtered strict text range",
            "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name DESC, id DESC LIMIT 2",
        ),
    ];

    for (context, sql) in queries {
        let descriptor = session
            .query_from_sql::<FilteredIndexedSessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"))
            .explain_execution()
            .unwrap_or_else(|err| {
                panic!("{context} SQL explain_execution should succeed: {err:?}")
            });

        assert_eq!(
            descriptor.node_type(),
            ExplainExecutionNodeType::IndexRangeScan,
            "{context} queries should stay on the shared reverse index-range root",
        );
        assert_eq!(
            descriptor.covering_scan(),
            Some(true),
            "{context} projections should keep the explicit reverse covering-read route",
        );
        assert_eq!(
            descriptor.node_properties().get("cov_read_route"),
            Some(&Value::Text("covering_read".to_string())),
            "{context} explain roots should expose the reverse covering-read route label",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::SecondaryOrderPushdown
            )
            .is_some(),
            "{context} roots should report secondary order pushdown",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::OrderByAccessSatisfied
            )
            .is_some(),
            "{context} roots should report access-satisfied reverse ordering",
        );
    }
}

#[test]
fn execute_sql_projection_filtered_composite_equivalent_strict_prefix_forms_match_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset where
    // the guard, equality prefix, and bounded text window together isolate one
    // ordered `(tier, handle)` range on the filtered secondary index.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
    // ranges to return the same guarded equality-prefix filtered window.
    let like_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
    )
    .expect("filtered composite strict LIKE prefix projection should execute");
    let starts_with_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle ASC, id ASC LIMIT 2",
    )
    .expect("filtered composite direct STARTS_WITH projection should execute");
    let range_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle ASC, id ASC LIMIT 2",
    )
    .expect("filtered composite strict text-range projection should execute");

    let expected_rows = vec![
        vec![
            Value::Text("gold".to_string()),
            Value::Text("bravo".to_string()),
        ],
        vec![
            Value::Text("gold".to_string()),
            Value::Text("bristle".to_string()),
        ],
    ];

    assert_eq!(
        like_rows, expected_rows,
        "filtered composite strict LIKE prefix projection must return the guarded equality-prefix rows",
    );
    assert_eq!(
        starts_with_rows, like_rows,
        "filtered composite direct STARTS_WITH projection must preserve row parity with strict LIKE",
    );
    assert_eq!(
        range_rows, like_rows,
        "filtered composite strict text-range projection must preserve row parity with strict LIKE",
    );
}

#[test]
fn execute_sql_projection_filtered_composite_equivalent_desc_strict_prefix_forms_match_guarded_rows()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset where
    // the reverse bounded window still depends on the same guard and equality
    // prefix before traversing the `handle` suffix.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
    // ranges to return the same reverse guarded equality-prefix window.
    let like_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered composite strict LIKE prefix projection should execute");
    let starts_with_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered composite direct STARTS_WITH projection should execute");
    let range_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered composite strict text-range projection should execute");

    let expected_rows = vec![
        vec![
            Value::Text("gold".to_string()),
            Value::Text("bristle".to_string()),
        ],
        vec![
            Value::Text("gold".to_string()),
            Value::Text("bravo".to_string()),
        ],
    ];

    assert_eq!(
        like_rows, expected_rows,
        "descending filtered composite strict LIKE prefix projection must return the guarded reverse equality-prefix rows",
    );
    assert_eq!(
        starts_with_rows, like_rows,
        "descending filtered composite direct STARTS_WITH projection must preserve row parity with strict LIKE",
    );
    assert_eq!(
        range_rows, like_rows,
        "descending filtered composite strict text-range projection must preserve row parity with strict LIKE",
    );
}

#[test]
fn session_explain_execution_filtered_composite_equivalent_strict_prefix_forms_preserve_covering_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset so the
    // guard and equality prefix drive the same bounded `handle` suffix window.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the three accepted bounded text spellings to preserve
    // one shared composite filtered covering route with a one-slot equality prefix.
    let queries = [
        (
            "filtered composite strict LIKE prefix",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
        ),
        (
            "filtered composite direct STARTS_WITH",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle ASC, id ASC LIMIT 2",
        ),
        (
            "filtered composite strict text range",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle ASC, id ASC LIMIT 2",
        ),
    ];

    for (context, sql) in queries {
        let descriptor = session
            .query_from_sql::<FilteredIndexedSessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"))
            .explain_execution()
            .unwrap_or_else(|err| {
                panic!("{context} SQL explain_execution should succeed: {err:?}")
            });

        assert_eq!(
            descriptor.node_type(),
            ExplainExecutionNodeType::IndexRangeScan,
            "{context} queries should stay on the shared composite index-range root",
        );
        assert_eq!(
            descriptor.covering_scan(),
            Some(true),
            "{context} projections should keep the explicit composite covering-read route",
        );
        assert_eq!(
            descriptor.node_properties().get("cov_read_route"),
            Some(&Value::Text("covering_read".to_string())),
            "{context} explain roots should expose the composite covering-read route label",
        );
        assert_eq!(
            descriptor.node_properties().get("prefix_len"),
            Some(&Value::Uint(1)),
            "{context} explain roots should report one equality-prefix slot before the bounded text suffix",
        );
        assert_eq!(
            descriptor.node_properties().get("prefix_values"),
            Some(&Value::List(vec![Value::Text("gold".to_string())])),
            "{context} explain roots should expose the concrete equality-prefix value before the bounded text suffix",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::SecondaryOrderPushdown
            )
            .is_some(),
            "{context} roots should report secondary order pushdown",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::OrderByAccessSatisfied
            )
            .is_some(),
            "{context} roots should report access-satisfied suffix ordering",
        );
    }
}

#[test]
fn session_explain_execution_filtered_composite_equivalent_desc_strict_prefix_forms_preserve_covering_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset so the
    // reverse bounded route still depends on the same guard and equality prefix.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the reverse bounded text spellings to preserve one
    // shared composite filtered covering route with the same equality prefix.
    let queries = [
        (
            "descending filtered composite strict LIKE prefix",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
        ),
        (
            "descending filtered composite direct STARTS_WITH",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle DESC, id DESC LIMIT 2",
        ),
        (
            "descending filtered composite strict text range",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle DESC, id DESC LIMIT 2",
        ),
    ];

    for (context, sql) in queries {
        let descriptor = session
            .query_from_sql::<FilteredIndexedSessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"))
            .explain_execution()
            .unwrap_or_else(|err| {
                panic!("{context} SQL explain_execution should succeed: {err:?}")
            });

        assert_eq!(
            descriptor.node_type(),
            ExplainExecutionNodeType::IndexRangeScan,
            "{context} queries should stay on the shared reverse composite index-range root",
        );
        assert_eq!(
            descriptor.covering_scan(),
            Some(true),
            "{context} projections should keep the explicit reverse composite covering-read route",
        );
        assert_eq!(
            descriptor.node_properties().get("cov_read_route"),
            Some(&Value::Text("covering_read".to_string())),
            "{context} explain roots should expose the reverse composite covering-read route label",
        );
        assert_eq!(
            descriptor.node_properties().get("prefix_len"),
            Some(&Value::Uint(1)),
            "{context} explain roots should report the same equality-prefix slot before reverse bounded traversal",
        );
        assert_eq!(
            descriptor.node_properties().get("prefix_values"),
            Some(&Value::List(vec![Value::Text("gold".to_string())])),
            "{context} explain roots should expose the same concrete equality-prefix value before reverse bounded traversal",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::SecondaryOrderPushdown
            )
            .is_some(),
            "{context} roots should report secondary order pushdown",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::OrderByAccessSatisfied
            )
            .is_some(),
            "{context} roots should report access-satisfied reverse suffix ordering",
        );
    }
}
