use super::*;

#[test]
fn load_stream_construction_routes_through_route_facade() {
    let load_sources = [
        (
            "aggregate/mod.rs",
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/db/executor/tests/route/",
                "../../aggregate/mod.rs"
            )),
        ),
        (
            "load/execute.rs",
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/db/executor/tests/route/",
                "../../load/execute.rs"
            )),
        ),
        (
            "load/fast_stream.rs",
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/db/executor/tests/route/",
                "../../load/fast_stream.rs"
            )),
        ),
        (
            "load/index_range_limit.rs",
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/db/executor/tests/route/",
                "../../load/index_range_limit.rs"
            )),
        ),
        (
            "load/pk_stream.rs",
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/db/executor/tests/route/",
                "../../load/pk_stream.rs"
            )),
        ),
        (
            "load/secondary_index.rs",
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/db/executor/tests/route/",
                "../../load/secondary_index.rs"
            )),
        ),
    ];

    for (path, source) in load_sources {
        assert!(
            !source_uses_direct_context_stream_construction(source),
            "{path} must construct streams via route::RoutedKeyStreamRequest facade",
        );
    }
}

#[test]
fn aggregate_fast_path_dispatch_requires_verified_gate_marker() {
    let aggregate_fast_path_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../aggregate/fast_path.rs"
    ));
    assert!(
        aggregate_fast_path_source.contains("struct VerifiedAggregateFastPathRoute"),
        "aggregate fast-path dispatch must define a verified route marker type",
    );
    assert!(
        aggregate_fast_path_source.contains("fn verify_aggregate_fast_path_eligibility<E>("),
        "aggregate fast-path dispatch must include one shared eligibility verifier",
    );
    assert!(
        aggregate_fast_path_source
            .contains("Result<Option<VerifiedAggregateFastPathRoute>, InternalError>"),
        "aggregate fast-path eligibility verifier must return a verified route marker",
    );
    assert!(
        aggregate_fast_path_source.contains("fn try_execute_verified_aggregate_fast_path<E>("),
        "aggregate fast-path branch execution must flow through a verified-dispatch helper",
    );
    assert!(
        aggregate_fast_path_source.contains("try_first_verified_fast_path_hit("),
        "aggregate fast-path loop must route dispatch through the shared verified-hit helper",
    );
    assert!(
        aggregate_fast_path_source
            .contains("|route| Self::verify_aggregate_fast_path_eligibility(inputs, route)"),
        "aggregate fast-path loop must obtain a verified marker before branch execution",
    );
    assert!(
        aggregate_fast_path_source.contains(
            "|verified_route| Self::try_execute_verified_aggregate_fast_path(inputs, verified_route)"
        ),
        "aggregate fast-path loop must execute branches only through verified-dispatch helper",
    );
}

#[test]
fn aggregate_fast_path_folding_uses_shared_stream_helpers() {
    let aggregate_fast_path_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../aggregate/fast_path.rs"
    ));
    let kernel_aggregate_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../aggregate/fast_path.rs"
    ));

    assert!(
        aggregate_fast_path_source.contains("fn fold_aggregate_from_routed_stream_request"),
        "aggregate routed-stream aggregate folding must expose a shared helper",
    );
    assert!(
        aggregate_fast_path_source.contains("fn fold_aggregate_from_fast_path_result<E>("),
        "aggregate fast-path folding must expose a shared fast-path fold helper",
    );
    assert!(
        aggregate_fast_path_source.contains("Self::run_streaming_aggregate_reducer("),
        "aggregate fold helpers must route through the shared kernel reducer runner",
    );
    assert!(
        kernel_aggregate_source.contains("fn try_fold_secondary_index_aggregate"),
        "aggregate secondary-index probe/fallback folding must expose a shared helper",
    );
    assert_eq!(
        aggregate_fast_path_source
            .matches("Self::decorate_key_stream_for_plan(")
            .count(),
        1,
        "aggregate fast-path DISTINCT decoration should be wired in one helper only",
    );
    assert_eq!(
        kernel_aggregate_source
            .matches("LoadExecutor::<E>::resolve_routed_key_stream(")
            .count(),
        1,
        "aggregate routed-stream resolution should be centralized in one helper only",
    );
    assert_eq!(
        kernel_aggregate_source
            .matches("LoadExecutor::<E>::try_execute_secondary_index_order_stream(")
            .count(),
        1,
        "aggregate secondary-index stream resolution should be centralized in one helper only",
    );
    assert!(
        aggregate_fast_path_source
            .matches("fold_aggregate_from_fast_path_result(")
            .count()
            >= 2,
        "aggregate fast-path call sites should route through shared fast-path folding helper",
    );
    assert!(
        aggregate_fast_path_source
            .matches("fold_aggregate_from_routed_stream_request(")
            .count()
            >= 3,
        "aggregate routed-stream call sites should route through shared routed-stream helper",
    );
}
