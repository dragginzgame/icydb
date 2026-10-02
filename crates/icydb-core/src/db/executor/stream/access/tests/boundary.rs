use super::*;
use syn::visit::{self, Visit};

// Inspect Rust types so comments, formatting, qualification and local import
// renames cannot change whether a stream is erased at the page boundary.
fn source_erases_ordered_key_stream(source: &str) -> bool {
    #[derive(Default)]
    struct StreamTypes {
        aliases: BTreeSet<String>,
        erased: bool,
    }

    impl<'ast> Visit<'ast> for StreamTypes {
        fn visit_item_mod(&mut self, module: &'ast syn::ItemMod) {
            if module.attrs.iter().any(|attribute| {
                attribute.path().is_ident("cfg")
                    && attribute
                        .parse_args::<syn::Path>()
                        .is_ok_and(|path| path.is_ident("test"))
            }) {
                return;
            }
            visit::visit_item_mod(self, module);
        }

        fn visit_use_rename(&mut self, rename: &'ast syn::UseRename) {
            if rename.ident == "OrderedKeyStream" {
                self.aliases.insert(rename.rename.to_string());
            }
        }

        fn visit_type_trait_object(&mut self, object: &'ast syn::TypeTraitObject) {
            self.erased |= object.bounds.iter().any(|bound| {
                let syn::TypeParamBound::Trait(bound) = bound else {
                    return false;
                };
                bound.path.segments.last().is_some_and(|segment| {
                    segment.ident == "OrderedKeyStream"
                        || self.aliases.contains(&segment.ident.to_string())
                })
            });
            visit::visit_type_trait_object(self, object);
        }
    }

    let syntax = syn::parse_file(source).expect("guarded runtime source should parse");
    let mut streams = StreamTypes::default();
    // Resolve local import names before examining types, regardless of item order.
    streams.visit_file(&syntax);
    streams.erased = false;
    streams.visit_file(&syntax);
    streams.erased
}

#[test]
fn stream_access_module_limits_direct_store_traversal_to_scan_boundary() {
    let access_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/stream/access");
    let mut sources = Vec::new();
    collect_rust_sources(access_root.as_path(), &mut sources);
    sources.sort();

    let allowed = ["scan.rs"];
    for source_path in sources {
        if source_path
            .components()
            .any(|part| part.as_os_str() == "tests")
            || source_path
                .file_name()
                .is_some_and(|name| name == "tests.rs")
        {
            continue;
        }

        if source_path
            .file_name()
            .is_some_and(|name| allowed.contains(&name.to_string_lossy().as_ref()))
        {
            continue;
        }

        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        assert!(
            !source_uses_direct_store_or_registry_access(source.as_str()),
            "stream access file {} must not directly traverse store/registry; only scan boundary adapters may do so",
            source_path.display(),
        );
    }
}

#[test]
fn executor_runtime_modules_have_no_raw_access_path_variant_matching() {
    let executor_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor");
    let mut sources = Vec::new();
    collect_rust_sources(executor_root.as_path(), &mut sources);
    sources.sort();

    let mut violations = Vec::new();
    for source_path in sources {
        if source_path
            .components()
            .any(|part| part.as_os_str() == "tests")
            || source_path
                .file_name()
                .is_some_and(|name| name == "tests.rs")
        {
            continue;
        }

        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        let runtime_source = strip_cfg_test_items(source.as_str());
        if source_uses_raw_access_path_variant_matching(runtime_source.as_str()) {
            violations.push(source_path);
        }
    }

    assert!(
        violations.is_empty(),
        "executor runtime modules must not pattern-match raw AccessPath variants; violations: {}",
        join_display_paths(&violations),
    );
}

#[test]
fn raw_access_path_variant_detector_requires_an_identifier_boundary() {
    assert!(source_uses_raw_access_path_variant_matching(
        "match access { AccessPath::ByKey => {} }",
    ));
    assert!(!source_uses_raw_access_path_variant_matching(
        "RequestDiagnosticAccessPath::ByKey",
    ));
}

#[test]
fn page_materialization_hot_path_uses_concrete_ordered_key_streams() {
    let manifest_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let checked_roots = [
        "src/db/executor/pipeline/contracts",
        "src/db/executor/pipeline/runtime",
        "src/db/executor/terminal/page",
    ];
    let mut sources = Vec::new();
    for root in checked_roots {
        collect_rust_sources(&manifest_root.join(root), &mut sources);
    }
    sources.sort();

    let mut violations = Vec::new();
    for source_path in sources {
        if source_path
            .components()
            .any(|part| part.as_os_str() == "tests")
            || source_path.file_name().is_some_and(|name| {
                name == "tests.rs" || name.to_string_lossy().ends_with("_tests.rs")
            })
        {
            continue;
        }
        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        if source_erases_ordered_key_stream(&source) {
            violations.push(source_path);
        }
    }

    assert!(
        violations.is_empty(),
        "page materialization hot path must keep ordered key streams concrete; violations: {}",
        join_display_paths(&violations),
    );
}

#[test]
fn ordered_stream_guard_checks_types_instead_of_source_spelling() {
    for source in [
        "type Stream = Box<dyn OrderedKeyStream>;",
        "type Stream = &'static mut dyn crate::stream::OrderedKeyStream;",
        "type Stream = Box<dyn\n OrderedKeyStream + Send>;",
        "type Stream = Box<dyn StreamAlias>; use crate::stream::{OrderedKeyStream as StreamAlias};",
    ] {
        assert!(source_erases_ordered_key_stream(source), "{source}");
    }
    for source in [
        "// Box<dyn OrderedKeyStream>\ntype Stream = OrderedKeyStreamBox;",
        "const NOTE: &str = \"dyn OrderedKeyStream\";",
        "type Callback = Box<dyn FnMut()>;",
        "fn consume(stream: impl OrderedKeyStream) {}",
        "#[cfg(test)] mod tests { type Stream = Box<dyn OrderedKeyStream>; }",
    ] {
        assert!(!source_erases_ordered_key_stream(source), "{source}");
    }
}
