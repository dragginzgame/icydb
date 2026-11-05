use super::Enum;
use crate::prelude::*;
use darling::{FromMeta, ast::NestedMeta};
use quote::quote;

#[test]
fn top_level_sorted_flag_emits_enum_order_guard() {
    let args =
        NestedMeta::parse_meta_list(quote!(sorted, variant(name = "A"), variant(name = "B")))
            .expect("enum args should parse");
    let mut node = Enum::from_list(&args).expect("sorted enum should lower");
    node.def = Def::new(
        syn::parse2(quote!(
            pub struct SortedExample {}
        ))
        .expect("enum input should parse as a struct"),
        crate::crate_path::CratePaths::test(),
    );

    assert!(node.sorted);
    assert!(
        node.type_part(&crate::crate_path::CratePaths::test())
            .to_string()
            .contains("remain :: sorted")
    );
}

#[test]
fn unit_enum_baseline_includes_total_rust_ordering() {
    let args = NestedMeta::parse_meta_list(quote!(variant(name = "A"), variant(name = "B")))
        .expect("enum args should parse");
    let mut node = Enum::from_list(&args).expect("enum should lower");
    node.def = Def::new(
        syn::parse2(quote!(
            pub struct OrderedUnitEnum {}
        ))
        .expect("enum input should parse as a struct"),
        crate::crate_path::CratePaths::test(),
    );

    let traits = node.traits();
    for trait_kind in [
        TraitKind::Copy,
        TraitKind::Hash,
        TraitKind::Ord,
        TraitKind::PartialOrd,
    ] {
        assert!(traits.contains(&trait_kind));
    }
}
