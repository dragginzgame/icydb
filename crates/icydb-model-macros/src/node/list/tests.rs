use super::List;
use crate::prelude::*;
use darling::{FromMeta, ast::NestedMeta};
use quote::quote;

#[test]
fn shape_baseline_precedes_trait_removal() {
    let args = NestedMeta::parse_meta_list(quote!(item(prim = "Nat8"), traits(remove(Deref))))
        .expect("list args should parse");
    let mut node = List::from_list(&args).expect("list should lower");
    node.def = Def::new(
        syn::parse2(quote!(
            pub struct WithoutDeref {}
        ))
        .expect("list input should parse as a struct"),
        crate::crate_path::CratePaths::test(),
    );

    node.validate().expect("shape trait should be removable");
    let traits = node.traits();
    assert!(!traits.contains(&TraitKind::Deref));
    assert!(traits.contains(&TraitKind::DerefMut));
    assert!(traits.contains(&TraitKind::From));
    assert!(traits.contains(&TraitKind::FromIterator));
    assert!(traits.contains(&TraitKind::IntoIterator));
}
