use super::{CratePathOverrides, found_crate_path};
use darling::ast::NestedMeta;
use proc_macro_crate::FoundCrate;
use quote::quote;

#[test]
fn explicit_model_path_is_resolved_without_consuming_authored_arguments() {
    let mut args = NestedMeta::parse_meta_list(quote!(
        model_crate = "crate::model_bridge",
        name = "icydb_model"
    ))
    .unwrap();
    let paths = CratePathOverrides::extract(&mut args)
        .unwrap()
        .resolve(false)
        .unwrap();
    assert_eq!(
        paths.model.to_string(),
        quote!(crate::model_bridge).to_string()
    );
    let expected = NestedMeta::parse_meta_list(quote!(name = "icydb_model")).unwrap();
    assert_eq!(args, expected);
}

#[test]
fn duplicate_and_malformed_overrides_reject() {
    for args in [
        quote!(model_crate = "model_api", model_crate = "other"),
        quote!(model_crate = 3),
        quote!(model_crate = "bad::"),
    ] {
        let mut args = NestedMeta::parse_meta_list(args).unwrap();
        assert!(CratePathOverrides::extract(&mut args).is_err());
    }
}

#[test]
fn package_resolution_distinguishes_self_from_renamed_dependencies() {
    assert_eq!(found_crate_path(FoundCrate::Itself).to_string(), "crate");
    assert_eq!(
        found_crate_path(FoundCrate::Name("model_api".into())).to_string(),
        quote!(::model_api).to_string()
    );
}
