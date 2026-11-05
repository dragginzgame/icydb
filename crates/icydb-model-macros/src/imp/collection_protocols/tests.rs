use super::*;

#[test]
fn borrowed_iteration_uses_a_lifetime_not_owned_by_the_authored_type() {
    let def = Def::new(
        syn::parse_quote! {
            pub struct GenericCollection<'__icydb_iter> {
                marker: ::std::marker::PhantomData<&'__icydb_iter ()>,
            }
        },
        crate::crate_path::CratePaths::test(),
    );

    assert_eq!(
        available_iteration_lifetime(&def).to_string(),
        "'__icydb_iter_1",
    );
}
