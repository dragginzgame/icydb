use crate::{prelude::*, view::traits::ViewExpr};

///
/// ItemView
///

pub struct ItemView<'a>(pub &'a Item);

impl ViewExpr for ItemView<'_> {
    fn expr(&self) -> Option<TokenStream> {
        let node = self.0;
        let ty = node.target().type_expr();
        let cp = paths().core;

        quote!(<#ty as #cp::traits::View>::ViewType).into()
    }
}

///
/// ItemUpdate
///

pub struct ItemUpdate<'a>(pub &'a Item);

impl ViewExpr for ItemUpdate<'_> {
    fn expr(&self) -> Option<TokenStream> {
        let node = self.0;
        let ty = node.target().type_expr();
        let cp = paths().core;

        quote!(<#ty as #cp::traits::UpdateView>::UpdateViewType).into()
    }
}

///
/// ItemFilter
///

pub struct ItemFilter<'a>(pub &'a Item);

impl ViewExpr for ItemFilter<'_> {
    fn expr(&self) -> Option<TokenStream> {
        let item = self.0;

        // The Rust type of the field's *value* type (String, i64, Decimal, Principal, etc.)
        let ty = item.target().type_expr();

        // Payload of the scalar filter kind:
        // <T::Filter as FilterKind>::Payload
        let cp = paths().core;
        let payload = quote!(
            <<#ty as #cp::traits::Filterable>::Filter
                as #cp::db::primitives::FilterKind
            >::Payload
        );

        Some(payload)
    }
}
