use crate::{prelude::*, view::traits::ViewExpr};

///
/// ItemView
///

pub struct ItemView<'a>(pub &'a Item);

impl ViewExpr for ItemView<'_> {
    fn expr(&self) -> Option<TokenStream> {
        let node = self.0;
        let ty = node.view_type_expr();

        quote!(<#ty as ::icydb::__macro::CoreAsView>::ViewType).into()
    }
}

///
/// ItemUpdate
///

pub struct ItemUpdate<'a>(pub &'a Item);

impl ViewExpr for ItemUpdate<'_> {
    fn expr(&self) -> Option<TokenStream> {
        let node = self.0;
        let ty = node.view_type_expr();

        quote!(<#ty as ::icydb::__macro::CoreUpdateView>::UpdateViewType).into()
    }
}
