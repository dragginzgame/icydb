//! Thin request-execution boundary attributes re-exported by the runtime facade.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_quote};

use crate::crate_path::icydb_runtime_path;

pub(crate) fn expand_request_execution(
    args: TokenStream,
    input: TokenStream,
) -> syn::Result<TokenStream> {
    reject_arguments(&args)?;
    let mut item = syn::parse2::<ItemFn>(input)?;
    let icydb =
        icydb_runtime_path().map_err(|error| syn::Error::new(item.sig.ident.span(), error))?;
    let body = item.block;

    item.block = if item.sig.asyncness.is_some() {
        item.attrs.push(parse_quote!(
            #[allow(
                clippy::future_not_send,
                reason = "IcyDB request roots are intentionally canister-poll-local"
            )]
        ));
        Box::new(parse_quote!({
            #icydb::db::with_request_execution_async(async move #body).await
        }))
    } else {
        Box::new(parse_quote!({
            #icydb::db::with_request_execution(|| #body)
        }))
    };

    Ok(quote!(#item))
}

pub(crate) fn expand_test(args: TokenStream, input: TokenStream) -> syn::Result<TokenStream> {
    reject_arguments(&args)?;
    let mut item = syn::parse2::<ItemFn>(input)?;
    if item.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            &item.sig,
            "#[icydb::test] requires a synchronous test; async executors should poll with_request_execution_async",
        ));
    }
    if !item.sig.inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &item.sig.inputs,
            "#[icydb::test] functions cannot accept arguments",
        ));
    }

    let icydb =
        icydb_runtime_path().map_err(|error| syn::Error::new(item.sig.ident.span(), error))?;
    let body = item.block;
    item.attrs.push(parse_quote!(#[test]));
    item.block = Box::new(parse_quote!({
        #icydb::db::with_request_execution(|| #body)
    }));

    Ok(quote!(#item))
}

fn reject_arguments(args: &TokenStream) -> syn::Result<()> {
    if args.is_empty() {
        Ok(())
    } else {
        Err(syn::Error::new_spanned(
            args,
            "request-execution attributes do not accept arguments",
        ))
    }
}
