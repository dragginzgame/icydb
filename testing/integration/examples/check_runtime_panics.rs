//! Development-only structural check for IcyDB's critical runtime source roots.
//! The shell gate owns discovery; Syn owns Rust syntax and test-item boundaries.

use std::{env, fs, process::ExitCode};

use proc_macro2::{Delimiter, Span, TokenStream, TokenTree};
use syn::{
    Attribute, Expr, ImplItem, Item, Meta, Token, TraitItem, Type,
    parse::Parser,
    punctuated::Punctuated,
    visit::{self, Visit},
};

#[derive(Default)]
struct Scanner {
    hits: Vec<(Span, String)>,
}

impl Scanner {
    // Macro arguments are token streams rather than expression ASTs. Scan their
    // structural tokens too, including nested macro templates, but never literals.
    fn scan_tokens(&mut self, stream: TokenStream) {
        let tokens: Vec<_> = stream.into_iter().collect();
        for (index, token) in tokens.iter().enumerate() {
            if let TokenTree::Group(group) = token {
                self.scan_tokens(group.stream());
            }
            let TokenTree::Ident(ident) = token else {
                continue;
            };
            let name = ident.to_string();
            let macro_call = forbidden_macro(&name)
                && matches!(tokens.get(index + 1), Some(TokenTree::Punct(p)) if p.as_char() == '!');
            let method_call = matches!(name.as_str(), "unwrap" | "expect")
                && matches!(index.checked_sub(1).and_then(|i| tokens.get(i)), Some(TokenTree::Punct(p)) if p.as_char() == '.')
                && matches!(tokens.get(index + 1), Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Parenthesis);
            if macro_call || method_call {
                self.hits.push((ident.span(), name));
            }
        }
    }
}

impl<'ast> Visit<'ast> for Scanner {
    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if node.method == "unwrap" || node.method == "expect" {
            self.hits
                .push((node.method.span(), node.method.to_string()));
        }
        visit::visit_expr_method_call(self, node);
    }

    fn visit_impl_item(&mut self, node: &'ast ImplItem) {
        let attrs = match node {
            ImplItem::Const(item) => &item.attrs[..],
            ImplItem::Fn(item) => &item.attrs[..],
            ImplItem::Type(item) => &item.attrs[..],
            ImplItem::Macro(item) => &item.attrs[..],
            _ => &[],
        };
        if !test_only(attrs) {
            visit::visit_impl_item(self, node);
        }
    }

    fn visit_item(&mut self, node: &'ast Item) {
        let attrs = match node {
            Item::Const(item) => &item.attrs[..],
            Item::Enum(item) => &item.attrs[..],
            Item::ExternCrate(item) => &item.attrs[..],
            Item::Fn(item) => &item.attrs[..],
            Item::ForeignMod(item) => &item.attrs[..],
            Item::Impl(item) => &item.attrs[..],
            Item::Macro(item) => &item.attrs[..],
            Item::Mod(item) => &item.attrs[..],
            Item::Static(item) => &item.attrs[..],
            Item::Struct(item) => &item.attrs[..],
            Item::Trait(item) => &item.attrs[..],
            Item::TraitAlias(item) => &item.attrs[..],
            Item::Type(item) => &item.attrs[..],
            Item::Union(item) => &item.attrs[..],
            Item::Use(item) => &item.attrs[..],
            _ => &[],
        };
        if test_only(attrs) || anonymous_const_assertion(node) {
            return;
        }
        visit::visit_item(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(segment) = node.path.segments.last()
            && forbidden_macro(&segment.ident.to_string())
        {
            self.hits
                .push((segment.ident.span(), segment.ident.to_string()));
        }
        self.scan_tokens(node.tokens.clone());
    }

    fn visit_token_stream(&mut self, tokens: &'ast TokenStream) {
        // Verbatim syntax must remain visible instead of silently disappearing.
        self.scan_tokens(tokens.clone());
    }

    fn visit_trait_item(&mut self, node: &'ast TraitItem) {
        let attrs = match node {
            TraitItem::Const(item) => &item.attrs[..],
            TraitItem::Fn(item) => &item.attrs[..],
            TraitItem::Type(item) => &item.attrs[..],
            TraitItem::Macro(item) => &item.attrs[..],
            _ => &[],
        };
        if !test_only(attrs) {
            visit::visit_trait_item(self, node);
        }
    }
}

fn forbidden_macro(name: &str) -> bool {
    matches!(
        name,
        "panic" | "assert" | "assert_eq" | "assert_ne" | "unreachable" | "todo" | "unimplemented"
    )
}

// Evaluate cfg conservatively with test=false and every other predicate unknown.
// Both truth possibilities are retained so not/any/all cannot turn an unknown
// feature into a false proof that an item is absent from production.
fn cfg_possibilities(meta: &Meta) -> (bool, bool) {
    if let Meta::Path(path) = meta
        && path.is_ident("test")
    {
        return (false, true);
    }
    if let Meta::List(list) = meta
        && let Ok(parts) =
            Punctuated::<Meta, Token![,]>::parse_terminated.parse2(list.tokens.clone())
    {
        if list.path.is_ident("all") {
            return parts.iter().map(cfg_possibilities).fold(
                (true, false),
                |(can_true, can_false), (part_true, part_false)| {
                    (can_true && part_true, can_false || part_false)
                },
            );
        }
        if list.path.is_ident("any") {
            return parts.iter().map(cfg_possibilities).fold(
                (false, true),
                |(can_true, can_false), (part_true, part_false)| {
                    (can_true || part_true, can_false && part_false)
                },
            );
        }
        if list.path.is_ident("not") && parts.len() == 1 {
            let (can_true, can_false) = cfg_possibilities(&parts[0]);
            return (can_false, can_true);
        }
    }
    (true, true)
}

fn test_only(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg")
            && attr
                .parse_args::<Meta>()
                .is_ok_and(|meta| !cfg_possibilities(&meta).0)
    })
}

// Only the direct anonymous unit assertion is compile-time-only. Blocks,
// additional statements and callable const functions must still be scanned.
fn anonymous_const_assertion(item: &Item) -> bool {
    let Item::Const(item) = item else {
        return false;
    };
    let Type::Tuple(ty) = &*item.ty else {
        return false;
    };
    let Expr::Macro(expr) = &*item.expr else {
        return false;
    };
    item.ident == "_"
        && ty.elems.is_empty()
        && expr.mac.path.is_ident("assert")
        && simple_assertion_tokens(expr.mac.tokens.clone())
}

fn simple_assertion_tokens(tokens: TokenStream) -> bool {
    tokens.into_iter().all(|token| match token {
        TokenTree::Group(group) => {
            group.delimiter() != Delimiter::Brace && simple_assertion_tokens(group.stream())
        }
        TokenTree::Punct(punct) => punct.as_char() != ';',
        _ => true,
    })
}

fn main() -> ExitCode {
    let files: Vec<_> = env::args_os().skip(1).collect();
    if files.is_empty() {
        eprintln!("runtime panic checker requires a nonempty source inventory");
        return ExitCode::from(2);
    }
    let mut failed = false;
    for file in files {
        let path = std::path::Path::new(&file);
        let parsed = fs::read_to_string(path)
            .map_err(|error| error.to_string())
            .and_then(|source| syn::parse_file(&source).map_err(|error| error.to_string()));
        let syntax = match parsed {
            Ok(syntax) => syntax,
            Err(error) => {
                eprintln!("{}: cannot parse runtime source: {error}", path.display());
                return ExitCode::from(2);
            }
        };
        let mut scanner = Scanner::default();
        if !test_only(&syntax.attrs) {
            scanner.visit_file(&syntax);
        }
        for (span, construct) in scanner.hits {
            failed = true;
            let position = span.start();
            eprintln!(
                "{}:{}:{}: forbidden runtime construct {construct}",
                path.display(),
                position.line,
                position.column + 1
            );
        }
    }
    if failed {
        eprintln!(
            "Production executor, commit, journal and startup code must return typed errors instead of panicking."
        );
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
