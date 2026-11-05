use super::*;
use syn::parse_quote;

#[test]
fn quoted_paths_remain_literal_defaults_and_arguments() {
    for text in ["::1", "a::b", "std::string", "Self::CONST", "42", ""] {
        let literal = LitStr::new(text, proc_macro2::Span::call_site());
        let arg = Arg::from_meta(&parse_quote!(default = #literal)).unwrap();
        assert!(matches!(&arg, Arg::String(value) if value.value() == text));
        assert_eq!(
            arg.to_token_stream().to_string(),
            quote!(#literal).to_string()
        );
        let args = Args::from_list(&[NestedMeta::Lit(Lit::Str(literal.clone()))]).unwrap();
        assert!(matches!(&args.0[0], Arg::String(value) if value.value() == text));
        let schema = arg.schema_part(&CratePaths::test());
        assert!(schema.to_string().contains("Arg :: String"));
    }
}

#[test]
fn bare_paths_work_in_defaults_and_argument_lists() {
    for (path, is_constant) in [
        (parse_quote!(String::new), false),
        (parse_quote!(crate::LIMIT), true),
        (parse_quote!(factory), false),
    ] {
        let default = Arg::from_meta(&parse_quote!(default = #path)).unwrap();
        let argument = Arg::from_nested_meta(&NestedMeta::Meta(syn::Meta::Path(path))).unwrap();
        assert_eq!(
            default.to_token_stream().to_string(),
            argument.to_token_stream().to_string()
        );
        assert_eq!(matches!(default, Arg::ConstPath(_)), is_constant);
    }
    assert!(Arg::from_meta(&parse_quote!(default = factory())).is_err());
    assert!(Arg::from_meta(&parse_quote!(default = 1 + 2)).is_err());
}

#[test]
fn test_bool_parsing() {
    let lit = parse_quote!(true);
    if let Ok(Arg::Bool(b)) = Arg::from_value(&lit) {
        assert!(b, "Parsed boolean should be true");
    } else {
        panic!("Expected Bool variant");
    }
}

#[test]
fn test_char_parsing() {
    let lit = parse_quote!('a');
    if let Ok(Arg::Char(c)) = Arg::from_value(&lit) {
        assert_eq!(c, 'a', "Parsed char should be 'a'");
    } else {
        panic!("Expected Char variant");
    }
}

#[test]
fn test_number_parsing() {
    let lit = parse_quote!(42);
    if let Ok(Arg::Number(num)) = Arg::from_value(&lit) {
        assert_eq!(num, ArgNumber::Int32(42), "Parsed number does not match");
    } else {
        panic!("Expected Number variant");
    }
}

#[test]
fn test_const_path_parsing() {
    let arg: Arg = Arg::from_nested_meta(&parse_quote!(MY_CONST)).unwrap();
    match arg {
        Arg::ConstPath(path) => {
            assert_eq!(path.segments.last().unwrap().ident.to_string(), "MY_CONST");
        }
        _ => panic!("Expected ConstPath variant"),
    }
}

#[test]
fn test_func_path_parsing() {
    let arg: Arg = Arg::from_nested_meta(&parse_quote!(my_func)).unwrap();
    match arg {
        Arg::FuncPath(path) => {
            assert_eq!(path.segments.last().unwrap().ident.to_string(), "my_func");
        }
        _ => panic!("Expected FuncPath variant"),
    }
}

#[test]
fn test_invalid_input() {
    let lit = parse_quote!(b"invalid");
    assert!(
        Arg::from_value(&lit).is_err(),
        "Expected an error for unsupported literal type."
    );
}
