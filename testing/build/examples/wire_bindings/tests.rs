use super::Args;
use clap::{Parser as _, error::ErrorKind};
use std::path::PathBuf;

#[test]
fn output_argument_retains_default_and_explicit_paths() {
    assert_eq!(
        Args::try_parse_from(["wire_bindings"]).unwrap().out,
        PathBuf::from("target/wire-browser")
    );
    assert_eq!(
        Args::try_parse_from(["wire_bindings", "a directory"])
            .unwrap()
            .out,
        PathBuf::from("a directory")
    );
    assert_eq!(
        Args::try_parse_from(["wire_bindings", "--", "-output"])
            .unwrap()
            .out,
        PathBuf::from("-output")
    );
}

#[test]
fn help_and_invalid_arguments_exit_before_generation() {
    for (args, kind) in [
        (vec!["wire_bindings", "--help"], ErrorKind::DisplayHelp),
        (
            vec!["wire_bindings", "--unknown"],
            ErrorKind::UnknownArgument,
        ),
        (
            vec!["wire_bindings", "first", "second"],
            ErrorKind::UnknownArgument,
        ),
    ] {
        assert_eq!(Args::try_parse_from(args).err().unwrap().kind(), kind);
    }
}

#[cfg(unix)]
#[test]
fn native_output_path_bytes_are_preserved() {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt as _};
    let path = OsString::from_vec(b"output-\xff".to_vec());
    let args = Args::try_parse_from([OsString::from("wire_bindings"), path.clone()]).unwrap();
    assert_eq!(args.out, PathBuf::from(path));
}
