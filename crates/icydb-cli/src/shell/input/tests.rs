use super::{
    SHELL_CONTINUATION_PROMPT, SHELL_PROMPT, ShellInput, ShellTopLevelInput,
    append_shell_statement_line, clear_shell_input_state, drain_complete_shell_statements,
    shell_input_at_eof, shell_prompt, top_level_shell_input,
};
use crate::shell::route::{SqlShellCallKind, sql_shell_call_kind};
use std::slice;

#[test]
fn statement_lines_preserve_multiline_literal_contents() {
    let lines = [
        "UPDATE character SET bio = 'Line one   ",
        "    indented line two;;",
        "",
        "  café; it''s SQL  ",
        "last line' WHERE id = 7;",
    ];
    let mut partial = String::new();
    for (index, line) in lines.iter().enumerate() {
        append_shell_statement_line(&mut partial, line);
        let complete = drain_complete_shell_statements(&mut partial);
        if index + 1 == lines.len() {
            let expected = lines.join("\n");
            assert_eq!(
                complete.into_iter().collect::<Vec<_>>().as_slice(),
                slice::from_ref(&expected),
            );
            assert_eq!(sql_shell_call_kind(&expected), Ok(SqlShellCallKind::Update));
            assert!(partial.is_empty());
        } else {
            assert!(complete.is_empty());
            assert_eq!(partial, lines[..=index].join("\n"));
        }
    }
}

#[test]
fn pasted_statements_preserve_an_unfinished_literal() {
    let first = "SELECT * FROM character;";
    let remainder = "\nUPDATE character SET bio = 'unfinished   ";
    let mut partial = format!("{first}{remainder}");
    assert_eq!(
        drain_complete_shell_statements(&mut partial)
            .into_iter()
            .collect::<Vec<_>>(),
        [first],
    );
    assert_eq!(partial, remainder);
    assert_eq!(shell_prompt(&partial), SHELL_CONTINUATION_PROMPT);

    let next = "    next line;;' WHERE id = 7;";
    append_shell_statement_line(&mut partial, next);
    let expected = format!("UPDATE character SET bio = 'unfinished   \n{next}");
    assert_eq!(
        drain_complete_shell_statements(&mut partial)
            .into_iter()
            .collect::<Vec<_>>()
            .as_slice(),
        slice::from_ref(&expected),
    );
    assert_eq!(sql_shell_call_kind(&expected), Ok(SqlShellCallKind::Update));
    assert_eq!(shell_prompt(&partial), SHELL_PROMPT);
}

#[test]
fn shell_meta_commands_ignore_outer_whitespace() {
    for line in ["", "  ", "\t"] {
        assert!(matches!(
            top_level_shell_input(line),
            ShellTopLevelInput::Blank
        ));
    }
    for line in ["  EXIT  ", "\t\\q\t", " Quit "] {
        assert!(matches!(
            top_level_shell_input(line),
            ShellTopLevelInput::Exit
        ));
    }
    for line in ["  help;;;  ", "  ?  ", "\\help\t"] {
        assert!(matches!(
            top_level_shell_input(line),
            ShellTopLevelInput::Help
        ));
    }
    assert!(matches!(
        top_level_shell_input("SELECT * FROM character;"),
        ShellTopLevelInput::Sql
    ));
}

#[test]
fn eof_exits_and_discards_unfinished_input() {
    for sql in [
        "",
        " \t",
        "  UPDATE character SET bio = 'last  ' WHERE id = 7  ",
        "UPDATE character SET bio = 'unfinished   \n  ",
    ] {
        let mut partial = sql.to_string();
        assert!(matches!(shell_input_at_eof(&mut partial), ShellInput::Exit));
        assert!(partial.is_empty());
    }
}

#[test]
fn interruption_clears_pasted_and_unfinished_input() {
    let mut pending = ["SELECT * FROM character;".into()].into();
    let mut partial = String::from("UPDATE character SET bio = 'unfinished");
    clear_shell_input_state(&mut pending, &mut partial);
    assert!(pending.is_empty());
    assert!(partial.is_empty());
    assert_eq!(shell_prompt(&partial), SHELL_PROMPT);
    append_shell_statement_line(&mut partial, "SELECT * FROM character;");
    assert_eq!(drain_complete_shell_statements(&mut partial).len(), 1);
}
