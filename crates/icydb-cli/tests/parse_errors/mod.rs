//! Parse refusals keep external tokens inert before command dispatch.

use super::*;

#[test]
fn clap_refusals_escape_unknown_options_subcommands_and_extra_tokens() {
    for label in [
        "界-e\u{301}-\\n",
        "界-e\u{301}-\\n\t\r\n\x1b[2J\u{0085}\u{202e}\u{2028}",
    ] {
        let unknown_option = format!("--unknown-{label}");
        let unknown_subcommand = format!("diagnostic-{label}");
        for arguments in [
            vec![unknown_subcommand.as_str()],
            vec!["schema", "migration", unknown_subcommand.as_str()],
            vec!["diagnostic", unknown_option.as_str()],
            vec!["diagnostic", "7", label],
        ] {
            let fixture = IcpFixture::new();
            let output = fixture.command("").args(&arguments).output().unwrap();
            assert_eq!(output.status.code(), Some(2));
            assert!(output.stdout.is_empty());
            let stderr = assert_terminal_lines(&output.stderr);
            let token = arguments.last().unwrap();
            assert!(stderr.contains(ic_host_text::escape::terminal_text(token).as_ref()));
            assert!(stderr.contains("Usage:"));
            assert!(!fixture.directory.join("calls").exists());
        }
    }
}

#[test]
fn ordinary_clap_tips_help_and_version_keep_their_output_channels() {
    let fixture = IcpFixture::new();
    let refused = fixture
        .command("")
        .args(["diagnostic", "--unknown-option"])
        .output()
        .unwrap();
    assert_eq!(refused.status.code(), Some(2));
    assert!(refused.stdout.is_empty());
    assert!(assert_terminal_lines(&refused.stderr).contains("-- --unknown-option"));
    for flag in ["--help", "--version"] {
        let output = fixture.command("").arg(flag).output().unwrap();
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        assert!(!output.stdout.is_empty());
        assert_terminal_lines(&output.stdout);
    }
    assert!(!fixture.directory.join("calls").exists());
}
