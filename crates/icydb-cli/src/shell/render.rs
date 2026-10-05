//! Module: shell output separation.
//! Responsibility: separate successful command output from the next prompt.
//! Does not own: SQL value formatting, canister calls, or query execution.
//! Boundary: appends the shell's blank separator to facade-rendered SQL text.

// Keep successful command output visually isolated so the next prompt or shell
// continuation appears after one blank separator line.
pub(super) fn finalize_successful_command_output(rendered: &str) -> String {
    let mut finalized = String::with_capacity(rendered.len().saturating_add(2));
    finalized.push_str(rendered);
    finalized.push('\n');
    finalized.push('\n');

    finalized
}
