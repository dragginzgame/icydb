# Local maintenance schedule

The coordinator runs the [maintenance prompt](maintenance-prompt.md) against
the [task catalog](README.md). Local source files stay unchanged; findings go to
the run report for human review. This optional coordinator needs Codex
and a scheduler only on its host, not in every consumer repository.
The runner also uses the prepared host's Bash, Git and Perl.

## Linux user timer

Use the reviewed [service](https://github.com/dragginzgame/shared-tooling/blob/main/tasks/systemd/shared-tooling-maintenance.service) and
[timer](https://github.com/dragginzgame/shared-tooling/blob/main/tasks/systemd/shared-tooling-maintenance.timer) with
[run-maintenance.sh](https://github.com/dragginzgame/shared-tooling/blob/main/scripts/dev/run-maintenance.sh). The timer wakes daily
at 09:00 in the host timezone. The helper dispatches an agent on the third UTC
date after the previous attempt; the first wakeup runs immediately. Comparing
dates avoids an extra day's delay from small timer jitter or daylight-saving
changes. This gives a three-day cadence during normal operation without a
day-of-month cron expression that resets at month boundaries. A missed wakeup is checked
when the user timer returns; overdue work runs once, without replaying a backlog.

The user service serializes execution and stops a pass after 45 minutes,
including its child processes. Failed attempts retain evidence and wait for the
next due pass; there is no automatic retry loop. The user manager must be running
and the computer available. Do not enable system-wide services or user lingering
as an incidental setup step.

After explicitly requesting installation, prepare the host-owned file
`~/.config/shared-tooling/maintenance.env`, using absolute paths:

```text
SHARED_TOOLING_ROOT=/absolute/path/to/shared-tooling
MAINTENANCE_PROJECTS_ROOT=/absolute/path/to/projects
MAINTENANCE_STATE_ROOT=/absolute/path/to/local-state/shared-tooling/maintenance
CODEX_BIN=/absolute/path/to/codex
MAINTENANCE_CODEX_RUNTIME_ROOT=/absolute/path/to/codex-installation
PATH=/directory/containing/codex-and-node:/home/USER/.cargo/bin:/usr/local/bin:/usr/bin:/bin
```

Quote values containing spaces using systemd EnvironmentFile syntax. Select the
prepared executable paths explicitly; a user service does not source an
interactive shell profile. Preserve existing host configuration rather than
overwriting it. Only the Codex harness uses its saved login; do not provision
GitHub or Cargo publication credentials for this runner.
For a non-system installation, select only the Codex package directory containing
its bundled sandbox executable as `MAINTENANCE_CODEX_RUNTIME_ROOT` (for npm,
the `@openai/codex` package directory). Do not grant the entire Node installation,
home or projects parent. System installations available through the minimal
runtime paths can omit this setting.

Project, state and runner-checkout paths must contain no CR/LF characters,
including their resolved directory aliases. Admission checks existing ancestors
of a future state root without creating it. Invalid paths fail before CLI probes
or state writes; ordinary spaces and aliases remain supported.

Run the helper's `--check PROJECTS_ROOT STATE_ROOT` mode to admit the generated
permission table with a no-model sandbox probe and verify saved login without
starting an agent. It retains temporary check evidence without creating the
selected state directory or consuming cadence. Runs perform the same preflight
before recording dispatch. Invalid setup can be retried immediately; a dispatched
agent failure still consumes its interval. Missing optional task tools
will be reported as blocked; neither the runner nor the timer installs them.

Install reviewed copies of both units under `~/.config/systemd/user/`, validate
them with `systemd-analyze --user verify`, reload the user manager, then enable
`shared-tooling-maintenance.timer`. Enabling the timer is separate from copying
the task catalog. The service uses the selected checkout's current task files;
each pass records its revision/dirty state. Review task changes before they run.

```bash
systemctl --user daemon-reload
systemctl --user enable --now shared-tooling-maintenance.timer
systemctl --user list-timers shared-tooling-maintenance.timer
systemctl --user status shared-tooling-maintenance.service
journalctl --user -u shared-tooling-maintenance.service
```

Pause future passes with `systemctl --user disable --now shared-tooling-maintenance.timer`.
That leaves an active service running; stop it explicitly if desired. To inspect
or retry manually, serialize through the service. `systemctl --user start
shared-tooling-maintenance.service` respects the three-day check; the helper's
`--run` mode bypasses that interval only when deliberately invoked with no active
service or other helper. Never delete evidence to make a retry possible.

## Reports and qualification

Each attempt gets a new `STATE_ROOT/runs/UTC-TIMESTAMP.XXXXXX/` directory containing
the prompt, source inputs, JSON event log, stderr, final `report.md` when available
and `exit-status`. Retain incomplete runs too; a forced process kill may leave no
exit-status or report. `last-attempt` is a dispatch timestamp, not a PASS marker.
A successful process exit does not mean every repository check passed: read the
per-task findings and blockers in the report. Do not commit private run logs.

The portable fixture uses a fake CLI to check shared preflight/dispatch settings,
due-time admission, prior-report selection and failed/empty report handling.
It does not establish live agent, GitHub or native
host qualification. Installation checks prove timer configuration and credentials;
the first real report establishes actual execution coverage.

## Report-only execution boundary

The runner clears inherited environment variables except its explicit runtime
paths and Codex login location. It ignores user configuration and execpolicy
rules, disables connected apps, web search and command networking, and refuses
approval escalation. The agent starts in its new evidence directory, outside
repository configuration discovery. Its named permission profile reads only
minimal runtime paths, the selected projects and Shared Tooling; only that run's
directory is writable. Explicit denials exclude the normal Cargo token files,
SSH/AWS/GitHub stores, Git credentials and Codex credential directory from shell
access. `permissions.txt` records the nonsecret launch policy.

Select a projects directory containing only the intended source checkouts,
not a home directory or filesystem root. Keep the state directory outside Git
repositories so project configuration is not discovered at the agent's working
directory. The runner refuses such state. Source files themselves remain readable;
do not put secrets in that read surface. This is not an OS boundary around the
Codex harness, which still needs its own login. Managed/system Codex configuration
and installed runtime behavior remain host trust inputs. Do not combine the
named profile with legacy `sandbox_mode` settings, which can override it.
See [Codex permissions](https://learn.chatgpt.com/docs/permissions).

The runner copies one completed prior report into the current evidence directory,
recording its source path. Failed/unfinished reports are not used for rotation;
the agent receives no extra state-root permission. Scheduled work covers local
unused-dependency/MSRV source review, snapshot integrity, local README claims and
one bounded audit. Remote freshness/CI/advisory observations are deferred once
per pass for interactive review.

The former `--approve-for-me` launch selected workspace-write plus automatic
approval review; it was not the CLI's unsandboxed bypass option. The replacement
removes that escalation route and makes the report-only scope explicit. Current
remote-state tasks are BLOCKED in unattended runs, not falsely fresh; review
their local findings interactively with current issues/CI/advisory evidence
before publishing anything. Interactive task invocations retain their separately
authorized issue workflow. No release or ordinary build waits for this report.

Before the next live pass, qualify the installed Codex permission-profile support
and managed configuration with harmless sandbox probes: allowed source read,
evidence write, denied source write, denied private credential-marker read and
denied network access. Use dummy markers, never print real credentials.
`--check` executes those no-model filesystem/network probes with the same table;
the substitute CLI fixture alone cannot establish runtime confinement.
An unsupported configuration must fail; do not fall back to broader permissions.

## Other local schedulers

The same prompt can run in a desktop scheduled task with an explicit Shared
Tooling project, absolute projects directory, three-day cadence and output
location. Use one scheduling owner to avoid duplicate passes. Keep the native
execution environment and concurrent-run policy explicit. Desktop local tasks
require the computer and app to be running; see the
[official scheduling documentation](https://learn.chatgpt.com/docs/automations).
The CLI launch mechanism is documented under
[Codex non-interactive mode](https://learn.chatgpt.com/docs/non-interactive-mode).
