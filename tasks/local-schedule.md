# Local maintenance schedule

The coordinator runs the [maintenance prompt](maintenance-prompt.md) against
the [task catalog](README.md). Local source files stay unchanged; findings go to
the run report and owning GitHub issues. This optional coordinator needs Codex
and a scheduler only on its host, not in every consumer repository.

## Linux user timer

Use the reviewed [service](systemd/shared-tooling-maintenance.service) and
[timer](systemd/shared-tooling-maintenance.timer) with
[run-maintenance.sh](../scripts/dev/run-maintenance.sh). The timer wakes daily
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
PATH=/directory/containing/codex-and-node:/home/USER/.cargo/bin:/usr/local/bin:/usr/bin:/bin
```

Quote values containing spaces using systemd EnvironmentFile syntax. Select the
prepared executable paths explicitly; a user service does not source an
interactive shell profile. Preserve existing host configuration rather than
overwriting it. Keep credentials in their normal authenticated stores.

Run the helper's `--check PROJECTS_ROOT STATE_ROOT` mode to check Codex version,
automatic-review support and saved login without starting an agent. Also check
`gh auth status` and the selected task prerequisites. Missing optional task tools
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

The portable fixture uses a fake CLI to check dispatch, due-time admission and
failed/empty report handling. It does not establish live agent, GitHub or native
host qualification. Installation checks prove timer configuration and credentials;
the first real report establishes actual execution coverage.

## Other local schedulers

The same prompt can run in a desktop scheduled task with an explicit Shared
Tooling project, absolute projects directory, three-day cadence and output
location. Use one scheduling owner to avoid duplicate passes. Keep the native
execution environment and concurrent-run policy explicit. Desktop local tasks
require the computer and app to be running; see the
[official scheduling documentation](https://learn.chatgpt.com/docs/automations).
The CLI launch mechanism is documented under
[Codex non-interactive mode](https://learn.chatgpt.com/docs/non-interactive-mode).
