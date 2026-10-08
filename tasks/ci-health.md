# Current CI and issue triage

Apply the [common run contract](README.md) and the existing
[CI/issue maintenance procedure](../rules/agent-maintenance.md).

Resolve each selected repository and source commit before querying GitHub.
Inspect all applicable workflows, latest attempts and PR/branch identities;
expand bounded listings before claiming complete coverage. Read failed logs and
separate code defects from missing prerequisites, runner faults and unavailable
evidence. Pending or absent checks are not success.

Review existing issues, discussions and linked PRs for those findings. Check
active ownership before proposing new work. Close an issue only when its actual
completion criteria and consumer adoption are supported by current evidence.
An upstream fix alone does not prove downstream adoption.

Return failing job, tested SHA, run URL, concrete error and smallest next action,
plus a short ordered list of actionable issues. An unchanged known failure needs
no repeated comment; a new source/run or changed diagnosis may warrant one.
Inspection does not dispatch, rerun or cancel workflows or start repairs.
