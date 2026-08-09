# Recurring Audit — Complexity And Technical Debt

## Identity

- report scope: `complexity-and-technical-debt`
- method: `CTD-1.0`
- report path:
  `docs/reports/recurring/YYYY/MM/DD/complexity-and-technical-debt/<run>/report.md`

This method replaces the former complexity-accretion, module-structure, and
velocity-preservation audits. Earlier reports are historical context and are
non-comparable except for explicitly named stable anchors.

## Purpose

Determine whether the current supported system is accumulating unnecessary
state space, ownership spread, or evidenced maintenance friction.

This is not a correctness, style, line-count, delivery-speed, TODO, broad
performance, or speculative redesign audit. It measures current structure and
current debt, not every way the code could be different.

## Run Triggers

Run this audit:

- at minor-line closeout for owners changed by that line;
- after adding a public mode, persisted state machine, execution route,
  protocol/cursor format, configuration axis, or widely consumed enum variant;
- when ordinary work repeatedly crosses the same unrelated owners; or
- as a periodic broad baseline when explicitly requested.

The default closeout run is affected-owner scoped. Full runtime enumeration is
reserved for an explicit broad baseline.

## Core Contract

The desired outcome is the smallest maintained state space that satisfies
demonstrated product and safety needs with clear ownership.

Counts are signals, not targets. A large file, enum, module count, branch count,
or public surface is debt only when inspection shows present friction,
multiplied decisions, unclear ownership, or disproportionate maintenance cost.

Apply the no-build and state-space rules in
`docs/governance/simplicity-and-maintainability.md`.

## Evidence Discipline

Enumerate evidence once for the run and reuse it throughout the report. Record:

- code snapshot and dirty-worktree relevance;
- selected owner boundaries and why they are in scope;
- public, persisted, configuration, route, and decision axes;
- owner-local and cross-owner branch/switch sites;
- visibility and dependency crossings;
- current callers and extension friction;
- applicable debt evidence and accepted-debt triggers; and
- the compared baseline or `N/A`.

Mechanical counts must be paired with inspected context before they affect a
finding. Method or scope changes make affected deltas non-comparable.

## Method

### 1. State-Space Map

Record independent behavior axes:

| Axis | Values | Canonical owner | Combining axes | Invalid combinations |
| --- | --- | --- | --- | --- |

Include only maintained current behavior. Do not count removed formats,
historical docs, tests, or rejected inputs as active product states.

Identify axes that duplicate an existing authority, exist only for
configuration convenience, or lack explicit recovery and upgrade ownership.

### 2. Decision And Ownership Spread

For important decisions, record:

| Decision | Owner | Semantic consumers | Plumbing consumers | Cross-owner switch sites |
| --- | --- | ---: | ---: | ---: |

Inspect:

- variants whose addition requires semantic edits in multiple owners;
- orchestration roots absorbing domain decisions;
- broad visibility without nonlocal authority need;
- public or generated surfaces driving runtime semantics;
- persisted state transitions without singular ownership; and
- abstractions whose vocabulary exceeds the invariant they protect.

### 3. Extension Rehearsal

Use at most three plausible near-term feature probes. For each, identify the
expected owner, semantic modules that must change, layers crossed, and the
specific blocker.

Feature probes reveal current friction; they are not roadmap proposals and do
not authorize implementation.

### 4. Debt Reconciliation

Classify evidenced debt using the project families:

- `DuplicatedFlowDebt`
- `StateSpaceDebt`
- `OwnershipDebt`

Duplicated-flow details belong in the Flow Convergence and Duplication audit;
reference that evidence instead of repeating its analysis.

For each debt item, record current friction, owner, evidence, disposition, and
reconsideration trigger. Report-local findings do not create a competing active
debt ledger.

### 5. Noise And Retention Gate

Before classifying debt, account for:

- mechanical file splits or moves;
- generated/test-only code;
- intentional boundary enforcement;
- recovery or corruption containment;
- hot-path specialization;
- public facade coordination; and
- direct test, fixture, documentation, or exhaustive-match propagation.

Do not recommend a refactor without a concrete simpler owner shape. Do not
perform cleanup merely because a threshold was crossed.

## Finding Classification

Use `LOW`, `MEDIUM`, or `HIGH` risk and exactly one disposition:

- `FIX NOW`: current high-risk friction warrants a separately authorized patch;
- `FIX WHEN TOUCHED`: correction belongs with the next change to the same
  owner;
- `ACCEPT UNTIL TRIGGER`: cost is understood and retained until the named
  trigger changes; or
- `NOT DEBT`: the signal has a current authority, safety, or measured reason.

Do not compute a composite complexity or velocity score.

The report may contain at most five active findings. Accepted and not-debt
signals may be summarized without creating follow-up work.

## Required Report

1. preamble and comparability;
2. verdict: `PASS`, `PASS WITH FINDINGS`, `FAIL`, or `BLOCKED`;
3. state-space map;
4. decision/ownership spread;
5. up to three extension rehearsals;
6. findings table with ID, debt family, risk, owner, evidence, present friction,
   disposition, and trigger;
7. accepted/not-debt signals;
8. complexity delta since the comparable baseline; and
9. focused verification readout using `PASS`, `FAIL`, or `BLOCKED`.

If no action is warranted, say so. Do not manufacture debt or cleanup work to
populate the report.

## Read-Only Default

The audit is read-only by default. It does not modify production code, turn
feature probes into designs, create an active debt ledger, or start external
services. Implementation requires a separately authorized bounded patch.
