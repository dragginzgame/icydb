> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# ICYDB-039 — Shared-shard convergence and virtual scheduling

2026-09-21. User-authorised I3 investigation on the existing 0.261 line.

## Finding

The retained application Wasms reproduce E263 at virtual second 42 with one
explicit PocketIC round per second. The same Wasms and action trace complete
with eight explicit rounds per second. The existing IcyDB convergence timer
continues to succeed; extra ordinary timer service drains the backlog and the
two rejected requests then succeed unchanged. This establishes sensitivity to
the benchmark's service schedule, not a live eight-player capacity limit.

No production runtime correction, higher backlog limit, automatic gameplay
retry, scheduler replacement or additional database is justified by this
comparison. The next owner is the downstream benchmark schedule and its
service accounting. Representative subnet ingress and live capacity remain
unqualified; ICYDB-039 is not closed by a synthetic round-schedule control.

## Source explanation

The inspected IcyDB source is committed `v0.261.0`,
`c1a5028eebed7b2f04cce00d49a4ea6b9c58512c`:

- `db/commit/backlog_admission/mod.rs` admits cumulative database-wide journal
  batches, records and encoded bytes. Batch count is bounded at 64; E263 carries
  the resource, current count, proposal and limit as typed numeric facts.

- `db/commit/recovery/mod.rs::fold_oldest_journal_batch` selects and folds one
  complete oldest batch, then checks whether tails are empty.

- `db/startup/driver/mod.rs` reuses that recovery owner for online convergence.
  Optional cardinality work can also use the same callback, so timer success
  counts alone are not a general exact retired-batch counter.

- The generated watchdog in `icydb-model` uses a one-second retry cadence and
  `ContinueImmediately` for remaining successful work. The pinned ic-timers
  0.8.0 runtime dispatches consumer work in a separate later message and
  coalesces repeated immediate scheduling requests.

The frozen application's `action::active` owner sets its checkpoint deadline
to five seconds. These gameplay waves are six seconds apart. A player can
therefore require a checkpoint of earlier accepted work plus a new recovery
record in the next call. One incoming action call need not mean one journal
batch. The same application checkpoint owner also receives scheduled work.
Increasing the journal ceiling would not establish a sustainable service rate.

## Downstream follow-up

Keep the existing admission bound and timer owner. Qualify the benchmark's
virtual-time/round schedule independently of its requested collection interval:
record explicit rounds, ingress-driven rounds where observable, timer progress,
checkpoint writes, admitted/rejected work and retained journal debt. Preserve
simultaneous submission when claiming a simultaneous workload. Do not treat an
arbitrary eight-round setting or a drain-until-success loop as representative
subnet capacity. Then compare a separately authorised representative subnet
workload and retain conservation, atomic rejection and exact replay checks.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/21/toko-miner-shared-shard-convergence/01/report.md).
