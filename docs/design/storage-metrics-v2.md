Storage Metrics v2 — Design Document
Status

Proposed

Motivation

We currently lack visibility into:

Actual encoded row sizes

Distribution (median, percentiles)

Storage waste / over-allocation

Fragmentation characteristics

Per-entity density

Before considering storage engine changes (e.g. slotted page store), we need empirical metrics.

This feature introduces structured storage telemetry without changing semantics.

Goals

Provide low-cost, low-intrusion metrics for:

Per-entity row counts

Average row size

Median row size

Percentiles (p50 / p90 / p99 optional)

Total logical bytes stored

Estimated storage overhead

Index-to-data ratio

Fragmentation indicators (if applicable)

Must:

Not change storage semantics

Not require full scan on every call (optional sampling mode)

Integrate cleanly with diagnostics::snapshot

Non-Goals

No storage engine changes

No compaction implementation

No page allocator redesign

No index format changes

This is observability only.

High-Level Architecture

Extend:

db/diagnostics/snapshot/

With new:

storage_metrics.rs

Expose through:

Db::storage_metrics()

Metrics computed either:

On-demand full scan

Or sampling mode (configurable)

Proposed API
#[derive(Clone, Debug)]
pub struct StorageMetrics {
    pub entities: Vec<EntityStorageMetrics>,
    pub global: GlobalStorageMetrics,
}

#[derive(Clone, Debug)]
pub struct EntityStorageMetrics {
    pub entity_path: &'static str,

    pub row_count: u64,

    pub total_logical_bytes: u64,
    pub average_row_size: u64,
    pub median_row_size: u64,

    pub p90_row_size: u64,
    pub p99_row_size: u64,

    pub min_row_size: u64,
    pub max_row_size: u64,

    pub index_entries: u64,
    pub index_total_bytes: u64,

    pub data_to_index_ratio: f64,
}

#[derive(Clone, Debug)]
pub struct GlobalStorageMetrics {
    pub total_rows: u64,
    pub total_logical_bytes: u64,
    pub total_index_bytes: u64,
    pub total_storage_bytes: u64,
}
Metric Definitions
Row Count

Number of rows per entity (logical count).

Source:

Data store iteration.

Row Size

Measured as:

encoded_row.len()

Use exact serialized byte length.

Important:
This is logical payload size, not allocated size.

Total Logical Bytes
sum(encoded_row.len())

Represents actual data footprint.

Median / Percentiles

Two possible implementations:

Option A — Exact (Full Scan)

Collect row sizes into Vec

Sort

Compute median/p90/p99

Pros:

Accurate

Cons:

Memory heavy for large datasets

Option B — Streaming Approximation (Recommended)

Use:

Reservoir sampling

Or fixed-size histogram buckets

Or t-digest (if desired)

Recommended minimal version:

Track fixed histogram with exponential buckets

Estimate percentiles from histogram

Low memory footprint.

Index Metrics

For each entity:

Count index entries

Measure raw index key + entry size

Useful to compute:

index_total_bytes / total_logical_bytes

This reveals index overhead ratio.

Fragmentation Indicator (If Using Stable BTreeMap)

If current store uses fixed value capacity:

Add:

allocated_bytes = row_count * configured_value_size
waste_bytes = allocated_bytes - total_logical_bytes
waste_ratio = waste_bytes / allocated_bytes

If slotted page store exists later:

Add:

page_free_space_total
page_fragmentation_ratio

Keep field optional for now.

Execution Modes
Mode 1 — Full Scan
Db::storage_metrics_full()

Iterates entire dataset

Accurate

Expensive

Use for offline diagnostics.

Mode 2 — Sampled
Db::storage_metrics_sampled(sample_size: usize)

Uniform random sampling

Approximate percentiles

Cheap

Use for runtime monitoring.

Implementation Plan
Step 1 — Row Size Collection

In data/store.rs:

Add helper:

pub(crate) fn row_encoded_size(raw: &RawRow) -> usize

Reuse existing encoding logic if possible.

Step 2 — Entity Aggregator

Create:

diagnostics/storage_metrics.rs

Aggregate per-entity stats:

row_count

total_logical_bytes

min/max

histogram (optional)

Step 3 — Index Stats

In:

index/store/

Add:

fn index_entry_size(raw_entry: &RawIndexEntry) -> usize

Aggregate same way.

Step 4 — Integrate with Snapshot

Extend:

diagnostics/snapshot/mod.rs

Add:

StorageMetricsSnapshot

Do not overload existing snapshot struct unless appropriate.

Cost Model

Full scan cost:

O(total rows)

Sampling cost:

O(sample size)

Memory cost:

Exact median: O(total rows)

Histogram: O(bucket_count)

Recommend histogram for production.

What This Enables

Once implemented, you can answer:

Are we wasting space?

Is average row 200B but allocated 4KB?

Is index overhead dominating?

Are some entities pathological?

Should we adopt slotted pages?

This turns guesswork into data.

Future Extensions

If slotted page store introduced later:

Add:

Page count

Page occupancy %

Free space distribution

Compaction opportunity estimate

Final Recommendation

Build this first.

It is low risk, high clarity, and directly informs whether a storage rewrite is justified.

If you want, I can now:

Sketch a minimal histogram-based percentile implementation

Or write the first storage_metrics.rs scaffold tailored to your current module layout