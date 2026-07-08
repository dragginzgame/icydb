Historical raw review note. This file records external feedback as received and
is not the current 0.198 API authority. The implemented paging surface is
documented in `0.198-supplemental-status.md` and the public guides; public
fluent pages now use `page(limit)?` / `next_page(limit, cursor)?`.

Yes. The direction is good — intent-first is the right public API model for IcyDB — but this facade is still too broad and slightly inconsistent. The main improvement is not to add more helpers; it is to make the public API smaller, more tiered, and more opinionated.

My verdict: keep the model, tighten the surface. The document already states the correct philosophy: IcyDB should expose exact row, public page, complete small set, exact aggregate, and trusted maintenance batch as separate endpoint promises, not blur them behind limit, one, or all.

The biggest issue

The API is still mixing four audiences into one facade:

normal public endpoint authors;
trusted/admin endpoint authors;
diagnostics/EXPLAIN users;
low-level generated/policy internals.

That makes the facade feel bigger than it probably is conceptually. The public surface should feel like:

db.load::<User>()
    .by_id(id)
    .try_one()

db.load::<User>()
    .filter_eq(User::status, Status::Active)
    .order_asc(User::created_at)
    .page(PageRequest::first(50))

db.load::<User>()
    .filter_eq(User::kind, Kind::SmallStaticSet)
    .collect_complete()

db.load::<User>()
    .filter_eq(User::status, Status::Active)
    .count_exact()

Right now the reference also exposes execute, execute_rows, execute_trusted, execute_rows_trusted, execute_paged, execute_paged_trusted, admin_batch, query, trace, planned, plan, explain, multiple execution explain variants, direct query execution, hidden SQL helpers, and many response narrowing helpers. That is too much to understand as “the public database facade.”

Recommendation 1: split the facade into public tiers

I would explicitly classify every method into one of these tiers.

Tier 1: Normal endpoint API
Safe, intent-first commands ordinary app endpoints should use.

Tier 2: Trusted/admin API
Visible escape hatches for maintenance, migrations, and internal canister tasks.

Tier 3: Diagnostics API
EXPLAIN, trace, plan inspection, attribution, debug helpers.

Tier 4: Generated/internal API
Used by macros, SQL validators, policy wrappers, or compatibility paths. Not taught as normal user API.

Then the reference becomes much clearer.

Tier 1 should be small

For normal public reads, I would try to keep the taught API to:

db.load::<E>()

.by_id(id)
.by_ids(ids)

.filter(...)
.filter_eq(...)
.filter_*...

.order_asc(...)
.order_desc(...)
.order_term(...)
.order_terms(...)

.try_one()
.execute_rows()       // primarily for by_ids or explicitly bounded windows
.page(request)
.collect_complete()

.exists()
.not_exists()

.count_exact()
.sum_exact(field)
.min_exact_by(field)
.max_exact_by(field)
.avg_exact(field)

I would not lead with raw execute(), execute_rows_trusted(), execute_paged_trusted(), trace, planned, plan, or direct query execution in the same list.

Recommendation 2: choose one paging terminal spelling

The reference currently lists both:

.page(request)?
.execute_paged(request)
.execute_paged_trusted(request)

For an intent-first API, I would strongly prefer:

.page(request)

as the public terminal.

Then either remove execute_paged(request) from public docs or classify it as compatibility/internal. Otherwise you have two names for the same endpoint promise: one intent-first and one engine/execution-shaped.

Suggested rule:

Use `.page(request)` in public endpoint code.

`execute_paged(request)` is retained only as a compatibility/internal spelling and
should not appear in new examples.

Or, if you want the hard cut:

Remove public `execute_paged(request)` and keep only `.page(request)`.

Given pre-1.0, I would hard cut unless downstream churn is already painful.

Recommendation 3: consider renaming bounded_window

This is the one method I would be careful with.

.bounded_window(n).execute_rows()

The phrase is technically accurate, but it is not obviously a “dangerous/partial” endpoint contract to ordinary developers. It sounds almost as safe as page.

I would consider a more explicit name:

.partial_window(n)

or:

.row_window(n)

or even:

allow_partial_window(n)

My preferred option:

.partial_window(n).execute_rows()

Then your docs can say:

Use `page(...)` for caller-facing lists.
Use `partial_window(...)` only when the endpoint contract explicitly allows a non-cursor partial result.

bounded_window is not wrong, but partial_window better communicates the semantic hazard: this is not a page, not complete, and not exact.

Recommendation 4: make execute() less prominent

execute() returning QueryResponse<E> is powerful, but it fights the read-intent story because the caller then has to narrow the shape later:

response.into_rows()
response.into_grouped()

That is fine for lower-level code, but for normal endpoint authors the terminal should name the promise.

I would classify:

.execute()
.query()
.planned()
.plan()
.trace()

as advanced/diagnostic/internal-ish, not the normal path.

Suggested docs wording:

Most endpoint code should not call `execute()` directly. Prefer a terminal that
states the endpoint promise: `try_one`, `page`, `collect_complete`, or an exact
aggregate helper. `execute()` is for advanced callers that intentionally handle
multiple response shapes.
Recommendation 5: collapse trusted naming

You currently have:

.trusted_read_unchecked()
.execute_trusted()
.execute_rows_trusted()
.execute_paged_trusted(request)
.admin_batch(request)

This is a lot of “trusted” surface.

I would prefer one trusted lane entrypoint:

db.load::<E>()
    .trusted_read_unchecked()
    .admin_batch(AdminBatchRequest::new())

And I would avoid teaching these unless there is a very specific need:

.execute_trusted()
.execute_rows_trusted()
.execute_paged_trusted(request)

If those must exist internally, classify them as Tier 4 or Tier 2 advanced.

The public mental model should be:

trusted_read_unchecked() opens the trusted lane.
admin_batch(...) is the intended maintenance terminal.

Not:

There are trusted versions of every terminal.

Because that encourages exactly the wrong pattern: “try public API, then add _trusted when rejected.”

Recommendation 6: be stricter about SQL surface visibility

The SQL section says SQL session commands are trusted/admin unless wrapped by app policy. That is good, but the list is scary:

db.execute_sql_query::<E>(sql)
db.execute_sql_update::<E>(sql)
db.execute_sql_ddl::<E>(sql)
...
db.execute_validated_sql_public_primary_key_update::<E>(plan)
...

I would split this hard:

### SQL Admin API

- execute_sql_query
- execute_sql_update
- execute_sql_ddl

### SQL Policy/Internal API

These are generated or policy-wrapper surfaces and should not be called directly
from ordinary endpoint code.

The “hidden generated/policy helpers” should maybe not be in a public facade reference at all, or they should be in an appendix titled:

Internal/generated surfaces — listed for audit only

Otherwise developers will think they are allowed to call them.

Recommendation 7: reduce aggregate helper ambiguity

This list is slightly uneven:

.count_exact()
.min_exact()
.min_exact_by(field)
.max_exact()
.max_exact_by(field)
.sum_exact(field)
.avg_exact(field)

min_exact() and max_exact() without a field are ambiguous unless they mean “over primary key/entity order.” If that is the meaning, I would consider renaming to make it explicit:

.min_id_exact()
.max_id_exact()
.min_field_exact(field)
.max_field_exact(field)

Or keep the current names but document them directly:

`min_exact()` and `max_exact()` operate on the entity's primary ordering.
Use `min_exact_by(field)` / `max_exact_by(field)` for field aggregates.

Without that clarification, the pair looks inconsistent.

Recommendation 8: only() needs very careful positioning

You have:

.only()

available for SingletonEntity types, and delete also has .only().

That could be fine, but only() is a very strong word and could be confused with “only one row.” I would avoid putting it near generic read terminals.

For singleton entities, I would consider:

.singleton()

or:

.only_singleton()

But if .only() is already established, document it as:

`only()` is only for `SingletonEntity` types. It is not a one-row query terminal.
For normal one-row reads, use `.by_id(id).try_one()`.
Recommendation 9: make delete intentionally different

It is good that delete keeps .limit(n) because it bounds affected rows rather than materialized reads. The document says that already.

I would make the distinction very explicit in the API:

db.delete::<E>()
    .filter(...)
    .limit(n)
    .execute()

This is okay because it is a mutation safety cap, not a public read materialization cap.

But I would consider renaming later to:

.affect_limit(n)

or:

.max_affected(n)

That would remove the last major semantic conflict around limit.

My preference:

.max_affected(n)

Example:

db.delete::<Session>()
    .filter_lt(Session::expires_at, now)
    .max_affected(500)
    .execute()

That reads much more clearly than limit(500).

Recommendation 10: response helpers are too large for the main API reference

This section is useful, but it makes the facade feel huge:

rows.count()
rows.exists()
rows.require_one()
rows.require_some()
rows.entity()
rows.try_entity()
rows.entities()
...
projection.rows()
projection.iter()
...
result.row_count()
result.count()
result.is_empty()
result.exists()
...

I would move response helpers into their own “Response Types” reference and keep the main facade focused on commands.

A better public reference structure:

# Public Database Facade

## Read Intent Model
## Common Endpoint Recipes
## Normal Load API
## Normal Delete API
## Write API
## Trusted/Admin API
## Diagnostics API
## SQL API
## Response Types Appendix
## Internal/Generated Appendix
The API I would aim for

For ordinary endpoint authors, I would want the taught surface to feel like this:

// Exact lookup
db.load::<User>()
    .by_id(user_id)
    .try_one()

// Multiple exact IDs
db.load::<User>()
    .by_ids(user_ids)
    .execute_rows()

// Public page
db.load::<User>()
    .filter_eq(User::status, Status::Active)
    .order_desc(User::created_at)
    .page(PageRequest::first(50))

// Complete small set
db.load::<Role>()
    .filter_eq(Role::project_id, project_id)
    .collect_complete()

// Exact aggregate
db.load::<User>()
    .filter_eq(User::status, Status::Active)
    .count_exact()

// Deliberate partial window
db.load::<Event>()
    .order_desc(Event::created_at)
    .partial_window(100)
    .execute_rows()

// Trusted maintenance scan
db.load::<Event>()
    .trusted_read_unchecked()
    .admin_batch(AdminBatchRequest::new())

That is a very clean public API. The rest can still exist, but it should be documented as advanced, diagnostics, trusted, or internal.

Concrete changes I would make
Hard public API recommendations
Current	Recommendation	Reason
page(request) and execute_paged(request)	Prefer only page(request) publicly	One spelling per behavior
bounded_window(n)	Consider partial_window(n)	Makes partial-result contract clearer
execute()	Keep but demote to advanced	Does not express endpoint promise
execute_trusted, execute_rows_trusted, execute_paged_trusted	Hide/demote behind trusted_read_unchecked()	Avoid casual trusted bypasses
delete .limit(n)	Consider future .max_affected(n)	Distinguishes mutation cap from read limit
min_exact() / max_exact()	Clarify or rename	Fieldless aggregate meaning is unclear
.only()	Document as singleton-only	Avoid confusion with one-row read
hidden SQL helpers	Move to internal/generated appendix	Prevent accidental direct use
Revised read-intent summary

I would rewrite the summary table like this:

## Read Intent Summary

Use the method that names the endpoint promise.

| Endpoint promise | Preferred command | Notes |
|---|---|---|
| Exact row by primary key | `load::<E>().by_id(id).try_one()` | Proved by key shape, not by truncation. |
| Exact rows by primary keys | `load::<E>().by_ids(ids).execute_rows()` | Exact key set. |
| Existence | `load::<E>().exists()` / `not_exists()` | Avoid materializing rows. |
| Public cursor page | `load::<E>().order_term(...).page(PageRequest::first(n))` | Preferred public list shape. |
| Complete small set | `load::<E>().collect_complete()` | Returns all admitted rows or fails. |
| Deliberate partial window | `load::<E>().partial_window(n).execute_rows()` | Not a page and not complete. Use only when partial results are the contract. |
| Exact aggregate | `count_exact()`, `sum_exact(field)`, `min_exact_by(field)`, `max_exact_by(field)`, `avg_exact(field)` | Aggregate over the admitted full result, not a materialized page. |
| Trusted maintenance batch | `trusted_read_unchecked().admin_batch(AdminBatchRequest::new())` | Admin/maintenance only. |

If you keep bounded_window, use that in the table, but I would at least describe it as a deliberate partial window.

Bottom line

The public API is conceptually strong but still too wide in presentation. The main fix is to make the facade tiered and teach only the intent-first path.

I would not bring back one() or all(). Removing them is right. But I would also go further and make sure execute_paged, trusted variants, SQL internals, and diagnostics helpers do not sit beside the normal public commands as if they are equally recommended.

The target should be:

Normal endpoint authors choose intent terminals.
Trusted/admin users enter an explicit trusted lane.
Diagnostics live in their own namespace/section.
Generated/internal helpers are listed only for audit, not taught.

That gives you a clean pre-1.0 API and keeps IcyDB’s public surface aligned with the safety model.

Pasted markdown.md
File
what do you thionk of this, can i improve the API further?

I think this version is a significant improvement over the previous public API reference. The big change is that you are no longer just listing methods — you are defining execution lanes and endpoint promises. That is the right abstraction for IcyDB.

The strongest part is that the API is now being shaped around what the caller is promising, not what the database happens to do internally. The contract explicitly separates:

PublicRead
AdminAdHoc
DiagnosticExplain
DevTest

and says admission is a property of the execution lane, not a random query modifier. That is much closer to how a database inside an IC canister should think.

However, I think there are still improvements possible. The API is probably 90% there conceptually, but the last 10% is where you make it feel elegant instead of merely safe.

1. The biggest remaining issue: partial_window(...) is becoming too important

You have made a very good decision removing ambiguous:

.one()
.all()
.limit()

for reads.

But now:

.partial_window(10)
.execute_rows()

is becoming the "escape hatch" that developers will reach for.

The danger is that it becomes the new .limit(10).

You already have the right semantics:

page = user-facing list
collect_complete = small complete set
exact aggregates = exact result
partial_window = deliberate partial result

The question is whether partial_window should be so easy to call.

Right now:

db.load::<User>()
    .filter(...)
    .partial_window(10)
    .execute_rows()

looks almost as innocent as:

.limit(10)

A junior developer may not understand the difference.

I would consider making the name more explicit:

Option A
.window(10)

No, too weak.

Option B
.partial_rows(10)

Better.

Option C (my preference)
.read_window(10)

with documentation:

read_window() returns an intentionally incomplete row window. It is not pagination and does not imply completeness.

The word "partial" is correct but slightly alarming. The word "window" is technically correct but slightly vague.

Something like:

.execute_window(WindowRequest::rows(10))

would be even harder to misuse.

2. I would introduce "endpoint intent types"

Currently the API expresses intent through terminal methods:

.try_one()
.page()
.collect_complete()
.count_exact()

This is already good.

But I think IcyDB could go one step further.

Instead of thinking:

Query -> terminal

think:

Query -> Intent -> Execution

Example:

let users = db()
    .load::<User>()
    .filter(...)
    .intent()
    .page(PageRequest::first(50));

or:

db.load::<User>()
   .filter(...)
   .as_page(PageRequest::first(50))

Why?

Because then the type system knows:

FluentLoadQuery<User>
        |
        v
PublicPageQuery<User>
        |
        v
execute()

You get impossible states removed.

Example:

Currently:

query.partial_window(10)
     .count_exact()

is rejected at runtime/builder validation.

A stronger API would make that impossible:

PartialWindowQuery<User>

doesn't even have:

count_exact()

available.

This is the Rust way.

3. trusted_read_unchecked() is the right idea, but the name could improve

I like the safety signal.

The problem is:

trusted_read_unchecked()

sounds like unsafe Rust.

Developers associate:

unchecked

with:

UB
unsafe blocks
bypassing invariants

But your meaning is different:

"I acknowledge this caller has already performed authorization and resource governance."

Maybe:

.admin_read()

or:

.trusted_admin_read()

or:

.enter_trusted_lane()

would communicate intent better.

My favourite:

.trusted_read()

with the type system making the lane obvious.

Example:

db.load::<User>()
   .trusted_read()
   .admin_batch(...)

The word "trusted" already tells the story.

4. execute_rows() is still slightly generic

You removed:

.all()

because it was ambiguous.

Good.

But:

.execute_rows()

still means multiple things.

It could mean:

partial window rows
exact IDs
admin batch
internal execution

The examples now rely heavily on:

.partial_window(...)
.execute_rows()

which is good.

But I would consider:

.fetch_rows()

or:

.materialize_rows()

for the low-level terminal.

Why?

Because execute_rows() sounds like the normal operation.

A better semantic split:

.page(...)
.fetch_page()

.collect_complete()
.collect()

.partial_window(...)
.fetch_rows()

Now the verbs match the promise.

5. collect_complete() is excellent. Keep it.

This is probably my favourite API choice.

It communicates the critical invariant:

collect_complete()

means:

"I expect everything."

The failure mode is obvious.

This is much better than:

all()

because all() lies.

collect_complete() tells the truth.

Keep it.

6. Aggregates are much better now

This improvement is important:

Old:

min_exact()
max_exact()

New:

min_id_exact()
max_id_exact()
min_exact_by(field)
max_exact_by(field)

Much better.

The API now exposes the hidden semantic:

aggregate over identity ordering
aggregate over field ordering

That is exactly the kind of thing databases normally hide.

I would maybe make them symmetrical:

min_by_exact(field)
max_by_exact(field)

rather than:

min_exact_by(field)

because Rust convention tends toward:

sort_by
group_by
order_by

So:

count_exact()
sum_exact(field)
min_by_exact(field)
max_by_exact(field)
avg_exact(field)

reads better.

7. I would remove offset() from the public fluent API entirely

This part bothers me:

.offset(n)

even though admission rejects it.

The existence of the method teaches the wrong model.

If public reads fundamentally do not support offset pagination, why expose it?

You currently have:

.offset(n)

and then:

PublicRead rejects non-zero OFFSET

That is like exposing:

unsafe_mode()

and saying "we reject it later."

For SQL compatibility, fine.

For fluent API:

I would remove it.

Use:

PageRequest::next(cursor)

only.

8. The API should have "recipes", not only methods

The document is very strong technically, but developers do not think in methods.

They think:

"I need a list endpoint."

I would add a cookbook:

Common endpoint shapes
Profile lookup
load::<User>()
.by_id(id)
.try_one()
User list
load::<User>()
.filter(...)
.order(...)
.page(PageRequest::first(50))
Dashboard count
load::<User>()
.filter(...)
.count_exact()
Small dropdown
load::<Country>()
.collect_complete()
Admin export
load::<User>()
.trusted_read()
.admin_batch(...)

This reduces misuse more than another 20 pages of rules.

9. One concern: the public API still has too many terminals

The current mental model should probably be:

Public
try_one
exists
not_exists
page
collect_complete
count_exact
sum_exact
min_by_exact
max_by_exact
avg_exact
Advanced
partial_window
execute_rows
execute
Trusted
trusted_read
admin_batch
Diagnostics
explain*
trace*
plan*

If ordinary developers see 40 methods, they will choose the wrong one.

The contract is correct, but the "front door" needs to be smaller.

My ideal IcyDB 1.0 read API

Something like:

// Exact lookup
db.load::<User>()
   .by_id(id)
   .try_one()

// Page
db.load::<User>()
   .filter(...)
   .order(...)
   .page(PageRequest::first(50))

// Complete small set
db.load::<Role>()
   .collect_complete()

// Existence
db.load::<User>()
   .filter(...)
   .exists()

// Exact aggregates
db.load::<User>()
   .filter(...)
   .count_exact()

// Admin
db.load::<User>()
   .trusted_read()
   .admin_batch(...)

Then hide everything else.

Overall judgement

I think this is now database-grade architecture. The important shift has happened:

Old ORM thinking:

query -> modifiers -> execute

IcyDB thinking:

endpoint promise -> admitted lane -> execution

That is the correct model for a canister database.

The remaining improvements are mostly about making the safe path shorter than the unsafe path.

My top 5 changes before 1.0 would be:

Remove .offset() from fluent public API.
Demote execute() / execute_rows() from normal documentation.
Rename or harden partial_window().
Consider type-level intent states instead of runtime terminal rejection.
Publish a tiny "five endpoint patterns" cookbook.

The underlying design is very strong. The main risk now is not correctness — it is that the API surface becomes too powerful for normal developers. The best database APIs are usually the ones where the correct thing is also the easiest thing.
