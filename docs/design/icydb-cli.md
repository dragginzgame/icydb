**0.15 — icydb-cli (Developer Query Harness)**

This is not SQL support.
This is a *developer productivity tool* layered strictly on the public API.

---

# 🎯 Goals for 0.15 — icydb-cli

### Primary Goal

Allow you to type:

```
SELECT * FROM users
WHERE created_at > 1700000000
ORDER BY created_at DESC
LIMIT 10
OFFSET 20;
```

…and execute it against IcyDB using the existing query builder.

---

### Explicit Non-Goals

* No joins
* No subqueries
* No aggregation
* No schema definition
* No full SQL compliance
* No optimizer
* No SQL injection safety
* No production guarantees

This is a developer REPL.

---

# 🏗 Architecture Overview

Create a new crate:

```
crates/icydb-cli
```

This crate:

* Depends only on `icydb` public API
* Has no access to icydb-core internals
* Does not modify planner or executor
* Does not introduce SQL into core

---

# 📦 Crate Layout

```
icydb-cli
 ├── src/
 │   ├── main.rs
 │   ├── repl.rs
 │   ├── parser.rs
 │   ├── ast.rs
 │   ├── lower.rs
 │   ├── printer.rs
 │   └── error.rs
 └── Cargo.toml
```

---

# 🧠 Design Philosophy

Split into 4 stages:

```
input string
   ↓
very small SQL parser
   ↓
DumbQueryAst
   ↓
lowering to icydb::Query<E>
   ↓
execute via facade
   ↓
pretty-print results
```

No logic duplication.
No semantic reimplementation.

All behavior stays inside icydb.

---

# 📜 SQL Subset Grammar (V1)

Support only:

```
SELECT * FROM <entity>
[WHERE <simple_predicate>]
[ORDER BY <field> [ASC|DESC]]
[LIMIT <n>]
[OFFSET <n>]
;
```

---

### WHERE clause (minimal)

Allow only:

```
field = literal
field > literal
field >= literal
field < literal
field <= literal
```

Optional:

```
AND (chain only, no OR)
```

No parentheses.
No expressions.
No functions.

---

# 🧾 Example Queries

Valid:

```
SELECT * FROM users;
SELECT * FROM users ORDER BY created_at DESC;
SELECT * FROM users WHERE id = 42;
SELECT * FROM users WHERE created_at > 1000 ORDER BY created_at ASC LIMIT 5;
SELECT * FROM users WHERE status = "active" AND age > 18;
```

Invalid:

```
SELECT id FROM users;       ❌
SELECT COUNT(*) FROM users; ❌
JOIN;                       ❌
GROUP BY;                   ❌
```

Keep it brutally small.

---

# 🧩 AST Representation

```rust
pub struct SelectAst {
    pub entity: String,
    pub predicate: Option<PredicateAst>,
    pub order_by: Option<OrderAst>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

pub enum PredicateAst {
    Compare {
        field: String,
        op: CompareOp,
        value: Literal,
    },
    And(Box<PredicateAst>, Box<PredicateAst>),
}

pub enum CompareOp {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
}

pub struct OrderAst {
    pub field: String,
    pub direction: Direction,
}

pub enum Literal {
    Int(i64),
    UInt(u64),
    String(String),
    Bool(bool),
}
```

Nothing more.

---

# 🔁 Lowering to icydb Query Builder

This is the key part.

You convert AST into:

```rust
db.query::<Entity>()
   .filter(...)
   .order_by(...)
   .limit(...)
   .offset(...)
```

Mapping rules:

| SQL Concept         | icydb Equivalent                     |
| ------------------- | ------------------------------------ |
| WHERE field = value | builder.filter(field.eq(value))      |
| ORDER BY            | builder.order_by(field.asc()/desc()) |
| LIMIT               | builder.limit(n)                     |
| OFFSET              | builder.offset(n)                    |

No planner logic lives here.

If something fails validation → surface icydb error.

---

# 🖥 REPL Mode

Add simple REPL:

```
> SELECT * FROM users ORDER BY created_at DESC LIMIT 5;
```

Loop:

* Read line
* Parse
* Lower
* Execute
* Print
* Repeat

---

# 🖨 Result Printer

Simple tabular output:

```
+----+------------------+
| id | created_at       |
+----+------------------+
| 42 | 1700000000       |
| 41 | 1699999999       |
+----+------------------+
```

Keep it simple.
No ANSI complexity needed initially.

---

# 🔍 Optional (Very Useful) Debug Mode

Add:

```
--explain
```

So you can run:

```
EXPLAIN SELECT * FROM users ORDER BY created_at DESC LIMIT 5;
```

Which prints:

* AccessPath used
* Direction
* Pushdown active?
* Candidate keys scanned
* Rows returned

This connects directly to your future Observability (17).

---

> Status note (2026-03-06):
> This is a CLI design draft. As of `0.42.x`, EXPLAIN is already shipped on the
> core query API (`explain_execution*` surfaces). A standalone CLI crate is not
> part of the current workspace yet.

# 🔐 Error Model

* Parser errors → CLI parse error
* Lowering errors → schema/field not found
* Execution errors → show icydb error classification

Do not remap errors.
Surface them.

---

# 🚦 Safety Boundaries

To protect core:

* CLI crate must not depend on icydb-core
* CLI crate must not use internal types
* CLI must call public facade only

If you can build CLI without touching icydb-core, you succeeded.

---

# 📈 Why This Is Powerful

Once built, you can:

* Manually test DESC interactively
* See ordering
* See pushdown
* See limit/offset behavior
* Rapidly sanity check changes
* Prototype streaming UX
* Explore composite ranges

It transforms system understanding.

---

# 📊 0.15 Milestone Breakdown

### Phase 1 — Minimal Parser + SELECT * only

### Phase 2 — WHERE simple comparisons

### Phase 3 — ORDER BY + DESC

### Phase 4 — LIMIT/OFFSET

### Phase 5 — REPL + pretty printer

### Phase 6 — EXPLAIN (core shipped in `0.42.x`; CLI integration optional)

That’s it.

---

# ⚠ Keep It Throwaway

You are not building SQL.

You are building:

> A debugging instrument.

If it grows too complex:
Stop.

---

# 🧭 Strategic Impact

This will:

* Lower cognitive friction
* Increase experimentation velocity
* Reveal planner edge cases
* Make streaming abstraction easier to design
* Make metrics/observability meaningful

You’ll stop feeling like you’re building in the dark.
