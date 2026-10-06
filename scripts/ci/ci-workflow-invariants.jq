# IcyDB owns workflow topology and supervision policy; shared rules own pins.
include "dependency-pins";

def require($condition; $message):
  if $condition then empty else $message end;
def needs_list:
  if . == null then []
  elif type == "string" then [.]
  elif type == "array" and all(.[]; type == "string") then .
  else error("needs must be a job name or list of job names") end;
def needs_are($expected):
  (.needs | needs_list | sort | unique) == ($expected | sort | unique);
def run_text: .run // "";

# One object and an explicit permission owner are required in every workflow.
require(type == "object" and (.jobs | type == "object") and (.jobs | length > 0);
  "workflow requires a nonempty jobs mapping"),
require(has("permissions") and (.permissions != null);
  "workflow must declare top-level token permissions"),
(.jobs | to_entries[] | .key as $job | .value |
  require(type == "object"; "job \($job) must be a mapping"),
  (select(has("runs-on")) |
    require(has("timeout-minutes") and .["timeout-minutes"] != null;
      "job \($job) must bound its runner with timeout-minutes"),
    require([.["runs-on"] | .. | strings | select(. == "ubuntu-latest")] | length == 0;
      "job \($job) must use the fixed Ubuntu 24.04 image")),
  (.steps[]? | select((.uses? // "") | startswith("actions/checkout@")) |
    require(.with["persist-credentials"] == false or .with["persist-credentials"] == "false";
      "job \($job) must disable credentials on each checkout step")),
  .steps as $steps |
  (range(0; ($steps // [] | length)) as $index |
    select($steps[$index] | run_text | test("\\bgh\\s+api\\b")) |
    require(any($steps[0:$index][]; run_text | test("\\bmake\\s+install-gh\\b"));
      "job \($job) uses gh api before its install-gh prerequisite"))),

# Reuse action-reference rules, including reusable workflows. The existing full
# dependency gate retains checkout exceptions, containers and Cargo provenance.
({file: $file, kind: "workflow", data: .} | checks | select(.rule == "action-ref") |
  "[\(.rule)] \(.subject) = \(.value): \(.message)"),

# Only the central CI workflow owns these product-specific topology rules.
(select($file == ".github/workflows/ci.yml") |
  . as $ci |
  (["dependency_msrv", "static", "rust", "macos_host", "check", "wasm_size_report", "release"][] as $job |
    require($ci.jobs | has($job); "CI is missing the \($job) validation job")),
  (["core", "workspace", "tier-a", "tier-b"][] as $lane |
    require(any(.jobs.rust.strategy.matrix.include[]?; .lane == $lane);
      "CI is missing the \($lane) Rust validation lane")),
  require(.jobs.rust.strategy["fail-fast"] == false;
    "parallel Rust validation must retain every lane after one fails"),
  require(.jobs.check | needs_are(["static", "rust", "macos_host"]);
    "terminal check must aggregate every validation lane"),
  require(.jobs.wasm_size_report | needs_are([]);
    "Wasm evidence must run independently from validation"),
  require(.jobs.release | needs_are(["dependency_msrv", "check", "wasm_size_report"]);
    "release artifacts must require MSRV, validation and Wasm evidence"),
  require(if .on | type == "object" then
    if .on.push | type == "object" then .on.push | has("tags") | not else true end
    else true end; "CI must not duplicate the release commit through a tag trigger"),
  (["ci-core", "ci-workspace", "ci-sql-tier-a", "ci-sql-tier-b"][] as $target |
    require(any(.jobs.rust.strategy.matrix.include[]?; .make_target == $target);
      "CI is missing the shared \($target) validation authority")))
