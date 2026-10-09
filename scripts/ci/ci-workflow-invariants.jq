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
  # Installation alone does not override rust-toolchain.toml. The executable
  # gate compares the selected compiler with the public manifest's own floor.
  require(.jobs.dependency_msrv.env.RUSTUP_TOOLCHAIN |
    if type == "string" then test("^[0-9]+\\.[0-9]+\\.[0-9]+$") else false end;
    "public MSRV must explicitly select its compiler"),
  require(any(.jobs.dependency_msrv.steps[]?;
    (.uses // "" | startswith("dtolnay/rust-toolchain@"))
    and .with.toolchain == "${{ env.RUSTUP_TOOLCHAIN }}"
    and .with.targets == "wasm32-unknown-unknown");
    "public MSRV must install its selected compiler and Wasm target"),
  require(all(.jobs.dependency_msrv.steps[]?;
    .env.RUSTUP_TOOLCHAIN == null and .if == null and (.["continue-on-error"] // false) == false);
    "public MSRV steps must retain compiler selection and stop on failure"),
  require(any(.jobs.dependency_msrv.steps[]?;
    run_text | test("^\\s*bash scripts/ci/check-public-msrv\\.sh\\s*$"));
    "public MSRV must run its compiler and public feature gate"),
  (.jobs.dependency_msrv.steps as $steps |
    range(0; ($steps | length)) as $index |
    select($steps[$index] | run_text | test("^\\s*bash scripts/ci/check-public-msrv\\.sh\\s*$")) |
    require(any($steps[0:$index][]; run_text | test("^\\s*make\\s+fetch\\s*$"));
      "public MSRV requires earlier locked cache preparation")),
  # Server setup is selected by lane; each lane must retain its matching body.
  ([["core", "ci-core"], ["workspace", "ci-workspace"],
    ["tier-a", "ci-sql-tier-a"], ["tier-b", "ci-sql-tier-b"]][] as $selection |
    require(any(.jobs.rust.strategy.matrix.include[]?;
      .lane == $selection[0] and .make_target == $selection[1]);
      "CI must bind \($selection[0]) to \($selection[1])")),
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
  # Static wrapper fixtures use the locked CLI with substitute server bytes.
  require(any(.jobs.static.steps[]?;
    .if == null and (.["continue-on-error"] // false) == false
    and (run_text | contains("bash scripts/ci/testkit-runner.sh\n"))
    and (run_text | contains("bash scripts/ci/testkit-runner.sh --check")));
    "static wrapper fixtures require explicit locked CLI preparation and admission"),
  require(all(.jobs.static.steps[]?;
    run_text | test("(^|\\s)(install-testkit|testkit-check|install-tools|tools-check)(\\s|$)") | not);
    "static fixtures require the CLI without official server provisioning"),
  # Real server setup belongs to the matrix lane that executes live canisters.
  require(any(.jobs.rust.steps[]?;
    .if == "matrix.lane == 'tier-b'" and (.["continue-on-error"] // false) == false
    and (run_text | test("^\\s*make\\s+install-testkit\\s+testkit-check\\s*$")));
    "Tier B requires explicit Testkit server setup and admission"),
  require(all(.jobs.rust.steps[]?;
    if run_text | test("(^|\\s)(install-testkit|testkit-check|install-tools|tools-check)(\\s|$)") then
      .if == "matrix.lane == 'tier-b'" and (.["continue-on-error"] // false) == false
    else true end);
    "native Rust lanes must not provision or admit an official server"),
  require(any(.jobs.macos_host.steps[]?;
    .if == null and (.["continue-on-error"] // false) == false
    and (run_text | test("^\\s*make\\s+install-dev\\s*$")));
    "native host qualification requires explicit workstation and server setup"),
  # Prepare the selected locked graph before explicit tool setup and checking.
  # A cache action is optional reuse, never a substitute for explicit setup.
  (.jobs.rust.steps as $steps |
    range(0; ($steps | length)) as $index |
    select($steps[$index] | run_text |
      test("\\bmake\\s+(install-host-tools\\s+host-tools-check|install-testkit\\s+testkit-check)\\b")) |
    require(any($steps[0:$index][];
      .if == null and (.["continue-on-error"] // false) == false
      and (run_text | test("^\\s*make\\s+fetch\\s*$")));
      "Rust tool checks require an earlier unconditional make fetch step that stops on failure")))
