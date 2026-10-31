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
# Cargo owns fetching the selected registry/public Git graph; preparation must
# precede offline callers and stop the lane on failure.
def fetches_locked_workspace:
  (run_text | test("^\\s*cargo\\s+fetch\\s+--locked\\s*$"))
  and .if == null and (.["continue-on-error"] // false) == false;
# Native-only lanes select the complete common roster without Testkit's server
# extension; live lanes must explicitly select the existing product owner.
def admits_server_tools:
  run_text | test("(^|\\s)(install-testkit|testkit-check)(\\s|$)");
def has_run($pattern):
  any(.steps[]?; (run_text | test($pattern)) and .if == null and (.["continue-on-error"] // false) == false);
def msrv_gate($script):
  . as $job |
  require($job.env.RUSTUP_TOOLCHAIN | if type == "string" then test("^[0-9]+\\.[0-9]+\\.[0-9]+$") else false end;
    "MSRV must explicitly select its compiler"),
  require(any($job.steps[]?; (.uses // "" | startswith("dtolnay/rust-toolchain@"))
    and .with.toolchain == "${{ env.RUSTUP_TOOLCHAIN }}" and .with.targets == "wasm32-unknown-unknown");
    "MSRV must install its selected compiler and Wasm target"),
  require(all($job.steps[]?; .env.RUSTUP_TOOLCHAIN == null and .if == null and (.["continue-on-error"] // false) == false);
    "MSRV steps must retain compiler selection and stop on failure"),
  require($job | has_run("^\\s*bash scripts/ci/" + $script + "\\s*$"); "MSRV must run its owned compiler gate"),
  ($job.steps as $steps | range(0; $steps|length) as $i |
    select($steps[$i] | run_text | contains($script)) |
    require(any($steps[0:$i][]; fetches_locked_workspace); "MSRV needs earlier locked cache preparation"));
def matrix_routes($routes):
  . as $job |
  require($job.strategy["fail-fast"] == false; "parallel Rust validation must retain every lane"),
  ($routes[] as $route | require(any($job.strategy.matrix.include[]?; .lane==$route[0] and .make_target==$route[1]);
    "missing owned Rust lane \($route[0])")),
  require(any($job.steps[]?; (run_text|contains("make \"$MAKE_TARGET\"")) and .env.MAKE_TARGET=="${{ matrix.make_target }}");
    "Rust validation must select its matrix Make target");

def ic_preparation($when; $caller):
  .steps as $steps |
  require(any(range(0; $steps|length); . as $i |
    ($steps[$i] | .if==$when and (.["continue-on-error"]//false)==false
      and (run_text|contains("make install-ic-tools >")) and (run_text|contains("make ic-tools-check >")))
    and any($steps[$i+1:][]; run_text|contains($caller)));
    "IC tools require setup and admission before the owned build/report");

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

(. as $workflow | .jobs | to_entries[] |
  select((.value.env.RUSTC_WRAPPER // $workflow.env.RUSTC_WRAPPER // "") == "sccache") |
  require(any(.value.steps[]?; (.uses // "" | startswith("mozilla-actions/sccache-action@")));
    "job \(.key) selects sccache without provisioning it")),

(select($file == ".github/workflows/ci.yml" or $file == ".github/workflows/canic-adapter.yml") |
  (.jobs | to_entries[] | .key as $job | .value.steps[]? |
    select(.uses=="./.github/actions/retain-failure-evidence") |
    require(.with["ic-tools"]=="${{ env.IC_TOOLS }}"; "\($job) evidence needs its IC selection")),
  require(.jobs.macos_host | has_run("(^|\\n)\\s*make install-dev\\s*($|\\n)"); "native host requires workstation preparation"),
  require(.env.IC_TOOLS=="icp ic-wasm wasm-opt"; "workstation needs its complete IC selection"),
  require(.jobs.macos_host.env.IC_TOOLS==null; "native host must retain workstation IC selection"),
  ([["rust","wasm-opt"],["wire_browser","wasm-opt"]][] as $selection |
    require(.jobs[$selection[0]].env.IC_TOOLS==$selection[1]; "owned lane needs its IC selection"))),

(select($file == ".github/workflows/ci.yml") |
  . as $ci |
  (["dependency_msrv","static","rust","macos_host","wire_browser","check"][] as $job |
    require($ci.jobs|has($job); "CI is missing product job \($job)")),
  (.jobs.dependency_msrv | msrv_gate("check-public-msrv.sh")),
  (.jobs.rust | ic_preparation("matrix.lane == 'workspace'"; "make \"$MAKE_TARGET\"")),
  (.jobs.rust | matrix_routes([["core","ci-core"],["workspace","ci-workspace"],["tier-a","ci-sql-tier-a"]])),
  require(.jobs.check | needs_are(["static","rust","macos_host","wire_browser"]);
    "product check must aggregate only product lanes"),
  require(.jobs | has("wasm_size_report")|not; "Canic Wasm evidence belongs to adapter qualification"),
  require(.jobs | has("release")|not; "canister artifact delivery belongs to adapter qualification"),
  require(any(.jobs.static.steps[]?; run_text|test("^\\s*make ci-static\\s*$")); "static must use its local product authority"),
  require(.jobs.wire_browser | has_run("^\\s*make check-wire-browser\\s*$"); "product browser gate must exercise native codec contracts"),
  require(any(.jobs.macos_host.steps[]?; run_text|contains("make check-portable-automation")); "native host must execute portable product fixtures"),
  (.jobs | to_entries[] | .key as $job | .value.steps[]? |
    require(run_text|test("testing/canic-adapter|fetch-adapter|install-testkit|testkit-check|check-canic-adapter-msrv|icydb-testing-integration|test-wire-browser|build-canister-production")|not;
      "product job \($job) dispatches adapter qualification")),
  require(all(.jobs.rust.strategy.matrix.include[]?; .lane != "tier-b"); "installed Tier B belongs to adapter qualification"),
  (.jobs.rust.steps as $steps | range(0; $steps|length) as $i |
    select($steps[$i]|run_text|contains("make \"$MAKE_TARGET\"")) |
    require(any($steps[0:$i][]; fetches_locked_workspace); "Rust product checks require earlier locked fetching"),
    require(any($steps[0:$i][]; .if=="matrix.lane == 'workspace'" and (run_text|test("^\\s*rustup target add wasm32-unknown-unknown\\s*$"))
      and (.["continue-on-error"]//false)==false); "workspace cache tests require their Wasm target")),
  (["static","rust"][] as $job |
    require(any(.jobs[$job].steps[]?; .if==null and (.["continue-on-error"]//false)==false
      and (run_text|contains("make install-tools LOCAL_TOOL_INSTALL_TARGETS="))
      and (run_text|contains("make tools-check LOCAL_TOOL_CHECK_TARGETS="))); "product \($job) needs explicit common setup and admission")),
  (["static","rust","macos_host"][] as $job |
    require(any(.jobs[$job].steps[]?; .if=="failure()" and .uses=="./.github/actions/retain-failure-evidence"
      and .with["temp-root"]=="${{ runner.temp }}" and .with["repository-root"]=="${{ github.workspace }}"
      and .with["ic-tools"]=="${{ env.IC_TOOLS }}"); "product \($job) must retain tooling failures"))),

(select($file == ".github/workflows/canic-adapter.yml") |
  . as $adapter |
  (["dependency_msrv","static","rust","macos_host","wire_browser","check","wasm_size_report","release"][] as $job |
    require($adapter.jobs|has($job); "adapter qualification is missing \($job)")),
  require(.on|has("workflow_dispatch"); "adapter qualification must be explicitly dispatchable"),
  require((.on.push.paths|length)>0 and (.on.pull_request.paths|length)>0;
    "adapter qualification must scope automatic runs to integration changes"),
  require(all(.on.push.paths[],.on.pull_request.paths[]; . != "crates/**" and . != "Cargo.toml" and . != "Cargo.lock");
    "unrelated core edits must not require adapter alignment"),
  (.jobs.dependency_msrv | msrv_gate("check-canic-adapter-msrv.sh")),
  require(.jobs.wasm_size_report.env.IC_TOOLS=="ic-wasm wasm-opt"; "adapter Wasm report needs its IC selection"),
  (.jobs.rust | ic_preparation("matrix.lane == 'workspace' || matrix.lane == 'tier-b'"; "make \"$MAKE_TARGET\"")),
  (.jobs.wasm_size_report | ic_preparation(null; "bash scripts/ci/wasm-size-report.sh")),
  require(any(.jobs.static.steps[]?; .if==null and (.["continue-on-error"]//false)==false
    and (run_text|contains("bash scripts/ci/testkit-runner.sh\n"))
    and (run_text|contains("bash scripts/ci/testkit-runner.sh --check")));
    "adapter static fixtures require explicit CLI preparation/admission"),
  (.jobs.rust | matrix_routes([["workspace","ci-adapter"],["tier-a","ci-adapter-sql-tier-a"],["tier-b","ci-sql-tier-b"]])),
  require(.jobs.static | has_run("^\\s*make check-adapter-automation\\s*$"); "adapter automation needs its local owner"),
  require(.jobs.wire_browser | has_run("^\\s*make test-wire-browser\\s*$"); "installed browser qualification must remain selected"),
  require(.jobs.check | needs_are(["static","rust","macos_host","wire_browser"]); "adapter check must aggregate its own proof"),
  require(.jobs.wasm_size_report | needs_are([]); "adapter Wasm evidence must run independently"),
  require(.jobs.release | needs_are(["dependency_msrv","check","wasm_size_report"]);
    "canister artifacts need adapter MSRV, validation and Wasm evidence"),
  (.jobs.rust.steps as $steps |
    require(any($steps[]; .if=="matrix.lane == 'workspace' || matrix.lane == 'tier-b'"
      and (.["continue-on-error"]//false)==false and (run_text|contains("make install-testkit >"))
      and (run_text|contains("make testkit-check >"))); "installed adapter lanes require Testkit preparation/admission")),
  require(all(.jobs.rust.steps[]?; if admits_server_tools then
    .if=="matrix.lane == 'workspace' || matrix.lane == 'tier-b'" and (.["continue-on-error"]//false)==false
    else true end); "native adapter comparisons must not prepare a live server"),
  (.jobs.rust.steps as $steps | range(0; $steps|length) as $i |
    select($steps[$i] | run_text | test("\\bmake\\s+(install-tools|install-testkit)\\b")) |
    require(any($steps[0:$i][]; fetches_locked_workspace); "adapter setup needs earlier locked cache preparation")),
  (.jobs.rust.steps as $steps | range(0; $steps|length) as $i |
    select($steps[$i] | run_text | contains("make \"$MAKE_TARGET\"")) |
    require(any($steps[0:$i][]; .if=="matrix.lane == 'workspace' || matrix.lane == 'tier-b'"
      and (run_text|test("^\\s*rustup target add wasm32-unknown-unknown\\s*$"))
      and (.["continue-on-error"]//false)==false); "adapter cache and installed tests need their Wasm target")),
  require(any(.jobs.macos_host.steps[]?; .if==null and (.["continue-on-error"]//false)==false
    and (run_text|contains("make install-testkit")) and (run_text|contains("make testkit-check")));
    "native adapter qualification must prepare its server")),

(select($file == ".github/workflows/ci.yml" or $file == ".github/workflows/canic-adapter.yml") |
  require(if .on|type=="object" then if .on.push|type=="object" then .on.push|has("tags")|not else true end else true end;
    "do not duplicate release commits through tag triggers"))
