# Parsed configuration checks. Shell owns Git inventory, file and path checks.
def full_commit: type == "string" and test("^([0-9a-f]{40}|[0-9a-f]{64})$");
def digest_image: type == "string" and test("^.+@sha256:[0-9a-f]{64}$");
def dependencies:
  [ .dependencies, .["dev-dependencies"], .["build-dependencies"],
    .workspace.dependencies,
    (.target[]? | .dependencies, .["dev-dependencies"], .["build-dependencies"]),
    .patch[]?, .replace ]
  | .[] | select(. != null) | to_entries[]
  | {name: .key, spec: (if .value | type == "string" then {version: .value} else .value end)};
def finding($file; $rule; $subject; $value; $message):
  {file: $file, rule: $rule, subject: $subject, value: $value, message: $message};
def package_dependencies:
  {dependencies, "dev-dependencies": .["dev-dependencies"], "build-dependencies": .["build-dependencies"]},
  (.target[]? | {dependencies, "dev-dependencies": .["dev-dependencies"], "build-dependencies": .["build-dependencies"]})
  | to_entries[] | select(.value != null) | .key as $section
  | .value | to_entries[] | {name: .key, spec: .value, section: $section};
def inheritance_checks($root; $member):
  .file as $file | .data as $data |
  (if $member and $data.package != null and $data.package.version != {workspace: true} then
    finding($file; "cargo-inheritance"; "package.version"; ($data.package.version | tojson); "member package version must inherit workspace.package.version")
  else empty end),
  (if $data.package.version == {workspace: true} and ($root.workspace.package.version | type) != "string" then
    finding($file; "cargo-inheritance"; "package.version"; "null"; "owning workspace must declare package.version")
  else empty end),
  ($data | package_dependencies | . as $dependency |
    if (.spec | type) != "object" or .spec.workspace != true then
      finding($file; "cargo-inheritance"; .name; (.spec | tojson); "package dependencies must inherit workspace.dependencies")
    elif (.spec | keys - ["workspace", "features", "optional", "default-features"] | length) != 0 then
      finding($file; "cargo-inheritance"; .name; (.spec | tojson); "child dependencies may select only features, optionality and default-features")
    elif ($root.workspace.dependencies // {} | has($dependency.name) | not) then
      finding($file; "cargo-inheritance"; .name; (.spec | tojson); "dependency alias is missing from the owning workspace catalog")
    else empty end);
def action_refs:
  (.jobs[]? | select(has("uses")) | .uses),
  (.jobs[]?.steps[]? | select(has("uses")) | .uses),
  (.runs.steps[]? | select(has("uses")) | .uses);
def steps: .jobs[]?.steps[]?, .runs.steps[]?;
def checks:
  .file as $file | .data as $data |
  if .kind == "cargo" then
    $data | dependencies | .name as $name | .spec as $spec |
    if ($spec | type) != "object" then
      finding($file; "cargo-declaration"; $name; ($spec | tojson); "dependency must be a string or table")
    else
      (if $spec | has("git") then
        if ($spec.rev | full_commit) and ($spec | has("branch") or has("tag") | not) then empty
        else finding($file; "cargo-git"; $name; ($spec | tojson); "Git dependency requires a full commit rev, without branch or tag") end
      else empty end),
      (if $spec.version? | type == "string" then
        if $spec.version | test("(^|,)\\s*=\\s*[0-9]") then
          finding($file; "cargo-exact"; $name; $spec.version; "exact version constraint requires a documented compatibility reason")
        elif $spec.version | contains("*") then
          finding($file; "cargo-range"; $name; $spec.version; "use an explicit compatible version requirement instead of a wildcard")
        else empty end
      else empty end)
    end
  elif .kind == "workflow" or .kind == "action" then
    ($data | action_refs | . as $ref |
      if type != "string" then finding($file; "action-ref"; "uses"; ($ref | tojson); "uses must be a literal action reference")
      elif startswith("./") and (split("/") | index("..") | not) then empty
      elif startswith("docker://") and digest_image then empty
      elif test("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(/[^@\\s]+)?@([0-9a-f]{40}|[0-9a-f]{64})$") then empty
      else finding($file; "action-ref"; "uses"; $ref; "external action/workflow requires a full commit SHA; Docker actions require sha256") end),
    ($data | steps | select((.uses? // "") | startswith("actions/checkout@")) |
      select(.with.repository? != null and .with.repository != "${{ github.repository }}") |
      .with.repository as $repository | (.with.ref // "") as $ref |
      if $ref | full_commit then empty
      else finding($file; "checkout-ref"; $repository; $ref; "external checkout requires a full commit ref or an approved moving-input exception") end),
    ($data | .jobs[]? | (.container?, .services[]?) | select(. != null) |
      (if type == "string" then . else .image end) as $image |
      if $image | digest_image then empty
      else finding($file; "container-image"; "image"; $image; "CI container images require a sha256 digest") end),
    ($data | select(.runs.using? == "docker") | .runs.image as $image |
      if $image == "Dockerfile" or
        ($image | startswith("./") and (split("/") | index("..") | not)) or
        ($image | digest_image) then empty
      else finding($file; "container-image"; "image"; $image; "Docker action images require a sha256 digest or a local Dockerfile") end)
  else empty end;

# Optional npm root declarations only; npm ci owns resolution and lifecycle.
def npm_sections: ["dependencies", "devDependencies", "optionalDependencies", "peerDependencies"];
def npm_dependencies:
  . as $package | npm_sections[] as $section |
  ($package[$section] // {}) | objects | to_entries[] |
  {name: .key, spec: .value};
def npm_local: type == "string" and test("^(file:|\\./|\\.\\./|/)");
def npm_git:
  test("^(git(\\+[^:]+)?://|git@|github:|gitlab:|bitbucket:|https?://.*\\.git(#.*)?$|[^/@\\s]+/[^/\\s]+(#|$))");
def npm_exact: type == "string" and test("^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)$");
def npm_checks($file; $lock; $node; $npm):
  . as $package |
  (if ($lock.lockfileVersion == 2 or $lock.lockfileVersion == 3) and
      ($lock.packages | type == "object") and ($lock.packages[""] | type == "object") then
    $lock.packages[""] as $root |
    (["name", "version"][] as $key |
      if $package | has($key) then
        if ($package[$key] | type == "string") and $package[$key] == $lock[$key] and $package[$key] == $root[$key]
        then empty else finding($file; "npm-lock"; $key; "<declaration mismatch>"; "manifest and lock root identity must agree") end
      else empty end),
    (npm_sections[] as $key |
      if ($package[$key] // {}) == ($root[$key] // {}) then empty
      else finding($file; "npm-lock"; $key; "<declaration mismatch>"; "manifest and lock root dependencies must agree") end)
  else finding($file; "npm-lock"; "lockfileVersion/packages"; "<unsupported lock>"; "requires package-lock schema 2 or 3 with a root packages entry") end),
  (npm_sections[] as $key |
    if ($package | has($key)) and (($package[$key] | type) != "object") then
      finding($file; "npm-declaration"; $key; "<invalid declaration>"; "dependency section must be an object")
    else empty end),
  (if ($package | has("packageManager")) then
    if ($package.packageManager | type != "string") then
      finding($file; "npm-toolchain"; "packageManager"; "<invalid declaration>"; "requires npm@X.Y.Z matching the selected npm version")
    elif ($package.packageManager | test("^npm@[0-9]+\\.[0-9]+\\.[0-9]+(\\+sha(224|256|384|512)\\.[0-9a-f]+)?$")) and
         ($package.packageManager | split("+")[0]) == "npm@\($npm)" then empty
    else finding($file; "npm-toolchain"; "packageManager"; "<selection mismatch>"; "requires npm@X.Y.Z matching the selected npm version") end
  else empty end),
  (if ($package | has("engines")) and ($package.engines | type != "object") then
    finding($file; "npm-toolchain"; "engines"; "<invalid declaration>"; "engines must be an object")
  else
    ([{key:"node",version:$node},{key:"npm",version:$npm}][] as $tool |
      $package.engines[$tool.key] as $engine |
      if $engine == null then empty
      elif ($engine | type) != "string" then finding($file; "npm-toolchain"; "engines.\($tool.key)"; "<invalid declaration>"; "engine must be a version requirement")
      elif ($engine | ltrimstr("v") | npm_exact) and ($engine | ltrimstr("v")) != $tool.version then
        finding($file; "npm-toolchain"; "engines.\($tool.key)"; "<selection mismatch>"; "exact engine and selected tool version must agree")
      else empty end)
  end),
  ($package | npm_dependencies |
    if (.spec | type) != "string" or .spec == "" then
      finding($file; "npm-declaration"; .name; "<invalid declaration>"; "dependency must be a nonempty string")
    elif (.spec | npm_local) then empty
    elif (.spec | npm_git) then
      if (.spec | split("#") | length == 2 and (.[1] | full_commit)) then empty
      else finding($file; "npm-git"; .name; "<redacted selector>"; "Git dependency requires a full commit fragment") end
    elif (.spec | startswith("link:") or startswith("workspace:")) then
      finding($file; "npm-declaration"; .name; "<unsupported selector>"; "use npm file directory inputs or registry requirements")
    else empty end);
