# Cargo owns resolved identity; catalog edits never select a supplier by name alone.
def qualification_member_catalog($product; $catalog):
  [$product.packages[] | select(.id as $id | $product.workspace_members | index($id)) | .name] as $names |
  [.dependencies, ."dev-dependencies", ."build-dependencies",
   ((.target // {})[] | .dependencies, ."dev-dependencies", ."build-dependencies")] |
  all(.[] // {} | to_entries[]; . as $d |
    if ($names | index($d.value.package? // $d.key)) != null or ($catalog | any(.alias == $d.key))
    then ($catalog | any(.alias == $d.key)) and
      (($d.value | type) == "object" and ($d.value.workspace? // false) == true and
       ($d.value | has("version") or has("path") | not))
    else true end);

def qualification_plan($product; $manifest; $plan; $directory; $next):
  . as $qualification |
  [$product.packages[] | select(.id as $id | $product.workspace_members | index($id))] as $owned |
  [.packages[] | select(.id as $id | $qualification.workspace_members | index($id))] as $private |
  if .workspace_root != $directory or ($private | length) == 0 or $manifest.package != null or
     any($private[]; .publish != [] or (.manifest_path | startswith($directory + "/") | not)) or
     any($product.packages[]; .manifest_path | startswith($directory + "/")) or
     any($private[]; .name as $n | $owned | any(.name == $n))
  then error("qualification must be a separate private virtual workspace with no reverse product edges") else . end |
  [($manifest.workspace.dependencies // {} | to_entries[]) |
    . as $d | ($d.value.package? // $d.key) as $name |
    $owned[] | select(.name == $name) | . as $p |
    if ($d.value | type) != "object" or ($d.value.path | type) != "string" or
       ($d.value.path | length) == 0 or ($d.value.path | test("[\r\n\t]")) or
       (($d.value | has("version")) and
        (($d.value.version | type) != "string" or
         ($d.value.version | sub("^[~^=]?[ \t]*"; "")) != $p.version))
    then error("qualification product alias \($d.key) needs its local path and optional full current version")
    else {alias:$d.key, package:$name, path:$d.value.path, manifest:$p.manifest_path, previous:$d.value.version} end] as $catalog |
  # A private package can depend on only a subset of the product workspace.
  # Path-only aliases need no manifest edit; the lock still follows the product.
  [$catalog[] | select(.previous != null and (.package as $n | $plan.members | index($n))) |
    {alias, previous, next:(.previous | capture("^(?<prefix>[~^=]?[ \t]*).*$") | .prefix + $next)}] as $edits |
  [.packages[] | select(.source == null) | . as $p |
    $owned[] | select(.name == $p.name and (.name as $n | $plan.members | index($n))) |
    if .manifest_path != $p.manifest_path or .version != $p.version
    then error("qualification lock contains a foreign product identity") else .name end] as $members |
  {catalog:$catalog, members:$members,
   versions:($private | map({key:.name,value:.version}) | from_entries),
   manifest:{path:"qualification/Cargo.toml",version:"unchanged",dependencies:$edits}};
