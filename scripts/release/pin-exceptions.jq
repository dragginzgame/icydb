# Release-owned projection: only coupled IcyDB constraints follow the bump.
# Callers provide the workspace package names; external oracle pins stay fixed.
map(if .file == "Cargo.toml" and .rule == "cargo-exact" and
       .value == ("=" + $previous) and
       (.subject as $name | $packages | index($name)) != null
    then .value = ("=" + $release)
    else . end)
