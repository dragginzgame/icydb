# Release-owned projection: only selected group constraints follow the bump.
# Callers provide the selected root dependency aliases; external oracle pins stay fixed.
map(if .file == "Cargo.toml" and .rule == "cargo-exact" and
       .value == ("=" + $previous) and
       (.subject as $name | $packages | index($name)) != null
    then .value = ("=" + $release)
    else . end)
