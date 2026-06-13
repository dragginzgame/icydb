use std::{
    fs,
    path::{Path, PathBuf},
};

pub fn read_source(relative_path: &str) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(relative_path);

    fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()))
}

pub fn read_sources(relative_paths: &[&str]) -> String {
    relative_paths
        .iter()
        .map(|relative_path| read_source(relative_path))
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn rust_sources_under(relative_path: &str) -> Vec<PathBuf> {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root.push(relative_path);

    rust_sources_under_path(root)
}

pub fn read_rust_sources_under(relative_path: &str) -> String {
    rust_sources_under(relative_path)
        .iter()
        .map(|path| {
            fs::read_to_string(path)
                .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn relative_source_path(path: &Path) -> String {
    let manifest_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.strip_prefix(manifest_root)
        .unwrap_or_else(|err| panic!("failed to relativize {}: {err}", path.display()))
        .to_string_lossy()
        .replace('\\', "/")
}

pub fn strip_cfg_test_items(source: &str) -> String {
    let mut output = String::new();
    let mut pending_cfg_test = false;
    let mut skipping_cfg_test_item = false;
    let mut skip_depth = 0usize;

    for line in source.lines() {
        let trimmed = line.trim();
        if skip_depth > 0 {
            skip_depth = skip_depth
                .saturating_add(line.matches('{').count())
                .saturating_sub(line.matches('}').count());
            continue;
        }
        if skipping_cfg_test_item {
            let opens = line.matches('{').count();
            let closes = line.matches('}').count();
            if opens > 0 {
                skip_depth = opens.saturating_sub(closes);
                skipping_cfg_test_item = skip_depth > 0;
            } else if trimmed.ends_with(';') {
                skipping_cfg_test_item = false;
            }
            continue;
        }

        if trimmed.starts_with("#[cfg(test)]") {
            pending_cfg_test = true;
            continue;
        }
        if pending_cfg_test {
            let opens = line.matches('{').count();
            let closes = line.matches('}').count();
            if opens > 0 {
                skip_depth = opens.saturating_sub(closes);
                skipping_cfg_test_item = skip_depth > 0;
            } else if !trimmed.ends_with(';') {
                skipping_cfg_test_item = true;
            }
            pending_cfg_test = false;
            continue;
        }

        output.push_str(line);
        output.push('\n');
    }

    output
}

pub fn compact_source(source: &str) -> String {
    source
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}

pub fn entity_attribute_blocks(source: &str) -> Vec<&str> {
    let mut blocks = Vec::new();
    let mut search_from = 0usize;

    while let Some(relative_start) = source[search_from..].find("#[entity(") {
        let start = search_from + relative_start;
        let mut depth = 0u32;
        let mut end = None;

        for (offset, character) in source[start..].char_indices() {
            match character {
                '(' => depth = depth.saturating_add(1),
                ')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        end = Some(start + offset + character.len_utf8());
                        break;
                    }
                }
                _ => {}
            }
        }

        let Some(end) = end else {
            panic!("unterminated #[entity(...)] attribute in source");
        };
        blocks.push(&source[start..end]);
        search_from = end;
    }

    blocks
}

pub fn rust_sources_under_path(root: PathBuf) -> Vec<PathBuf> {
    let mut sources = Vec::new();
    let mut pending = vec![root];
    while let Some(path) = pending.pop() {
        let entries = fs::read_dir(&path)
            .unwrap_or_else(|err| panic!("failed to list {}: {err}", path.display()));
        for entry in entries {
            let path = entry
                .unwrap_or_else(|err| panic!("failed to read directory entry: {err}"))
                .path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                sources.push(path);
            }
        }
    }

    sources.sort();
    sources
}
