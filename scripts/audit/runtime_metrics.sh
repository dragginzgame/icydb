#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
SRC_ROOT="${RUNTIME_METRICS_SRC_ROOT:-$ROOT/crates/icydb-core/src}"
OUTPUT_PATH="${1:-}"

if [[ ! -d "$SRC_ROOT" ]]; then
    echo "[runtime-metrics] source root not found: $SRC_ROOT" >&2
    exit 1
fi

if [[ -n "$OUTPUT_PATH" ]]; then
    mkdir -p "$(dirname "$OUTPUT_PATH")"
fi

export ROOT SRC_ROOT OUTPUT_PATH
python3 - <<'PY'
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(os.environ["ROOT"])
SRC_ROOT = Path(os.environ["SRC_ROOT"])
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

RUNTIME_HEADERS = [
    "module",
    "file",
    "loc",
    "match_count",
    "match_arms_total",
    "avg_match_arms",
    "if_count",
    "if_chain_count",
    "max_branch_depth",
    "fanout",
    "branch_sites_total",
]

EXCLUDED_FILE_PATTERNS = (
    "tests.rs",
    "*_tests.rs",
    "test_*.rs",
)

EXCLUDED_DIR_NAMES = {"tests", "benches", "examples"}

USE_TOKEN_RE = re.compile(r"::|[{},;*]|\bas\b|[A-Za-z_][A-Za-z0-9_]*")
CODE_TOKEN_RE = re.compile(r"=>|::|[{};()]|[A-Za-z_][A-Za-z0-9_]*")
RAW_STRING_START_RE = re.compile(r"(?:br|rb|r|br|rb)(?P<hashes>#+)?\"")


@dataclass
class Metrics:
    loc: int = 0
    match_count: int = 0
    match_arms_total: int = 0
    if_count: int = 0
    if_chain_count: int = 0
    max_branch_depth: int = 0
    fanout: int = 0


def iter_runtime_files(root: Path):
    for path in sorted(root.rglob("*.rs")):
        if path.name == "lib.rs":
            continue
        if any(part in EXCLUDED_DIR_NAMES for part in path.parts):
            continue
        if any(path.match(pattern) for pattern in EXCLUDED_FILE_PATTERNS):
            continue
        yield path


def module_path_for(path: Path) -> str:
    relative = path.relative_to(SRC_ROOT)
    parts = list(relative.parts)
    parts[-1] = parts[-1][:-3]
    if parts[-1] == "mod":
        parts = parts[:-1]
    return "::".join(parts)


def strip_comments_and_literals(text: str) -> str:
    out = []
    i = 0
    n = len(text)
    block_comment_depth = 0

    while i < n:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < n else ""

        if block_comment_depth:
            if ch == "/" and nxt == "*":
                block_comment_depth += 1
                out.extend("  ")
                i += 2
                continue
            if ch == "*" and nxt == "/":
                block_comment_depth -= 1
                out.extend("  ")
                i += 2
                continue
            out.append("\n" if ch == "\n" else " ")
            i += 1
            continue

        if ch == "/" and nxt == "/":
            out.extend("  ")
            i += 2
            while i < n and text[i] != "\n":
                out.append(" ")
                i += 1
            continue

        if ch == "/" and nxt == "*":
            block_comment_depth = 1
            out.extend("  ")
            i += 2
            continue

        raw_match = RAW_STRING_START_RE.match(text, i)
        if raw_match:
            hashes = raw_match.group("hashes") or ""
            terminator = "\"" + hashes
            out.extend(" " * (raw_match.end() - i))
            i = raw_match.end()
            while i < n:
                if text.startswith(terminator, i):
                    out.extend(" " * len(terminator))
                    i += len(terminator)
                    break
                out.append("\n" if text[i] == "\n" else " ")
                i += 1
            continue

        if ch == "b" and nxt == "\"":
            out.extend("  ")
            i += 2
            while i < n:
                cur = text[i]
                if cur == "\\" and i + 1 < n:
                    out.extend("  ")
                    i += 2
                    continue
                if cur == "\"":
                    out.append(" ")
                    i += 1
                    break
                out.append("\n" if cur == "\n" else " ")
                i += 1
            continue

        if ch == "\"":
            out.append(" ")
            i += 1
            while i < n:
                cur = text[i]
                if cur == "\\" and i + 1 < n:
                    out.extend("  ")
                    i += 2
                    continue
                if cur == "\"":
                    out.append(" ")
                    i += 1
                    break
                out.append("\n" if cur == "\n" else " ")
                i += 1
            continue

        if ch == "'" and i + 2 < n:
            lookahead = text[i + 1 : min(n, i + 8)]
            if "\\" in lookahead or "'" in lookahead:
                out.append(" ")
                i += 1
                while i < n:
                    cur = text[i]
                    if cur == "\\" and i + 1 < n:
                        out.extend("  ")
                        i += 2
                        continue
                    if cur == "'":
                        out.append(" ")
                        i += 1
                        break
                    out.append("\n" if cur == "\n" else " ")
                    i += 1
                continue

        out.append(ch)
        i += 1

    return "".join(out)


def strip_comments_only(text: str) -> str:
    out = []
    i = 0
    n = len(text)
    block_comment_depth = 0

    while i < n:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < n else ""

        if block_comment_depth:
            if ch == "/" and nxt == "*":
                block_comment_depth += 1
                out.extend("  ")
                i += 2
                continue
            if ch == "*" and nxt == "/":
                block_comment_depth -= 1
                out.extend("  ")
                i += 2
                continue
            out.append("\n" if ch == "\n" else " ")
            i += 1
            continue

        if ch == "/" and nxt == "/":
            out.extend("  ")
            i += 2
            while i < n and text[i] != "\n":
                out.append(" ")
                i += 1
            continue

        if ch == "/" and nxt == "*":
            block_comment_depth = 1
            out.extend("  ")
            i += 2
            continue

        out.append(ch)
        i += 1

    return "".join(out)


def remove_inline_test_modules(text: str) -> str:
    pattern = re.compile(
        r"\bmod\s+(tests|test_[A-Za-z0-9_]*|[A-Za-z0-9_]*_tests)\s*\{"
    )
    result = text

    while True:
        match = pattern.search(result)
        if match is None:
            return result

        brace_index = result.find("{", match.start())
        depth = 1
        i = brace_index + 1
        while i < len(result) and depth > 0:
            if result[i] == "{":
                depth += 1
            elif result[i] == "}":
                depth -= 1
            i += 1

        if depth != 0:
            return result

        replacement = "".join(
            "\n" if ch == "\n" else " " for ch in result[match.start() : i]
        )
        result = result[: match.start()] + replacement + result[i:]


def logical_loc(text: str) -> int:
    count = 0
    for line in text.splitlines():
        if line.strip():
            count += 1
    return count


def code_tokens(text: str):
    return [match.group(0) for match in CODE_TOKEN_RE.finditer(text)]


def brace_pairs(tokens):
    stack = []
    pairs = {}
    for index, token in enumerate(tokens):
        if token == "{":
            stack.append(index)
        elif token == "}":
            if stack:
                open_index = stack.pop()
                pairs[open_index] = index
    return pairs


def next_token(tokens, start, target, end):
    for index in range(start, end):
        if tokens[index] == target:
            return index
    return None


def count_match_arms(tokens, block_start, block_end, pairs):
    depth = 0
    arms = 0
    index = block_start + 1
    while index < block_end:
        token = tokens[index]
        if token == "{":
            depth += 1
            index += 1
            continue
        if token == "}":
            depth -= 1
            index += 1
            continue
        if token == "=>" and depth == 0:
            arms += 1
        index += 1
    return arms


def analyze_branching(tokens):
    pairs = brace_pairs(tokens)
    metrics = Metrics()

    def walk(start, end, current_depth):
        index = start
        previous_token = None
        while index < end:
            token = tokens[index]

            if token == "if":
                metrics.if_count += 1
                if previous_token != "else":
                    metrics.if_chain_count += 1
                branch_depth = current_depth + 1
                metrics.max_branch_depth = max(metrics.max_branch_depth, branch_depth)

                body_open = next_token(tokens, index + 1, "{", end)
                if body_open is not None and body_open in pairs:
                    body_close = pairs[body_open]
                    walk(body_open + 1, body_close, branch_depth)
                    index = body_close + 1
                    if index + 1 < end and tokens[index] == "else" and tokens[index + 1] == "{":
                        else_open = index + 1
                        else_close = pairs.get(else_open)
                        if else_close is not None:
                            walk(else_open + 1, else_close, branch_depth)
                            index = else_close + 1
                    previous_token = "if"
                    continue

            if token == "match":
                metrics.match_count += 1
                branch_depth = current_depth + 1
                metrics.max_branch_depth = max(metrics.max_branch_depth, branch_depth)

                block_open = next_token(tokens, index + 1, "{", end)
                if block_open is not None and block_open in pairs:
                    block_close = pairs[block_open]
                    metrics.match_arms_total += count_match_arms(
                        tokens,
                        block_open,
                        block_close,
                        pairs,
                    )
                    walk(block_open + 1, block_close, branch_depth)
                    index = block_close + 1
                    previous_token = "match"
                    continue

            if token == "{":
                block_close = pairs.get(index)
                if block_close is not None:
                    walk(index + 1, block_close, current_depth)
                    index = block_close + 1
                    previous_token = "}"
                    continue

            previous_token = token
            index += 1

    walk(0, len(tokens), 0)
    return metrics


def normalize_use_path(current_module: str, raw_segments):
    current_parts = current_module.split("::")
    segments = list(raw_segments)

    if not segments:
        return []

    if segments[0] == "crate":
        return segments[1:]

    if segments[0] == "self":
        return current_parts + segments[1:]

    if segments[0] == "super":
        base = current_parts[:-1]
        while segments and segments[0] == "super":
            if base:
                base = base[:-1]
            segments = segments[1:]
        return base + segments

    return segments


def parse_use_tree(statement: str):
    tokens = USE_TOKEN_RE.findall(statement)
    index = 0

    def parse_entries(prefix):
        nonlocal index
        results = []
        while index < len(tokens):
            token = tokens[index]
            if token in {"}", ";"}:
                break
            if token == ",":
                index += 1
                continue
            results.extend(parse_entry(prefix))
            if index < len(tokens) and tokens[index] == ",":
                index += 1
        return results

    def parse_entry(prefix):
        nonlocal index
        segments = list(prefix)
        while index < len(tokens):
            token = tokens[index]
            if token in {",", "}", ";", "{"} or token == "as":
                break
            if token == "::":
                index += 1
                continue
            if token == "*":
                index += 1
                return [segments]
            segments.append(token)
            index += 1

        if index < len(tokens) and tokens[index] == "as":
            index += 2
            return [segments]

        if index < len(tokens) and tokens[index] == "{":
            index += 1
            nested = parse_entries(segments)
            if index < len(tokens) and tokens[index] == "}":
                index += 1
            return nested

        return [segments]

    return parse_entries([])


def module_prefix_for_import(module_names, current_module, import_segments):
    normalized = normalize_use_path(current_module, import_segments)
    if not normalized:
        return None

    candidate = normalized[0]
    if candidate == current_module:
        return None
    if candidate in module_names:
        return candidate
    return None


def fanout_for(text: str, module_name: str, module_names):
    imports = set()
    use_chunks = re.findall(r"(?:^|\n)\s*(?:pub\s+)?use\s+([^;]+);", text, re.MULTILINE)

    for chunk in use_chunks:
        for import_segments in parse_use_tree(chunk):
            module_prefix = module_prefix_for_import(
                module_names,
                module_name,
                import_segments,
            )
            if module_prefix is not None:
                imports.add(module_prefix)

    return len(imports)


files = list(iter_runtime_files(SRC_ROOT))
module_names = {module_path_for(path) for path in files}
rows = []

for path in files:
    module_name = module_path_for(path)
    raw_text = path.read_text(encoding="utf-8")

    comment_sanitized = strip_comments_only(raw_text)
    comment_sanitized = remove_inline_test_modules(comment_sanitized)

    sanitized = strip_comments_and_literals(raw_text)
    sanitized = remove_inline_test_modules(sanitized)

    tokens = code_tokens(sanitized)
    metrics = analyze_branching(tokens)
    metrics.loc = logical_loc(comment_sanitized)
    metrics.fanout = fanout_for(comment_sanitized, module_name, module_names)

    avg_match_arms = (
        f"{metrics.match_arms_total / metrics.match_count:.2f}"
        if metrics.match_count
        else "0.00"
    )

    rows.append(
        [
            module_name,
            str(path.relative_to(ROOT)),
            str(metrics.loc),
            str(metrics.match_count),
            str(metrics.match_arms_total),
            avg_match_arms,
            str(metrics.if_count),
            str(metrics.if_chain_count),
            str(metrics.max_branch_depth),
            str(metrics.fanout),
            str(metrics.match_count + metrics.if_chain_count),
        ]
    )

output_lines = ["\t".join(RUNTIME_HEADERS)]
output_lines.extend("\t".join(row) for row in rows)
output = "\n".join(output_lines) + "\n"

if OUTPUT_PATH:
    Path(OUTPUT_PATH).write_text(output, encoding="utf-8")
else:
    sys.stdout.write(output)
PY
