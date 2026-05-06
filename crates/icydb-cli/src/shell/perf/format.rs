use crate::shell::perf::{ShellLocalRenderAttribution, ShellPerfAttribution};

pub(crate) fn render_perf_suffix(attribution: Option<&ShellPerfAttribution>) -> Option<String> {
    let attribution = attribution?;
    if attribution.total == 0 {
        return None;
    }

    let total = format_instructions(attribution.total);
    let Some(bar) = render_perf_composition_bar(attribution) else {
        return Some(total);
    };

    Some(format!("{total} {bar}"))
}

pub(crate) fn render_shell_render_suffix(
    render_attribution: Option<&ShellLocalRenderAttribution>,
) -> Option<String> {
    let render_attribution = render_attribution?;
    if render_attribution.render_micros == 0 {
        return None;
    }

    Some(format!(
        "{{r={}}}",
        format_render_duration(render_attribution.render_micros)
    ))
}

pub(crate) fn render_pure_covering_suffix(
    attribution: Option<&ShellPerfAttribution>,
) -> Option<String> {
    let attribution = attribution?;
    if attribution.pure_covering_decode == 0 && attribution.pure_covering_row_assembly == 0 {
        return None;
    }

    Some(format!(
        "{{pc={}/{}}}",
        format_instructions(attribution.pure_covering_decode),
        format_instructions(attribution.pure_covering_row_assembly),
    ))
}

pub(crate) fn render_executor_residual_suffix(
    attribution: Option<&ShellPerfAttribution>,
) -> Option<String> {
    let attribution = attribution?;
    let residual = attribution.pure_covering_executor_residual();
    if residual == 0 {
        return None;
    }

    Some(format!("{{er={}}}", format_instructions(residual)))
}

fn format_render_duration(render_micros: u128) -> String {
    if render_micros >= 1_000 {
        let millis_tenths = (render_micros + 50) / 100;
        let whole = millis_tenths / 10;
        let fractional = millis_tenths % 10;

        return format!("{whole}.{fractional}ms");
    }

    format!("{render_micros}us")
}

// Render one compact fixed-order composition bar for compiler/planner/store/executor/decode shares.
fn render_perf_composition_bar(attribution: &ShellPerfAttribution) -> Option<String> {
    let phases = [
        ('c', attribution.compiler),
        ('p', attribution.planner),
        ('s', attribution.store),
        ('e', attribution.executor),
        ('d', attribution.decode),
        ('?', attribution.residual_total()),
    ];
    let phase_total = phases.iter().map(|(_, value)| *value).sum::<u64>();
    if phase_total == 0 {
        return None;
    }

    let width = perf_composition_bar_width(attribution.total);
    let mut allocated = phases
        .iter()
        .map(|(label, value)| PerfBarBucket::new(*label, *value, width, phase_total))
        .collect::<Vec<_>>();
    let assigned = allocated.iter().map(PerfBarBucket::count).sum::<usize>();
    let mut remaining = width.saturating_sub(assigned);

    // Phase 1: distribute the largest rounding remainders first so the bar
    // stays stable while still summing to the configured width exactly.
    allocated.sort_by(|left, right| {
        right
            .remainder
            .cmp(&left.remainder)
            .then_with(|| right.value.cmp(&left.value))
            .then_with(|| left.label.cmp(&right.label))
    });
    for bucket in &mut allocated {
        if remaining == 0 {
            break;
        }
        if bucket.value == 0 {
            continue;
        }

        bucket.count = bucket.count.saturating_add(1);
        remaining = remaining.saturating_sub(1);
    }

    // Phase 2: restore the canonical c/p/s/e/d order in the rendered shell surface.
    allocated.sort_by_key(|bucket| match bucket.label {
        'c' => 0,
        'p' => 1,
        's' => 2,
        'e' => 3,
        'd' => 4,
        '?' => 5,
        _ => 6,
    });

    let mut rendered = String::with_capacity(width.saturating_add(2));
    rendered.push('[');
    for bucket in allocated {
        for _ in 0..bucket.count {
            rendered.push(bucket.label);
        }
    }
    rendered.push(']');

    Some(rendered)
}

// Scale the composition bar by powers of ten so larger queries get a little
// more resolution without letting the footer sprawl indefinitely.
fn perf_composition_bar_width(total_instructions: u64) -> usize {
    let mut width = 10usize;
    let mut threshold = 1_000_000u64;
    while total_instructions >= threshold && width < 50 {
        width = width.saturating_add(5).min(50);
        threshold = threshold.saturating_mul(10);
    }

    width
}

///
/// PerfBarBucket
///
/// Rounded per-phase allocation bucket used while building one shell perf
/// composition bar.
/// This keeps width allocation explicit so the final `[cped...]` footer stays
/// deterministic even when integer rounding leaves leftover cells.
///

struct PerfBarBucket {
    label: char,
    value: u64,
    count: usize,
    remainder: u128,
}

impl PerfBarBucket {
    fn new(label: char, value: u64, width: usize, total: u64) -> Self {
        let scaled = u128::from(value).saturating_mul(width as u128);
        let total = u128::from(total);

        Self {
            label,
            value,
            count: usize::try_from(scaled / total).unwrap_or(usize::MAX),
            remainder: scaled % total,
        }
    }

    const fn count(&self) -> usize {
        self.count
    }
}

fn format_instructions(instructions: u64) -> String {
    if instructions >= 1_000_000 {
        return format_scaled_instructions(instructions, 1_000_000, "Mi");
    }

    if instructions >= 1_000 {
        return format_scaled_instructions(instructions, 1_000, "Ki");
    }

    format!("{instructions}i")
}

fn format_scaled_instructions(instructions: u64, scale: u64, suffix: &str) -> String {
    let scaled_tenths =
        ((u128::from(instructions) * 10) + (u128::from(scale) / 2)) / u128::from(scale);
    let whole = scaled_tenths / 10;
    let fractional = scaled_tenths % 10;

    format!("{whole}.{fractional}{suffix}")
}
