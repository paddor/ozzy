//! Number, byte, and duration text for table cells.

/// `1234567` as `1,234,567`.
pub(crate) fn count(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// Binary units with three significant digits: `512 B`, `1.50 KiB`.
pub(crate) fn bytes(value: f64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut scaled = value;
    let mut unit = 0;
    while scaled >= 1024.0 && unit + 1 < UNITS.len() {
        scaled /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{scaled:.0} B")
    } else {
        format!("{} {}", significant(scaled), UNITS[unit])
    }
}

/// Decimal units with three significant digits: `950`, `1.23k`, `45.6M`.
pub(crate) fn rate(value: f64) -> String {
    const UNITS: [&str; 5] = ["", "k", "M", "G", "T"];
    let mut scaled = value;
    let mut unit = 0;
    while scaled >= 1000.0 && unit + 1 < UNITS.len() {
        scaled /= 1000.0;
        unit += 1;
    }
    if unit == 0 && (scaled >= 100.0 || scaled == 0.0) {
        format!("{scaled:.0}")
    } else {
        format!("{}{}", significant(scaled), UNITS[unit])
    }
}

fn significant(value: f64) -> String {
    match value {
        value if value >= 100.0 => format!("{value:.0}"),
        value if value >= 10.0 => format!("{value:.1}"),
        value => format!("{value:.2}"),
    }
}

/// `3d 04:05:06`, or `04:05:06` below a day.
pub(crate) fn uptime(ms: u64) -> String {
    let seconds = ms / 1000;
    let (days, rest) = (seconds / 86_400, seconds % 86_400);
    let clock = format!(
        "{:02}:{:02}:{:02}",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60
    );
    if days == 0 {
        clock
    } else {
        format!("{days}d {clock}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_read_at_a_glance() {
        assert_eq!(count(0), "0");
        assert_eq!(count(999), "999");
        assert_eq!(count(1_000), "1,000");
        assert_eq!(count(1_234_567), "1,234,567");
        assert_eq!(bytes(0.0), "0 B");
        assert_eq!(bytes(1023.0), "1023 B");
        assert_eq!(bytes(1536.0), "1.50 KiB");
        assert_eq!(bytes(5.0 * 1024.0 * 1024.0 * 1024.0), "5.00 GiB");
        assert_eq!(rate(0.0), "0");
        assert_eq!(rate(0.5), "0.50");
        assert_eq!(rate(12.34), "12.3");
        assert_eq!(rate(950.0), "950");
        assert_eq!(rate(1234.0), "1.23k");
        assert_eq!(rate(45_600_000.0), "45.6M");
        assert_eq!(uptime(59_999), "00:00:59");
        assert_eq!(uptime(3_723_000), "01:02:03");
        assert_eq!(uptime(2 * 86_400_000 + 5_000), "2d 00:00:05");
    }
}
