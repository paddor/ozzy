//! Newline-separated JSON events of the Rust OMQ compression benchmark.
//!
//! Same five event kinds, field values, and byte-exact truncation as that
//! benchmark's `json_payload`. Its generator always starts at event zero; here a
//! record number selects a disjoint event range, so records never repeat.
//! Values come from one multiplicative hash per event. No filler text exists,
//! so payloads compress like the log events they model. No OS randomness or
//! corpus reuse enters the timed path.

const LEVELS: &[&str] = &["DEBUG", "INFO", "WARN", "ERROR"];
const SERVICES: &[&str] = &[
    "api-gateway",
    "auth-svc",
    "order-svc",
    "payment-svc",
    "notify-svc",
];
const METHODS: &[&str] = &["GET", "POST", "PUT", "DELETE", "PATCH"];
const PATHS: &[&str] = &[
    "/v1/widgets",
    "/v1/users",
    "/v1/orders",
    "/v2/events",
    "/v1/health",
];
const REGIONS: &[&str] = &[
    "us-east-1",
    "us-west-2",
    "eu-west-1",
    "ap-south-1",
    "eu-central-1",
];
const STATUSES: &[usize] = &[200, 201, 204, 400, 404, 500, 502, 503];
const ACTIONS: &[&str] = &["login", "logout", "purchase", "refund", "update_profile"];
const CURRENCIES: &[&str] = &["USD", "EUR", "GBP", "JPY", "CHF"];
const ERROR_CODES: &[&str] = &[
    "TIMEOUT",
    "RATE_LIMITED",
    "AUTH_EXPIRED",
    "INVALID_INPUT",
    "UPSTREAM_5XX",
];
/// Every event is longer than this, so record event ranges never overlap.
const MINIMUM_EVENT_BYTES: usize = 128;

/// Append exactly `size` bytes of events; the last event is cut at `size`.
pub fn append_json_record(output: &mut Vec<u8>, size: usize, number: u64) {
    let start = output.len();
    let stride = (size / MINIMUM_EVENT_BYTES + 1) as u32;
    let mut counter = ((number ^ (number >> 32)) as u32).wrapping_mul(stride);
    while output.len() - start < size {
        append_event(&mut Out(output), counter);
        counter = counter.wrapping_add(1);
    }
    output.truncate(start + size);
}

struct Out<'a>(&'a mut Vec<u8>);

impl Out<'_> {
    fn text(&mut self, text: &str) -> &mut Self {
        self.0.extend_from_slice(text.as_bytes());
        self
    }

    fn number(&mut self, value: usize) -> &mut Self {
        self.0
            .extend_from_slice(itoa::Buffer::new().format(value).as_bytes());
        self
    }

    /// The event hash as eight lowercase hex digits.
    fn id(&mut self, hash: usize) -> &mut Self {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for shift in (0..32).step_by(4).rev() {
            self.0.push(HEX[(hash >> shift) & 15]);
        }
        self
    }

    /// `scaled / 10^decimals` with exactly `decimals` fraction digits.
    fn fixed(&mut self, scaled: usize, decimals: u32) -> &mut Self {
        let unit = 10_usize.pow(decimals);
        self.number(scaled / unit).text(".");
        let fraction = scaled % unit;
        if decimals == 2 && fraction < 10 {
            self.text("0");
        }
        self.number(fraction)
    }

    fn timestamp(&mut self, hash: usize) -> &mut Self {
        self.text("\"ts\":\"2026-04-27T12:34:56.")
            .id(hash)
            .text("Z\"")
    }
}

fn append_event(out: &mut Out<'_>, counter: u32) {
    let h = counter.wrapping_mul(0x9E37_79B1) as usize;
    match h % 5 {
        0 => request(out, h),
        1 => session(out, h),
        2 => transaction(out, h),
        3 => metric(out, h),
        _ => failure(out, h),
    }
    out.text("\n");
}

fn request(out: &mut Out<'_>, h: usize) {
    out.text("{")
        .timestamp(h)
        .text(",\"level\":\"")
        .text(LEVELS[h % LEVELS.len()])
        .text("\",\"service\":\"")
        .text(SERVICES[(h >> 4) % SERVICES.len()])
        .text("\",\"trace_id\":\"")
        .id(h)
        .text("\",\"method\":\"")
        .text(METHODS[(h >> 8) % METHODS.len()])
        .text("\",\"path\":\"")
        .text(PATHS[(h >> 12) % PATHS.len()])
        .text("/")
        .id(h)
        .text("\",\"status\":")
        .number(STATUSES[(h >> 20) % STATUSES.len()])
        .text(",\"latency_ms\":")
        .number(h % 500 + 1)
        .text("}");
}

fn session(out: &mut Out<'_>, h: usize) {
    out.text("{\"event\":\"")
        .text(ACTIONS[(h >> 4) % ACTIONS.len()])
        .text("\",\"user_id\":\"u-")
        .id(h)
        .text("\",\"session\":\"")
        .id(h)
        .text("\",")
        .timestamp(h)
        .text(",\"ip\":\"10.")
        .number((h >> 8) % 256)
        .text(".")
        .number((h >> 12) % 256)
        .text(".")
        .number((h >> 16) % 256)
        .text("\",\"region\":\"")
        .text(REGIONS[(h >> 16) % REGIONS.len()])
        .text("\",\"user_agent\":\"Mozilla/5.0\",\"success\":true}");
}

fn transaction(out: &mut Out<'_>, h: usize) {
    out.text("{\"type\":\"transaction\",\"id\":\"txn-")
        .id(h)
        .text("\",\"user_id\":\"u-")
        .id(h)
        .text("\",\"amount\":")
        .fixed(h % 99900 + 100, 2)
        .text(",\"currency\":\"")
        .text(CURRENCIES[(h >> 4) % CURRENCIES.len()])
        .text("\",")
        .timestamp(h)
        .text(",\"items\":[{\"sku\":\"SKU-")
        .id(h)
        .text("\",\"qty\":")
        .number((h >> 8) % 10 + 1)
        .text(",\"price\":")
        .fixed(h % 9900 + 100, 2)
        .text("}]}");
}

fn metric(out: &mut Out<'_>, h: usize) {
    let service = SERVICES[(h >> 4) % SERVICES.len()];
    out.text("{\"type\":\"metric\",\"service\":\"")
        .text(service)
        .text("\",\"host\":\"")
        .text(service)
        .text("-")
        .id(h)
        .text(".svc.cluster.local\",\"region\":\"")
        .text(REGIONS[(h >> 16) % REGIONS.len()])
        .text("\",")
        .timestamp(h)
        .text(",\"cpu\":")
        .fixed(h % 1000, 1)
        .text(",\"mem_mb\":")
        .number((h >> 8) % 8192 + 256)
        .text(",\"gc_ms\":")
        .number((h >> 12) % 200)
        .text(",\"conns\":")
        .number((h >> 16) % 500 + 10)
        .text("}");
}

fn failure(out: &mut Out<'_>, h: usize) {
    let service = SERVICES[(h >> 8) % SERVICES.len()];
    out.text("{\"type\":\"error\",\"code\":\"")
        .text(ERROR_CODES[(h >> 4) % ERROR_CODES.len()])
        .text("\",\"service\":\"")
        .text(service)
        .text("\",\"region\":\"")
        .text(REGIONS[(h >> 16) % REGIONS.len()])
        .text("\",\"trace_id\":\"")
        .id(h)
        .text("\",")
        .timestamp(h)
        .text(",\"stack\":[\"at ")
        .text(service)
        .text("::handle (src/handler.rs:")
        .number((h >> 12) % 500 + 1)
        .text(")\",\"at ")
        .text(service)
        .text("::dispatch (src/router.rs:")
        .number((h >> 16) % 300 + 1)
        .text(")\",\"at tokio::runtime::task (")
        .id(h)
        .text(")\"]}");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The OMQ benchmark's formatted event, kept only as the reference here.
    fn formatted(counter: u32) -> String {
        let h = counter.wrapping_mul(0x9E37_79B1) as usize;
        let id = format!("{h:08x}");
        let mut event = match h % 5 {
            0 => format!(
                r#"{{"ts":"2026-04-27T12:34:56.{id}Z","level":"{level}","service":"{service}","trace_id":"{id}","method":"{method}","path":"{path}/{id}","status":{status},"latency_ms":{latency}}}"#,
                level = LEVELS[h % LEVELS.len()],
                service = SERVICES[(h >> 4) % SERVICES.len()],
                method = METHODS[(h >> 8) % METHODS.len()],
                path = PATHS[(h >> 12) % PATHS.len()],
                status = STATUSES[(h >> 20) % STATUSES.len()],
                latency = (h % 500) as u32 + 1,
            ),
            1 => format!(
                r#"{{"event":"{action}","user_id":"u-{id}","session":"{id}","ts":"2026-04-27T12:34:56.{id}Z","ip":"10.{a}.{b}.{c}","region":"{region}","user_agent":"Mozilla/5.0","success":true}}"#,
                action = ACTIONS[(h >> 4) % ACTIONS.len()],
                region = REGIONS[(h >> 16) % REGIONS.len()],
                a = (h >> 8) % 256,
                b = (h >> 12) % 256,
                c = (h >> 16) % 256,
            ),
            2 => format!(
                r#"{{"type":"transaction","id":"txn-{id}","user_id":"u-{id}","amount":{amount:.2},"currency":"{currency}","ts":"2026-04-27T12:34:56.{id}Z","items":[{{"sku":"SKU-{id}","qty":{qty},"price":{price:.2}}}]}}"#,
                currency = CURRENCIES[(h >> 4) % CURRENCIES.len()],
                amount = ((h % 99900) as f64 + 100.0) / 100.0,
                qty = (h >> 8) % 10 + 1,
                price = ((h % 9900) as f64 + 100.0) / 100.0,
            ),
            3 => format!(
                r#"{{"type":"metric","service":"{service}","host":"{service}-{id}.svc.cluster.local","region":"{region}","ts":"2026-04-27T12:34:56.{id}Z","cpu":{cpu:.1},"mem_mb":{mem},"gc_ms":{gc},"conns":{conns}}}"#,
                service = SERVICES[(h >> 4) % SERVICES.len()],
                region = REGIONS[(h >> 16) % REGIONS.len()],
                cpu = (h % 1000) as f64 / 10.0,
                mem = (h >> 8) % 8192 + 256,
                gc = (h >> 12) % 200,
                conns = (h >> 16) % 500 + 10,
            ),
            _ => format!(
                r#"{{"type":"error","code":"{code}","service":"{service}","region":"{region}","trace_id":"{id}","ts":"2026-04-27T12:34:56.{id}Z","stack":["at {service}::handle (src/handler.rs:{line})","at {service}::dispatch (src/router.rs:{line2})","at tokio::runtime::task ({id})"]}}"#,
                code = ERROR_CODES[(h >> 4) % ERROR_CODES.len()],
                service = SERVICES[(h >> 8) % SERVICES.len()],
                region = REGIONS[(h >> 16) % REGIONS.len()],
                line = (h >> 12) % 500 + 1,
                line2 = (h >> 16) % 300 + 1,
            ),
        };
        event.push('\n');
        event
    }

    #[test]
    fn direct_encoding_matches_the_omq_benchmark_format_byte_for_byte() {
        let mut kinds = [false; 5];
        for counter in (0..200_000).chain(u32::MAX - 1000..=u32::MAX) {
            let mut direct = Vec::new();
            append_event(&mut Out(&mut direct), counter);
            let expected = formatted(counter);
            assert_eq!(direct, expected.as_bytes(), "event {counter}");
            assert!(expected.len() > MINIMUM_EVENT_BYTES, "event {counter}");
            kinds[counter.wrapping_mul(0x9E37_79B1) as usize % 5] = true;
        }
        assert_eq!(kinds, [true; 5]);
    }

    #[test]
    fn records_have_exact_sizes_keep_their_prefix_and_never_share_events() {
        for size in [24, 120, 1016, 8184, 16376] {
            let mut output = b"prefix".to_vec();
            append_json_record(&mut output, size, 7);
            assert_eq!(output.len(), 6 + size);
            assert!(output.starts_with(b"prefix{"));
            let mut again = Vec::new();
            append_json_record(&mut again, size, 7);
            assert_eq!(again, output[6..]);

            // Neighboring records and equal sequences of different lanes differ
            // in every complete event.
            let mut events = std::collections::BTreeSet::new();
            for number in [7, 8, 9, (1 << 56) | 7] {
                let mut record = Vec::new();
                append_json_record(&mut record, size, number);
                let text = std::str::from_utf8(&record).unwrap();
                for event in text.split_inclusive('\n').filter(|e| e.ends_with('\n')) {
                    assert!(events.insert(event.to_owned()), "repeated event");
                }
            }
        }
    }

    #[test]
    fn a_batch_of_records_compresses_like_log_events_not_like_filler() {
        let mut batch = Vec::new();
        for number in 0..2048 {
            append_json_record(&mut batch, 1016, number);
        }
        let mut compressed = vec![0; lz4rip::get_maximum_output_size(batch.len())];
        let bytes = lz4rip::block::Compressor::new()
            .compress_into(&batch, &mut compressed)
            .unwrap();
        assert!(bytes * 2 < batch.len(), "{bytes}");
    }
}
