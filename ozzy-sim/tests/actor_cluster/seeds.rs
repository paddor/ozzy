//! Seed streams and the schedule hash. One run seed splits into independent
//! child streams, so a new draw in one stream never shifts another.

use omq_tokio::Message;

/// splitmix64: small, fast, and fixed across Rust releases.
#[derive(Debug, Clone)]
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..bound`. Bounds are small, so modulo bias is negligible.
    pub(crate) fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    /// Uniform in `[0, 1)`.
    pub(crate) fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    pub(crate) fn range(&mut self, low: f64, high: f64) -> f64 {
        low + (high - low) * self.unit()
    }

    /// Zero probability draws nothing, so a disabled fault keeps the stream.
    pub(crate) fn chance(&mut self, probability: f64) -> bool {
        probability > 0.0 && self.unit() < probability
    }

    /// Whole ticks with this mean. A zero mean draws nothing.
    pub(crate) fn exponential(&mut self, mean: f64) -> u64 {
        if mean <= 0.0 {
            return 0;
        }
        (-mean * (1.0 - self.unit()).ln()) as u64
    }

    pub(crate) fn shuffle<T>(&mut self, items: &mut [T]) {
        for index in (1..items.len()).rev() {
            let other = self.below(index as u64 + 1) as usize;
            items.swap(index, other);
        }
    }
}

/// Child seeds, drawn in this order. Append new streams at the end.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Seeds {
    pub network: u64,
    pub disk: u64,
    pub workload: u64,
    /// Broker order within each tick.
    pub executor: u64,
    /// Parameters of the `swarm` profile.
    pub swarm: u64,
}

impl Seeds {
    pub(crate) fn derive(seed: u64) -> Self {
        let mut parent = Rng::new(seed);
        Self {
            network: parent.next(),
            disk: parent.next(),
            workload: parent.next(),
            executor: parent.next(),
            swarm: parent.next(),
        }
    }
}

/// FNV-1a over everything the run schedules: sends, deliveries, drops, disk
/// completions, and replies. Stable across Rust releases, unlike
/// `DefaultHasher`. Equal seeds on one build must give equal hashes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Trace(u64);

impl Default for Trace {
    fn default() -> Self {
        Self(0xCBF2_9CE4_8422_2325)
    }
}

impl Trace {
    pub(crate) fn bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0100_0000_01B3);
        }
    }

    pub(crate) fn word(&mut self, word: u64) {
        self.bytes(&word.to_le_bytes());
    }

    pub(crate) fn message(&mut self, message: &Message) {
        self.word(message.len() as u64);
        for index in 0..message.len() {
            let part = message.part_slice(index).unwrap_or_default();
            self.word(part.len() as u64);
            self.bytes(part);
        }
    }

    pub(crate) fn value(self) -> u64 {
        self.0
    }
}
