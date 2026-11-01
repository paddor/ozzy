//! Runtime-independent scheduling for shard-owned journal CPU work.

mod sorting;
pub(crate) use sorting::sort_by;

pub(crate) const TURN_BYTES: usize = 256 * 1024;
/// Fixed-width index rows in one turn.
const TURN_ROWS: usize = 1024;
/// A decoded unit's share of a turn, in rows.
const UNIT_ROWS: usize = TURN_ROWS / 64;

#[derive(Default)]
pub(crate) struct Budget {
    bytes: usize,
    rows: usize,
}

impl Budget {
    /// One bounded unit remains indivisible. Account its encoded or decoded
    /// work before continuing with another unit, even if all I/O was ready.
    pub(crate) async fn charge(&mut self, bytes: usize) {
        self.spend(bytes, UNIT_ROWS).await;
    }

    /// One fixed-width index row. Even with a binary search it costs far less
    /// than a decoded unit, and a segment of small records has millions of them.
    pub(crate) async fn charge_row(&mut self, bytes: usize) {
        self.spend(bytes, 1).await;
    }

    async fn spend(&mut self, bytes: usize, rows: usize) {
        self.bytes = self.bytes.saturating_add(bytes);
        self.rows += rows;
        if self.bytes >= TURN_BYTES || self.rows >= TURN_ROWS {
            ozzy_io::signal::yield_once().await;
            self.bytes = 0;
            self.rows = 0;
        }
    }
}
