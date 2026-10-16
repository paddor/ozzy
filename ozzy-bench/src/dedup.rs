//! Deterministic redundant feeds with independent producer identities.

/// Redundant copies of each logical feed and a fixed hot-partition share.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::Args)]
pub struct Feeds {
    /// Independent producers sending the same ordered event IDs and bytes.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..=32))]
    pub redundant_producers: u32,
    /// Percent of equally paced logical feeds routed to partition zero.
    #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u32).range(0..=100))]
    pub hot_partition_percent: u32,
    /// Leading logical feeds sent by every redundant producer; later feeds
    /// have one producer. Zero copies every feed.
    #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u32).range(0..=32))]
    pub redundant_feeds: u32,
}

impl Default for Feeds {
    fn default() -> Self {
        Self {
            redundant_producers: 1,
            hot_partition_percent: 0,
            redundant_feeds: 0,
        }
    }
}

impl Feeds {
    /// Whether generation and verification use the redundant-feed contract.
    pub fn enabled(self) -> bool {
        self != Self::default()
    }

    /// Number of logical feeds represented by the physical writer count.
    pub fn logical_writers(self, writers: usize) -> usize {
        let copies = self.redundant_producers as usize;
        match self.redundant_feeds as usize {
            0 => writers / copies,
            redundant => writers - redundant * (copies - 1),
        }
    }

    /// Physical writers that send `logical` feeds.
    pub fn writers(self, logical: usize) -> usize {
        let redundant = match self.redundant_feeds as usize {
            0 => logical,
            redundant => redundant.min(logical),
        };
        logical + redundant * (self.redundant_producers as usize - 1)
    }

    /// Writers sending one logical feed.
    pub fn copies(self, lane: usize) -> usize {
        let redundant = self.redundant_feeds as usize;
        if redundant == 0 || lane < redundant {
            self.redundant_producers as usize
        } else {
            1
        }
    }

    /// Event identity lane, shared by the independent copies.
    pub fn logical_lane(self, writer: usize) -> usize {
        let copies = self.redundant_producers as usize;
        let redundant = self.redundant_feeds as usize;
        if redundant == 0 || writer < redundant * copies {
            writer / copies
        } else {
            writer - redundant * (copies - 1)
        }
    }

    /// Earliest scheduled copy of a logical feed.
    pub fn primary(self, lane: usize) -> usize {
        let copies = self.redundant_producers as usize;
        let redundant = self.redundant_feeds as usize;
        if redundant == 0 || lane < redundant {
            lane * copies
        } else {
            lane + redundant * (copies - 1)
        }
    }

    /// Fixed destination; redundant copies always use the same routing key.
    pub fn partition(self, lane: usize, writers: usize, partitions: usize) -> usize {
        let hot = self.logical_writers(writers) * self.hot_partition_percent as usize / 100;
        if hot == 0 {
            lane % partitions
        } else if lane < hot || partitions == 1 {
            0
        } else {
            1 + (lane - hot) % (partitions - 1)
        }
    }

    /// Refuse truncated feed groups and approximate skew percentages.
    pub fn validate(self, writers: usize) -> Result<(), &'static str> {
        let copies = self.redundant_producers as usize;
        let redundant = self.redundant_feeds as usize;
        if copies == 0
            || copies > 32
            || self.hot_partition_percent > 100
            || writers == 0
            || writers > 32
            || (redundant == 0 && !writers.is_multiple_of(copies))
            || (redundant != 0 && (copies == 1 || writers < redundant * copies))
        {
            return Err("redundant feeds require complete groups and at most 32 physical writers");
        }
        if !(self.logical_writers(writers) * self.hot_partition_percent as usize)
            .is_multiple_of(100)
        {
            return Err("hot partition percentage must represent an exact number of logical feeds");
        }
        Ok(())
    }

    /// Forward the nondefault controls to a worker process.
    pub fn append(self, arguments: &mut Vec<String>) {
        if self.redundant_producers != 1 {
            arguments.extend([
                "--redundant-producers".into(),
                self.redundant_producers.to_string(),
            ]);
        }
        if self.hot_partition_percent != 0 {
            arguments.extend([
                "--hot-partition-percent".into(),
                self.hot_partition_percent.to_string(),
            ]);
        }
        if self.redundant_feeds != 0 {
            arguments.extend(["--redundant-feeds".into(), self.redundant_feeds.to_string()]);
        }
    }

    /// Physical and logical feed counts a result row reports; one each without
    /// redundant feeds.
    pub fn reported(row: &serde_json::Value) -> [u64; 2] {
        let ingest = &row["dedup_ingest"];
        match (
            ingest["physical_feeds"].as_u64(),
            ingest["logical_feeds"].as_u64(),
        ) {
            (Some(physical), Some(logical)) if logical != 0 => [physical, logical],
            _ => [ingest["redundant_producers"].as_u64().unwrap_or(1), 1],
        }
    }

    /// Whether confirmed attempts are exactly the verified unique records
    /// scaled by the reported feed shape.
    pub fn attempts_match(row: &serde_json::Value) -> bool {
        let [physical, logical] = Self::reported(row);
        row["total_verified_records"]
            .as_u64()
            .and_then(|n| n.checked_mul(physical))
            == row["total_confirmed_records"]
                .as_u64()
                .and_then(|n| n.checked_mul(logical))
    }

    /// Independently check every feed's count, original bytes, and receipt offsets.
    /// Return one expected unique prefix per logical feed for reader verification.
    pub fn logical_rows(
        self,
        writers: usize,
        rows: &[serde_json::Value],
    ) -> crate::BenchResult<Vec<serde_json::Value>> {
        self.validate(writers).map_err(crate::bench_error)?;
        if !self.enabled() {
            return Ok(rows.to_vec());
        }
        let mut lanes = vec![None; writers];
        for worker in rows {
            for row in worker["lanes"]
                .as_array()
                .ok_or_else(|| crate::bench_error("missing feed lanes"))?
            {
                let lane = usize::try_from(
                    row["lane"]
                        .as_u64()
                        .ok_or_else(|| crate::bench_error("missing feed lane"))?,
                )?;
                if lane >= writers || lanes[lane].replace(row.clone()).is_some() {
                    return Err(crate::bench_error("duplicate or unknown physical feed"));
                }
                serde_json::from_value::<[u8; 16]>(row["digest_xxh3_128"].clone())?;
                serde_json::from_value::<[u8; 16]>(
                    row["confirmed_offset_digest_xxh3_128"].clone(),
                )?;
                row["total"]
                    .as_u64()
                    .ok_or_else(|| crate::bench_error("missing feed count"))?;
            }
        }
        let mut logical = Vec::new();
        for lane in 0..self.logical_writers(writers) {
            let first = self.primary(lane);
            let primary = lanes[first]
                .as_ref()
                .ok_or_else(|| crate::bench_error("missing primary feed"))?;
            for copy in 0..self.copies(lane) {
                let row = lanes[first + copy]
                    .as_ref()
                    .ok_or_else(|| crate::bench_error("missing redundant feed"))?;
                for field in [
                    "total",
                    "partition",
                    "digest_xxh3_128",
                    "confirmed_offset_digest_xxh3_128",
                ] {
                    if row[field] != primary[field] {
                        return Err(crate::bench_error(format!(
                            "redundant feed {lane}/{copy} differs in {field}"
                        )));
                    }
                }
            }
            let mut row = primary.clone();
            row["lane"] = serde_json::json!(lane);
            logical.push(row);
        }
        Ok(vec![serde_json::json!({"lanes":logical})])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn redundant_proofs_reject_changed_bytes_offsets_counts_and_missing_copies() {
        let feeds = Feeds {
            redundant_producers: 2,
            ..Feeds::default()
        };
        let row = |lane| {
            json!({"lane":lane,"partition":0,"total":7,
            "digest_xxh3_128":vec![1_u8;16], "confirmed_offset_digest_xxh3_128":vec![2_u8;16]})
        };
        let rows = vec![json!({"lanes":[row(0),row(1)]})];
        let expected = feeds.logical_rows(2, &rows).unwrap();
        assert_eq!(expected[0]["lanes"].as_array().unwrap().len(), 1);
        for field in [
            "total",
            "partition",
            "digest_xxh3_128",
            "confirmed_offset_digest_xxh3_128",
        ] {
            let mut changed = rows.clone();
            changed[0]["lanes"][1][field] = if field.ends_with("128") {
                json!(vec![9_u8; 16])
            } else {
                json!(1)
            };
            assert!(feeds.logical_rows(2, &changed).is_err(), "{field}");
        }
        assert!(feeds.logical_rows(2, &[json!({"lanes":[row(0)]})]).is_err());
        assert!(
            feeds
                .logical_rows(2, &[json!({"lanes":[row(0),row(0)]})])
                .is_err()
        );
    }

    #[test]
    fn leading_feeds_have_copies_and_later_feeds_one_writer() {
        let feeds = Feeds {
            redundant_producers: 2,
            redundant_feeds: 4,
            ..Feeds::default()
        };
        assert_eq!(feeds.writers(8), 12);
        feeds.validate(12).unwrap();
        assert_eq!(feeds.logical_writers(12), 8);
        let lanes = (0..12).map(|w| feeds.logical_lane(w)).collect::<Vec<_>>();
        assert_eq!(lanes, [0, 0, 1, 1, 2, 2, 3, 3, 4, 5, 6, 7]);
        for lane in 0..8 {
            let first = feeds.primary(lane);
            assert_eq!(feeds.logical_lane(first), lane);
            assert_eq!(feeds.copies(lane), if lane < 4 { 2 } else { 1 });
            assert!(first == 0 || feeds.logical_lane(first - 1) < lane);
        }
        let row = |lane| {
            json!({"lane":lane,"partition":lane,"total":7,
            "digest_xxh3_128":vec![1_u8;16], "confirmed_offset_digest_xxh3_128":vec![2_u8;16]})
        };
        let rows = vec![json!({"lanes":(0..12).map(|w| {
            let mut row = row(w);
            row["partition"] = json!(feeds.logical_lane(w));
            row
        }).collect::<Vec<_>>()})];
        assert_eq!(
            feeds.logical_rows(12, &rows).unwrap()[0]["lanes"]
                .as_array()
                .unwrap()
                .len(),
            8
        );
        assert!(feeds.validate(7).is_err());
        assert!(
            Feeds {
                redundant_producers: 1,
                ..feeds
            }
            .validate(12)
            .is_err()
        );
    }

    #[test]
    fn copies_share_event_lanes_and_exact_hot_destinations() {
        let feeds = Feeds {
            redundant_producers: 3,
            hot_partition_percent: 75,
            ..Feeds::default()
        };
        feeds.validate(24).unwrap();
        let destinations = (0..24)
            .map(|writer| feeds.partition(feeds.logical_lane(writer), 24, 8))
            .collect::<Vec<_>>();
        assert_eq!(&destinations[..18], &[0; 18]);
        assert_eq!(&destinations[18..], &[1, 1, 1, 2, 2, 2]);
        assert!(feeds.validate(23).is_err());
        assert!(
            Feeds {
                hot_partition_percent: 80,
                ..feeds
            }
            .validate(24)
            .is_err()
        );
    }
}
