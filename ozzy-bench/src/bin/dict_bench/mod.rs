mod codec;
mod corpus;

use super::Args;
use ozzy_bench::automation::Result;
use serde_json::{Value, json};
use std::time::Duration;

pub(super) fn run(args: &Args) -> Result<Value> {
    let mut rows = vec![];
    for random in [false, true] {
        for &record_bytes in &args.record_bytes {
            for &target in &args.body_targets {
                let shape = corpus::Shape {
                    record_bytes,
                    target,
                    random,
                };
                let Some(records) = corpus::record_count(shape)? else {
                    continue;
                };
                let training = corpus::bodies(shape, records, 64, 0)?;
                let bodies = corpus::bodies(shape, records, args.samples, 1 << 40)?;
                let dicts = args
                    .dict_bytes
                    .iter()
                    .map(|&n| codec::train(&training, n))
                    .collect::<Result<Vec<_>>>()?;
                for repetition in 1..=args.repetitions {
                    let mut order: Vec<_> = (0..dicts.len()).collect();
                    if repetition % 2 == 0 {
                        order.reverse();
                    }
                    for index in order {
                        let mut row = codec::measure(
                            &bodies,
                            &dicts[index],
                            Duration::from_millis(args.warmup_ms),
                            Duration::from_millis(args.duration_ms),
                        )?;
                        row["case"] = json!({"pattern":if random {"random"} else {"events"},"record_bytes":record_bytes,
                            "body_target":target,"records_per_operation":records,"repetition":repetition});
                        rows.push(row);
                    }
                }
            }
        }
    }
    if rows.is_empty() {
        return Err("no complete record fits the selected operation targets".into());
    }
    Ok(
        json!({"kind":"lz4-dictionary-experiment","boundary":"codec CPU and modeled group bytes; no broker or disk I/O",
        "worker_cpus":ozzy_bench::automation::isolation::cpus(None)?,
        "training":"first 64 canonical bodies, capacity-sized opaque chunks, bounded COVER trainer",
        "test_first_sequence":1_u64<<40,"warmup_ms_per_codec_phase":args.warmup_ms,"duration_ms_per_codec_phase":args.duration_ms,
        "model":"current entry/seal framing, 32-byte savings threshold, 4 KiB write padding; dictionary persistence/repair cost excluded",
        "timing":"always encode/decode for codec comparison; production skips compression when no aligned block can be saved",
        "measurements":rows}),
    )
}
