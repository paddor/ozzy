//! Broker CPU layout: one list shared by every broker, or one list per broker.

/// Parsed `--broker-cpus`: `0,1,2,3` is one pool that all brokers share;
/// `0,6/1,7/2,8` gives each of the three brokers its own CPUs, such as both
/// threads of one physical core.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerCpus(Vec<Vec<usize>>);

impl BrokerCpus {
    /// Parse one shared CPU set or slash-separated per-broker sets.
    pub fn parse(text: &str) -> Result<Self, String> {
        let sets = text
            .split('/')
            .map(|set| {
                set.split(',')
                    .map(|cpu| cpu.trim().parse::<usize>().map_err(|e| e.to_string()))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut all = sets.concat();
        all.sort_unstable();
        if !matches!(sets.len(), 1 | 3)
            || sets.iter().any(Vec::is_empty)
            || all.windows(2).any(|w| w[0] == w[1])
        {
            return Err("expected one CPU list, or three disjoint lists separated by '/'".into());
        }
        Ok(Self(sets))
    }

    /// Every broker CPU, sorted.
    pub fn pool(&self) -> Vec<usize> {
        let mut all = self.0.concat();
        all.sort_unstable();
        all
    }

    /// Explicit CPU IDs in each parsed broker set.
    pub fn sets(&self) -> &[Vec<usize>] {
        &self.0
    }

    /// Whether every broker has its own CPUs.
    pub fn per_broker(&self) -> bool {
        self.0.len() > 1
    }

    /// CPUs of each of `brokers` brokers. A single broker gets the whole pool.
    pub fn brokers(&self, brokers: usize) -> Vec<Vec<usize>> {
        if brokers == 1 {
            vec![self.pool()]
        } else if self.per_broker() {
            debug_assert_eq!(brokers, self.0.len());
            self.0.clone()
        } else {
            vec![self.0[0].clone(); brokers]
        }
    }
}

impl std::fmt::Display for BrokerCpus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sets = self
            .0
            .iter()
            .map(|set| {
                set.iter()
                    .map(usize::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect::<Vec<_>>();
        f.write_str(&sets.join("/"))
    }
}

/// Shards or reactors for broker `index`: its CPU count, divided among the
/// brokers that share the same list. A shared pool is one deployment budget.
pub fn shards(cpus: &[Vec<usize>], index: usize) -> usize {
    let sharing = cpus.iter().filter(|set| **set == cpus[index]).count();
    (cpus[index].len() / sharing).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_pool_and_per_broker_lists() {
        let shared = BrokerCpus::parse("0,1,2,3").unwrap();
        assert!(!shared.per_broker());
        assert_eq!(shared.brokers(3), vec![vec![0, 1, 2, 3]; 3]);
        assert_eq!(shards(&shared.brokers(3), 0), 1);
        assert_eq!(shards(&shared.brokers(1), 0), 4);

        let cores = BrokerCpus::parse("0,6/1,7/2,8").unwrap();
        assert!(cores.per_broker());
        assert_eq!(cores.pool(), vec![0, 1, 2, 6, 7, 8]);
        assert_eq!(cores.brokers(3)[1], vec![1, 7]);
        assert_eq!(cores.brokers(1), vec![vec![0, 1, 2, 6, 7, 8]]);
        assert_eq!(shards(&cores.brokers(3), 2), 2);
        assert_eq!(cores.to_string(), "0,6/1,7/2,8");

        for bad in ["", "0,6/1,7", "0,6/0,7/2,8", "0,0", "0,6//2,8", "a"] {
            assert!(BrokerCpus::parse(bad).is_err(), "{bad}");
        }
    }
}
