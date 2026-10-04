# ozzy-io-pool

Fixed file workers for Ozzy storage devices. Shards submit through bounded lanes;
the pool owns file handles, physical jobs, and worker threads.

Progress jobs have reserved capacity. Explicit shutdown drains admitted work and
closes handles before joining the workers.
