# ozzy-io-aio

Linux native AIO writes for Ozzy, with fixed blocking helpers for other file
operations. Backend threads own kernel contexts, descriptors, and completions.

The backend shares device admission and handle bounds with `ozzy-io-pool` and
reserves independent capacity for progress writes.
