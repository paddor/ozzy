# ozzy-ui

Admin console of [Ozzy](https://github.com/paddor/ozzy) brokers. The
`ozzy-tui` binary polls one broker's `admin` endpoint and shows its status in
the terminal.

## Run

```bash
ozzy-tui ipc://@ozzy-admin
ozzy-tui --config deployment.toml --broker broker-0
```

- The broker needs an `admin` endpoint, `ipc://` or `inproc://` only. Run the
  console on the broker's host.
- There is no authentication yet: anyone who can connect is an admin.
- `--interval-ms` sets the refresh interval, 1000 by default. Rates are per
  second between two refreshes, measured by the broker's uptime.

## Screens

| Key | Screen | Shows |
| --- | --- | --- |
| `1` | Broker | Identity, connected clients, cluster members with link state and the partitions each leads, partition totals, key counters |
| `2` | Partitions | Each local partition's state, leader, record offsets, producers, commit rate, pending work, and cleanup |
| `3` | Replication | Links to the other brokers, repair counters, and each partition's journal progress and how far each broker is behind |
| `4` | Groups | Consumer groups with their lag; the selected group's members and its cursor on each partition |
| `5` | Threads | Every event counter, totaled or for one thread |

- Tab and Shift+Tab cycle screens.
- Arrows, PgUp/PgDn, Home/End (or `j`, `k`, `g`, `G`) select rows. Left and
  Right (or `h`, `l`) switch panes on Groups and Threads.
- `z` shows zero counters, `r` refreshes now, `q` or Esc quits.
- Click a tab or a row; the wheel scrolls the table under it. Hold Shift to
  select text instead.
- Narrow terminals hide the less important columns.

## Reading the numbers

- Offsets and lag on the Partitions and Groups screens count records. Other
  positions are operation numbers; one operation can hold many records.
- On Replication, a leader knows how far each follower's acknowledgment is
  behind. A follower knows only its own distance to the leader's commit, so
  the other follower shows `-`.
- One broker stores every partition, so it shows every group. Its view can
  trail the leader by a refresh.

## Library

`App` holds the console state and draws it with ratatui. It does no I/O and
reads no clock, so any frontend can drive it: ask `App::requests` what to
send, then feed it replies, link changes, keys, and mouse events. The default
`terminal` feature adds the `client` module and the binary.
