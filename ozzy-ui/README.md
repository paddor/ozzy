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
- To try every screen with live traffic, follow
  [Try it](https://github.com/paddor/ozzy/blob/main/GETTING_STARTED.md#try-it):
  `ozzy quickstart` runs a broker and the `ozzy_demo` example of `ozzy-broker`
  writes and reads as group `workers`.

## Screens

| Key | Screen | Shows |
| --- | --- | --- |
| `1` | Broker | Identity, connected clients, cluster members with link state and the partitions each leads, partition totals, key counters |
| `2` | Partitions | Each local partition's state, leader, record offsets, producers, segments and their size, dedup fill, commit and record rates, pending work, and cleanup; the selected partition's retention policy and dedup limit |
| `3` | Replication | Links to the other brokers, repair counters, and each partition's journal progress and how far each broker is behind |
| `4` | Groups | Consumer groups with their lag; the selected group's members and its cursor on each partition; `d` deletes the selected group after a `y` |
| `5` | Threads | Every event counter, totaled or for one thread |
| `6` | Logs | The broker process's last 64 diagnostics, newest first |

- A single broker replicates nothing: it has no Replication screen, so
  Groups is `3`, Threads `4`, and Logs `5`. The Broker and Partitions screens
  leave out cluster members, elections, leaders, views, and replica
  counters.
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
send, then feed it replies, link changes, keys, and mouse events. A key can
return `Action::Command`; send it and feed its reply to
`App::command_reply`. The default `terminal` feature adds the `client`
module and the binary.
