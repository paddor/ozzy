use crate::{
    Action, App, Key, Mouse, Screen,
    app::Pane,
    model::{Behind, OVERVIEW, REPAIRS},
};
use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, GroupId, NodeId,
    admin::{
        Broker, Connections, Counters, EventKind, GroupCursor, GroupStatus, LogEntry, LogLevel,
        PartitionLog, PartitionName, PartitionState, PartitionStatus, Partitions, Reply, Request,
    },
};
use ratatui::{Terminal, backend::TestBackend};
use std::num::NonZeroU64;

const A: NodeId = NodeId::from_bytes([1; 16]);
const B: NodeId = NodeId::from_bytes([2; 16]);
const C: NodeId = NodeId::from_bytes([3; 16]);
const WORKERS: ConsumerGroupId = ConsumerGroupId::from_bytes([7; 16]);
const MEMBER: ConsumerMemberId = ConsumerMemberId::from_bytes([8; 16]);

fn group(index: u8) -> GroupId {
    GroupId::from_bytes([index; 16])
}

fn broker() -> Reply {
    Reply::Broker(Broker {
        name: "broker-a".into(),
        id: A,
        pid: 42,
        uptime_ms: 1_000,
        max_clients: 64,
        shards: 2,
        brokers: vec![
            ("broker-a".into(), A),
            ("broker-b".into(), B),
            ("broker-c".into(), C),
        ],
        partitions: (0..3)
            .map(|index| PartitionName {
                group: group(index),
                topic: "orders".into(),
                partition: u32::from(index),
                shard: u32::from(index % 2),
            })
            .collect(),
    })
}

fn status(index: u8, committed: u64, leader: NodeId) -> PartitionStatus {
    PartitionStatus {
        group: group(index),
        state: PartitionState::Normal,
        ready: true,
        disk_pending: false,
        view: 1,
        leader: Some(leader),
        accepted: committed,
        committed,
        applied: committed,
        pending_operations: 0,
        pending_body_bytes: 0,
        validation_cycles: 0,
        reclaimed_bytes: 0,
        written: committed,
        durable: committed,
        leader_commit: committed,
        followers: Vec::new(),
        log: None,
    }
}

/// Partition 2's shard did not answer.
fn partitions(uptime_ms: u64, committed: u64) -> Reply {
    Reply::Partitions(Partitions {
        uptime_ms,
        missing_shards: 1,
        rows: vec![status(0, committed, A), status(1, 7, B)],
    })
}

/// Two threads share a name; the second one counts faster.
fn counters(uptime_ms: u64, turns: u64) -> Reply {
    Reply::Counters(Counters {
        uptime_ms,
        events: vec![
            ("shard_turn".into(), EventKind::Count),
            ("io_write_bytes".into(), EventKind::Count),
            ("dedup_live_entries".into(), EventKind::Gauge),
            ("io_failed".into(), EventKind::Count),
        ],
        totals: vec![3 * turns, 4096 * turns, 5, 0],
        threads: vec![
            ("ozzy/shard/0".into(), vec![turns, 0, 7, 0]),
            (
                "ozzy/shard/0".into(),
                vec![2 * turns, 4096 * turns, u64::MAX - 1, 0],
            ),
            ("exited".into(), vec![0; 4]),
        ],
    })
}

fn screen(app: &mut App) -> String {
    let mut terminal = Terminal::new(TestBackend::new(170, 24)).unwrap();
    terminal.draw(|frame| app.draw(frame)).unwrap();
    terminal.backend().to_string()
}

#[test]
fn partitions_show_names_leaders_rates_and_silent_shards() {
    let mut app = App::new("ipc://@admin");
    app.reply(broker());
    app.reply(partitions(1_000, 10));
    app.reply(partitions(3_000, 30));
    assert_eq!(app.key(Key::Char('2')), Action::None);
    let text = screen(&mut app);
    assert!(text.contains("silent shards: 1"), "{text}");
    for expected in [
        "orders/0",
        "broker-a",
        "orders/1",
        "broker-b",
        "10.0",
        "no answer",
    ] {
        assert!(text.contains(expected), "{expected}:\n{text}");
    }
    let summary = app.summary();
    assert_eq!(
        (summary.partitions, summary.ready, summary.leading),
        (3, 2, 1)
    );
    assert!((summary.rate - 10.0).abs() < f64::EPSILON);
}

#[test]
fn thread_rates_follow_threads_by_name_and_place() {
    let mut app = App::new("ipc://@admin");
    app.reply(counters(1_000, 10));
    app.reply(counters(2_000, 20));
    app.key(Key::Char('5'));
    let totals = app.event_rows();
    assert_eq!(
        totals.iter().map(|row| row.name).collect::<Vec<_>>(),
        ["shard_turn", "io_write_bytes", "dedup_live_entries"]
    );
    assert_eq!(totals[0].rate, Some(30.0));
    assert_eq!(totals[2].rate, None);
    app.key(Key::Down);
    app.key(Key::Down);
    let second = app.event_rows();
    assert_eq!(second[0].rate, Some(20.0));
    assert_eq!(second[1].rate, Some(40_960.0));
    let text = screen(&mut app);
    for expected in ["all threads", "exited", "40.0 KiB", "-2", "(zeros hidden)"] {
        assert!(text.contains(expected), "{expected}:\n{text}");
    }
    app.key(Key::Char('z'));
    assert_eq!(app.event_rows().len(), 4);
    // The event pane keeps its selection inside its rows.
    app.key(Key::Right);
    app.key(Key::End);
    assert_eq!(app.event_table.selected(), Some(3));
    app.key(Key::Char('z'));
    screen(&mut app);
    assert_eq!(app.event_table.selected(), Some(2));
}

#[test]
fn restarts_and_lost_links_ask_for_broker_facts_again() {
    let mut app = App::new("ipc://@admin");
    assert_eq!(
        app.requests(),
        [
            Request::Broker,
            Request::Counters,
            Request::Partitions,
            Request::Connections,
            Request::Logs,
        ]
    );
    app.link(false);
    assert!(screen(&mut app).contains("connecting"));
    app.link(true);
    app.reply(broker());
    app.reply(counters(5_000, 1));
    assert_eq!(
        app.requests(),
        [
            Request::Counters,
            Request::Partitions,
            Request::Connections,
            Request::Logs,
        ]
    );
    app.reply(counters(6_000, 2));
    assert!(app.event_rows()[0].rate.is_some());
    // Uptime went back: a new process, so no rate across it.
    app.reply(counters(1_000, 3));
    assert_eq!(app.requests()[0], Request::Broker);
    assert!(app.event_rows()[0].rate.is_none());
    app.reply(broker());
    app.link(false);
    assert_eq!(app.requests()[0], Request::Broker);
    assert!(screen(&mut app).contains("lost"));
}

#[test]
fn keys_switch_screens_and_keep_selections_in_range() {
    let mut app = App::new("ipc://@admin");
    app.reply(broker());
    assert_eq!(app.screen(), Screen::Broker);
    app.key(Key::Tab);
    assert_eq!(app.screen(), Screen::Partitions);
    app.key(Key::BackTab);
    app.key(Key::BackTab);
    assert_eq!(app.screen(), Screen::Logs);
    app.key(Key::Char('9'));
    assert_eq!(app.screen(), Screen::Logs);
    app.key(Key::Char('2'));
    app.key(Key::PageDown);
    assert_eq!(app.partition_table.selected(), Some(2));
    app.key(Key::Char('k'));
    assert_eq!(app.partition_table.selected(), Some(1));
    app.key(Key::Home);
    assert_eq!(app.partition_table.selected(), Some(0));
    assert_eq!(app.key(Key::Char('r')), Action::Refresh);
    assert_eq!(app.key(Key::Esc), Action::Quit);
    app.failed("no reply in 5 s");
    assert!(screen(&mut app).contains("no reply in 5 s"));
    app.reply(partitions(1, 1));
    assert!(!screen(&mut app).contains("no reply"));
}

#[test]
fn mouse_picks_tabs_rows_and_panes() {
    let mut app = App::new("ipc://@admin");
    app.reply(broker());
    app.reply(partitions(1, 1));
    app.reply(counters(1_000, 10));
    screen(&mut app);
    // " 1 Broker │ 2 Partitions ": the divider is column 10.
    app.mouse(Mouse::Click, 10, 0);
    assert_eq!(app.screen(), Screen::Broker);
    app.mouse(Mouse::Click, 12, 0);
    assert_eq!(app.screen(), Screen::Partitions);
    screen(&mut app);
    // Row 1 is the border and row 2 the header; three rows follow.
    app.mouse(Mouse::Click, 3, 5);
    assert_eq!(app.partition_table.selected(), Some(2));
    app.mouse(Mouse::Click, 3, 6);
    app.mouse(Mouse::Click, 3, 2);
    assert_eq!(app.partition_table.selected(), Some(2));
    app.mouse(Mouse::ScrollUp, 3, 6);
    assert_eq!(app.partition_table.selected(), Some(1));
    app.key(Key::Char('5'));
    screen(&mut app);
    // The thread list is 28 columns wide; events fill the rest.
    app.mouse(Mouse::Click, 40, 3);
    assert_eq!(app.focus, Pane::Detail);
    app.mouse(Mouse::ScrollDown, 40, 3);
    assert_eq!(app.event_table.selected(), Some(1));
    app.mouse(Mouse::Click, 2, 4);
    assert_eq!(
        (app.focus, app.thread_table.selected()),
        (Pane::List, Some(1))
    );
}

#[test]
fn broker_screen_summarizes_the_cluster_and_key_counters() {
    let mut app = App::new("ipc://@admin");
    app.reply(broker());
    app.reply(counters(1_000, 1));
    app.reply(partitions(1_000, 1));
    let text = screen(&mut app);
    for expected in [
        "broker-a connecting",
        "Admin          ipc://@admin",
        "? of 64",
        "00:00:01",
        "broker-b",
        "this broker",
        "Disk writes",
        "4.00 KiB",
        "Shards silent",
    ] {
        assert!(text.contains(expected), "{expected}:\n{text}");
    }
    app.reply(Reply::Connections(Connections {
        brokers: vec![(B, true), (C, false)],
        clients: 3,
    }));
    let text = screen(&mut app);
    for expected in ["3 of 64", "up", "down"] {
        assert!(text.contains(expected), "{expected}:\n{text}");
    }
}

/// Partition 0 is led here with one follower behind; partition 1 follows
/// broker-b and is behind its commit.
#[test]
fn replication_shows_who_is_behind_as_known_here() {
    let mut app = App::new("ipc://@admin");
    app.reply(broker());
    let mut led = status(0, 100, A);
    led.accepted = 110;
    led.written = 105;
    led.durable = 104;
    led.followers = vec![(B, 110), (C, 97)];
    let mut follows = status(1, 50, B);
    follows.leader_commit = 58;
    let mut electing = status(2, 0, A);
    electing.state = PartitionState::Electing;
    app.reply(Reply::Partitions(Partitions {
        uptime_ms: 1,
        missing_shards: 0,
        rows: vec![led.clone(), follows.clone(), electing.clone()],
    }));
    let behind = |status, broker| app.behind(status, broker);
    assert_eq!(behind(&led, A), Behind::Leader);
    assert_eq!(behind(&led, B), Behind::Ops(0));
    assert_eq!(behind(&led, C), Behind::Ops(13));
    assert_eq!(behind(&follows, A), Behind::Ops(8));
    assert_eq!(behind(&follows, B), Behind::Leader);
    assert_eq!(behind(&follows, C), Behind::Unknown);
    assert_eq!(behind(&electing, A), Behind::Unknown);
    app.key(Key::Char('3'));
    let text = screen(&mut app);
    for expected in [
        "leader", "follower", "electing", "13", "105", "104", "Links", "Repairs",
    ] {
        assert!(text.contains(expected), "{expected}:\n{text}");
    }
}

fn log(next_offset: u64, groups: Vec<GroupStatus>, cursors: Vec<GroupCursor>) -> PartitionLog {
    PartitionLog {
        first_offset: 0,
        next_offset,
        producers: 1,
        retention_age_millis: NonZeroU64::new(7 * 86_400_000),
        retention_bytes: None,
        policy_revision: 2,
        segments: 3,
        segment_bytes: 3 << 26,
        dedup_entries: 1_200,
        dedup_limit: NonZeroU64::new(100_000),
        groups,
        cursors,
    }
}

/// Partition 0 coordinates the group; both partitions have its cursors.
fn grouped(uptime_ms: u64, with_logs: bool) -> Reply {
    let mut coordinator = status(0, 10, A);
    let mut source = status(1, 10, B);
    if with_logs {
        coordinator.log = Some(log(
            40,
            vec![GroupStatus {
                id: WORKERS,
                name: "workers".into(),
                revision: 3,
                members: vec![(MEMBER, C)],
            }],
            vec![GroupCursor {
                group: WORKERS,
                member: Some(MEMBER),
                offset: Some(30),
            }],
        ));
        source.log = Some(log(
            5,
            Vec::new(),
            vec![GroupCursor {
                group: WORKERS,
                member: None,
                offset: None,
            }],
        ));
    }
    Reply::Partitions(Partitions {
        uptime_ms,
        missing_shards: 0,
        rows: vec![coordinator, source],
    })
}

#[test]
fn groups_join_plans_and_cursors_and_survive_a_busy_journal() {
    let mut app = App::new("ipc://@admin");
    app.reply(broker());
    app.key(Key::Char('4'));
    assert!(screen(&mut app).contains("Groups (none)"));
    app.reply(grouped(1_000, true));
    // A busy journal answers without its log; the last one stays.
    app.reply(grouped(2_000, false));
    let groups = app.group_rows();
    assert_eq!(groups.len(), 1);
    let group = &groups[0];
    assert_eq!(group.label, "orders/workers");
    assert_eq!(group.cursors.len(), 2);
    assert_eq!(group.lag(), 10);
    let text = screen(&mut app);
    for expected in [
        "orders/workers",
        "plan revision 3",
        &MEMBER.to_string(),
        "orders/0",
        "orders/1",
        "none",
        "Groups",
    ] {
        assert!(text.contains(expected), "{expected}:\n{text}");
    }
    // A restarted broker's logs are not carried over.
    app.reply(grouped(10, false));
    assert!(app.group_rows().is_empty());
    app.key(Key::Char('l'));
    app.key(Key::Down);
    assert_eq!(app.cursor_table.selected(), Some(0));
}

#[test]
fn partitions_show_storage_and_limits() {
    let mut app = App::new("ipc://@admin");
    app.reply(broker());
    app.reply(grouped(1_000, true));
    app.key(Key::Char('2'));
    let text = screen(&mut app);
    for expected in [
        "Segs",
        "192 MiB",
        "1.20%",
        "retention age 7d (revision 2)  dedup 1,200 of 100,000",
    ] {
        assert!(text.contains(expected), "{expected}:\n{text}");
    }
}

/// A single broker replicates nothing: no Replication tab, and the tabs
/// after it move up.
#[test]
fn a_single_broker_has_no_replication_screen() {
    let mut app = App::new("ipc://@admin");
    app.key(Key::Char('3'));
    assert_eq!(app.screen(), Screen::Replication);
    let mut single = broker();
    if let Reply::Broker(facts) = &mut single {
        facts.brokers.truncate(1);
    }
    app.reply(single);
    assert_eq!(app.screen(), Screen::Partitions);
    app.key(Key::Char('3'));
    assert_eq!(app.screen(), Screen::Groups);
    app.key(Key::Tab);
    app.key(Key::Tab);
    assert_eq!(app.screen(), Screen::Logs);
    app.key(Key::Tab);
    assert_eq!(app.screen(), Screen::Broker);
    app.key(Key::BackTab);
    assert_eq!(app.screen(), Screen::Logs);
    app.key(Key::Char('6'));
    assert_eq!(app.screen(), Screen::Logs);
    app.key(Key::Char('1'));
    app.reply(counters(1_000, 1));
    let text = screen(&mut app);
    for expected in ["3 Groups", "4 Threads", "5 Logs", "Tab/1-5", "I/O errors"] {
        assert!(text.contains(expected), "{expected}:\n{text}");
    }
    for hidden in ["Replication", "Cluster", "Electing", "Led here", "Replica"] {
        assert!(!text.contains(hidden), "{hidden}:\n{text}");
    }
    app.reply(partitions(1_000, 1));
    app.key(Key::Char('2'));
    let text = screen(&mut app);
    assert!(text.contains("Next offset"), "{text}");
    for hidden in ["Leader", "View"] {
        assert!(!text.contains(hidden), "{hidden}:\n{text}");
    }
}

#[test]
fn logs_show_the_newest_first() {
    let mut app = App::new("ipc://@admin");
    app.reply(broker());
    app.key(Key::Char('6'));
    assert!(screen(&mut app).contains("Logs (none)"));
    let entry = |uptime_ms, level, message: &str| LogEntry {
        uptime_ms,
        level,
        thread: "ozzy/admin".into(),
        message: message.into(),
    };
    app.reply(Reply::Logs(vec![
        entry(1_000, LogLevel::Info, "alpha"),
        entry(61_000, LogLevel::Error, "omega"),
    ]));
    let text = screen(&mut app);
    for expected in ["00:01:01", "error", "ozzy/admin"] {
        assert!(text.contains(expected), "{expected}:\n{text}");
    }
    assert!(text.find("omega") < text.find("alpha"), "{text}");
    app.key(Key::Down);
    assert_eq!(app.log_table.selected(), Some(1));
}

/// Records per second come from fresh logs only: a busy journal's
/// carried-over log keeps the last rate.
#[test]
fn record_rates_skip_carried_over_logs() {
    let mut app = App::new("ipc://@admin");
    app.reply(broker());
    let at = |uptime_ms, next_offset| {
        let mut status = status(0, 10, A);
        status.log = Some(log(next_offset, Vec::new(), Vec::new()));
        Reply::Partitions(Partitions {
            uptime_ms,
            missing_shards: 0,
            rows: vec![status],
        })
    };
    app.reply(at(1_000, 100));
    assert_eq!(app.partition_rows()[0].records, None);
    app.reply(at(3_000, 300));
    assert_eq!(app.partition_rows()[0].records, Some(100.0));
    app.reply(partitions(4_000, 10));
    assert_eq!(app.partition_rows()[0].records, Some(100.0));
    assert!((app.summary().records - 100.0).abs() < f64::EPSILON);
    app.key(Key::Char('2'));
    assert!(screen(&mut app).contains("Records/s"));
}

#[test]
fn groups_are_deleted_only_after_asking() {
    let mut app = App::new("ipc://@admin");
    app.reply(broker());
    app.key(Key::Char('4'));
    app.key(Key::Char('d'));
    assert!(screen(&mut app).contains("no group to delete"));
    app.reply(grouped(1_000, true));
    assert_eq!(app.key(Key::Char('d')), Action::None);
    assert!(screen(&mut app).contains("Delete group orders/workers? y deletes"));
    // Any other key cancels, without its usual effect.
    assert_eq!(app.key(Key::Esc), Action::None);
    assert!(!screen(&mut app).contains("Delete group"));
    app.key(Key::Char('d'));
    let delete = Request::DeleteGroup {
        topic: "orders".into(),
        name: "workers".into(),
    };
    assert_eq!(app.key(Key::Char('y')), Action::Command(delete));
    assert!(screen(&mut app).contains("deleting orders/workers"));
    // No second deletion while one runs.
    app.key(Key::Char('d'));
    assert!(!screen(&mut app).contains("Delete group"));
    app.reply(Reply::GroupDeleted(Some(WORKERS)));
    let deleted = format!("deleted orders/workers ({WORKERS})");
    assert!(screen(&mut app).contains(&deleted));
    app.key(Key::Char('d'));
    app.key(Key::Char('y'));
    app.command_failed("no reply in 70 s");
    assert!(screen(&mut app).contains("orders/workers: no reply in 70 s"));
}

/// The Broker and Replication screens name broker events, so a renamed
/// event must fail here.
#[test]
fn labeled_events_exist() {
    let names = ozzy_telemetry::totals()
        .rows()
        .map(|(name, ..)| name)
        .collect::<Vec<_>>();
    for (event, _) in OVERVIEW.iter().chain(&REPAIRS) {
        assert!(names.contains(event), "{event}");
    }
}
