//! The console against a real broker on inproc and memory storage.
#![allow(clippy::unwrap_used, clippy::panic, reason = "test code")]

use ozzy_config::{DeploymentMode, HostResources};
use ozzy_proto::ConsumerMemberId;
use ozzy_runtime::replicated::{GroupReader, GroupReaderConfig, WriterRuntime};
use ozzy_sim::broker::{Shape, deployment_with_resources, start_brokers};
use ozzy_ui::{
    Action, App, Key,
    client::{Client, Update},
};
use ratatui::{Terminal, backend::TestBackend};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use uuid::Uuid;

fn host() -> HostResources {
    HostResources {
        cpus: BTreeMap::from([(0, Some(0))]),
        memory_nodes: [0].into(),
        linux_aio: true,
    }
}

/// Wait for the handshake, then apply every reply of one refresh and any
/// command reply that arrives meanwhile.
async fn refresh(client: &mut Client, app: &mut App) {
    let requests = app.requests();
    while !client.refresh(&requests).await {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    app.link(true);
    let mut replies = 0;
    while replies < requests.len() {
        match client.update().await {
            Update::Reply(reply) => {
                app.reply(reply);
                replies += 1;
            }
            Update::Command(reply) => app.command_reply(reply),
            Update::Failed(message) | Update::CommandFailed(message) => panic!("{message}"),
        }
    }
}

fn screen(app: &mut App) -> String {
    let mut terminal = Terminal::new(TestBackend::new(170, 30)).unwrap();
    terminal.draw(|frame| app.draw(frame)).unwrap();
    terminal.backend().to_string()
}

/// The value after `label` on the screen, such as `Commits/s` on the
/// Broker screen.
fn value<'a>(text: &'a str, label: &str) -> &'a str {
    let line = text.lines().find(|line| line.contains(label)).unwrap();
    let rest = &line[line.find(label).unwrap() + label.len()..];
    rest.split_whitespace().next().unwrap()
}

fn assert_shows(text: &str, expected: &[&str]) {
    for expected in expected {
        assert!(text.contains(expected), "{expected}:\n{text}");
    }
}

/// Every partition shows its storage, and the selected one its limits. A
/// busy journal answers without its committed state, so refresh until the
/// limits show.
async fn assert_partitions(client: &mut Client, app: &mut App) {
    app.key(Key::Char('2'));
    let text = loop {
        let text = screen(app);
        if text.contains("retention") {
            break text;
        }
        refresh(client, app).await;
    };
    assert_shows(
        &text,
        &[
            "orders/0",
            "orders/2",
            "local",
            "Next offset",
            "Segs",
            "retention keep all (revision 1)",
        ],
    );
}

/// One member reads; the Groups screen shows it on every partition's
/// cursor once its plan and assignments commit, and the Broker screen
/// counts it as a client.
async fn assert_groups(writer: &ozzy_sim::client::Client, client: &mut Client, app: &mut App) {
    let member = ConsumerMemberId::new();
    let links = writer.group_links().await;
    let mut reader = GroupReader::open(
        links.clone(),
        "orders",
        "workers",
        GroupReaderConfig::new(member),
    )
    .await
    .unwrap();
    let record = reader.next().await.unwrap();
    drop(reader.acknowledge(record.token).unwrap());
    app.key(Key::Char('3'));
    let text = loop {
        refresh(client, app).await;
        let text = screen(app);
        // The members table and three owned cursors.
        if text.matches(&member.to_string()).count() == 4 {
            break text;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    assert_shows(&text, &["orders/workers", "plan revision", "orders/1"]);
    refresh(client, app).await;
    app.key(Key::Char('1'));
    assert_shows(&screen(app), &["2 of", "Groups         1"]);
    reader.close().await.unwrap();
    links.shutdown().await.unwrap();
    assert_delete(client, app).await;
}

/// The console deletes the group after asking.
async fn assert_delete(client: &mut Client, app: &mut App) {
    app.key(Key::Char('3'));
    app.key(Key::Char('d'));
    assert_shows(&screen(app), &["Delete group orders/workers? y deletes"]);
    let Action::Command(request) = app.key(Key::Char('y')) else {
        panic!("a delete command expected");
    };
    client.command(&request).unwrap();
    loop {
        refresh(client, app).await;
        if screen(app).contains("deleted orders/workers") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // The broker's log has the deletion after the next refresh.
    refresh(client, app).await;
    app.key(Key::Char('5'));
    assert_shows(
        &screen(app),
        &["admin deleted consumer group orders/workers"],
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn console_shows_a_serving_broker() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let root = PathBuf::from(format!("/ozzy-simulation-{}", Uuid::now_v7()));
        let endpoint = format!("inproc://ozzy-ui-{}", Uuid::now_v7());
        let runtime = WriterRuntime::new().unwrap();
        let shape = Shape {
            mode: DeploymentMode::Single,
            partitions: 3,
        };
        let deployment = deployment_with_resources(&root, shape, &host(), |config| {
            for broker in config.brokers.values_mut() {
                broker.endpoints.admin = Some(endpoint.clone());
            }
        });
        let checked = deployment[0].0.clone();
        let brokers = Box::pin(start_brokers(&runtime, deployment, false)).await;
        brokers[0].ready().await.unwrap();
        let mut client = Client::connect(runtime.context(), &endpoint).await.unwrap();
        let mut app = App::new(&endpoint);
        refresh(&mut client, &mut app).await;
        // Records confirmed between two refreshes give rates.
        let mut writer = ozzy_sim::client::Client::open_with_runtime(&checked, &runtime).await;
        let pending = writer.queue(0).await;
        writer.confirm(pending).await;
        refresh(&mut client, &mut app).await;
        let text = screen(&mut app);
        assert_shows(&text, &["broker-0", "connected", "1 of", "Tab/1-5"]);
        assert!(!text.contains("Replication"), "{text}");
        assert_ne!(value(&text, "Commits/s"), "0", "{text}");
        assert_ne!(value(&text, "Appends confirmed"), "0", "{text}");
        assert_partitions(&mut client, &mut app).await;
        app.key(Key::Char('4'));
        assert_shows(
            &screen(&mut app),
            &["all threads", "ozzy/shard/0", "exited", "shard_turn"],
        );
        assert_groups(&writer, &mut client, &mut app).await;
        writer.close().await;
        client.close().await;
        for broker in brokers {
            broker.shutdown().await.unwrap();
        }
    })
    .await
    .unwrap();
}
