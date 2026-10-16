//! Terminal admin console of one broker's admin endpoint.

use clap::Parser;
use crossterm::{
    event::{
        DisableMouseCapture, EnableMouseCapture, Event, EventStream, KeyCode, KeyEvent,
        KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
    },
    execute,
};
use futures::StreamExt;
use ozzy_ui::{
    Action, App, Key, Mouse,
    client::{Client, Update},
};
use std::{path::PathBuf, process::ExitCode, time::Duration};
use tokio::time::MissedTickBehavior;

/// How soon to look for the broker again while its handshake is missing.
const CONNECT_POLL: Duration = Duration::from_millis(20);

#[derive(Debug, Parser)]
#[command(
    name = "ozzy-tui",
    about = "Live status and counters of one Ozzy broker"
)]
struct Args {
    /// Admin endpoint, for example `ipc://@ozzy-admin`.
    #[arg(required_unless_present = "config", conflicts_with = "config")]
    endpoint: Option<String>,
    /// Deployment config to read the admin endpoint from.
    #[arg(long, requires = "broker")]
    config: Option<PathBuf>,
    /// Broker name in `--config`.
    #[arg(long, requires = "config")]
    broker: Option<String>,
    /// Refresh interval in milliseconds.
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u64).range(100..))]
    interval_ms: u64,
}

impl Args {
    fn endpoint(&self) -> Result<String, String> {
        let (Some(path), Some(name)) = (&self.config, &self.broker) else {
            return self.endpoint.clone().ok_or_else(|| "no endpoint".into());
        };
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        let deployment = ozzy_config::Deployment::parse(&text)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        let broker = deployment
            .brokers
            .get(name)
            .ok_or_else(|| format!("{}: no broker {name}", path.display()))?;
        broker
            .endpoints
            .admin
            .clone()
            .ok_or_else(|| format!("{}: broker {name} has no admin endpoint", path.display()))
    }
}

fn main() -> ExitCode {
    let args = Args::parse();
    let interval = Duration::from_millis(args.interval_ms);
    let result = args.endpoint().and_then(|endpoint| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        let terminal = ratatui::try_init().map_err(|error| format!("terminal: {error}"))?;
        let result = capture_mouse()
            .map_err(|error| format!("terminal: {error}"))
            .and_then(|()| runtime.block_on(run(terminal, endpoint, interval)));
        let _ = execute!(std::io::stdout(), DisableMouseCapture);
        ratatui::restore();
        result
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ozzy-tui: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Draw, then wait for a key, a mouse event, a reply, or the next refresh.
async fn run(
    mut terminal: ratatui::DefaultTerminal,
    endpoint: String,
    interval: Duration,
) -> Result<(), String> {
    // TODO(admin-auth): load the admin's CURVE key once brokers require one.
    let context = omq_tokio::Context::current();
    let mut client = Client::connect(&context, &endpoint)
        .await
        .map_err(|error| format!("{endpoint}: {error}"))?;
    let mut app = App::new(endpoint);
    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let result = loop {
        if let Err(error) = terminal.draw(|frame| app.draw(frame)) {
            break Err(error.to_string());
        }
        let action = tokio::select! {
            _ = tick.tick() => Action::Refresh,
            update = client.update() => {
                match update {
                    Update::Reply(reply) => app.reply(reply),
                    Update::Failed(message) => app.failed(message),
                    Update::Command(reply) => app.command_reply(reply),
                    Update::CommandFailed(message) => app.command_failed(message),
                }
                Action::None
            }
            event = events.next() => match event {
                Some(Ok(Event::Key(event))) => key(event).map_or(Action::None, |key| app.key(key)),
                Some(Ok(Event::Mouse(event))) => mouse(event)
                    .map_or(Action::None, |mouse| app.mouse(mouse, event.column, event.row)),
                Some(Ok(_)) => Action::None,
                Some(Err(error)) => break Err(error.to_string()),
                None => Action::Quit,
            },
        };
        match action {
            Action::None => {}
            Action::Quit => break Ok(()),
            Action::Refresh => {
                let up = client.refresh(&app.requests()).await;
                app.link(up);
                if !up {
                    tick.reset_after(CONNECT_POLL);
                }
            }
            Action::Command(request) => {
                if let Err(message) = client.command(&request) {
                    app.command_failed(message);
                }
            }
        }
    };
    client.close().await;
    result
}

/// Report mouse events. A panic turns reporting off before ratatui's hook
/// restores the screen.
fn capture_mouse() -> std::io::Result<()> {
    let restore = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = execute!(std::io::stdout(), DisableMouseCapture);
        restore(info);
    }));
    execute!(std::io::stdout(), EnableMouseCapture)
}

/// The console mouse event of a terminal one: left clicks and the wheel.
fn mouse(event: MouseEvent) -> Option<Mouse> {
    Some(match event.kind {
        MouseEventKind::Down(MouseButton::Left) => Mouse::Click,
        MouseEventKind::ScrollUp => Mouse::ScrollUp,
        MouseEventKind::ScrollDown => Mouse::ScrollDown,
        _ => return None,
    })
}

/// The console key of a terminal key press. Ctrl+C quits like `q`.
fn key(event: KeyEvent) -> Option<Key> {
    if event.kind == KeyEventKind::Release {
        return None;
    }
    if event.modifiers.contains(KeyModifiers::CONTROL) {
        return (event.code == KeyCode::Char('c')).then_some(Key::Char('q'));
    }
    Some(match event.code {
        KeyCode::Char(char) => Key::Char(char),
        KeyCode::Up => Key::Up,
        KeyCode::Down => Key::Down,
        KeyCode::Left => Key::Left,
        KeyCode::Right => Key::Right,
        KeyCode::PageUp => Key::PageUp,
        KeyCode::PageDown => Key::PageDown,
        KeyCode::Home => Key::Home,
        KeyCode::End => Key::End,
        KeyCode::Tab => Key::Tab,
        KeyCode::BackTab => Key::BackTab,
        KeyCode::Esc => Key::Esc,
        _ => return None,
    })
}
