use std::cmp::max;
use std::sync::Arc;
use color_eyre::eyre::Result;
use crossterm::event::KeyCode::Char;
use crossterm::event::KeyCode;
use log::{error, Level, warn};
use ratatui::{Frame, widgets::*};
use ratatui::layout::{Constraint, Direction, Layout, Size};
use ratatui::prelude::*;
use ratatui::style::Color;
use ratatui::widgets::ListItem;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use anyhow::Result as AnyHowResult;
use ratatui::style::palette::tailwind;
use reqwest::header::HeaderMap;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tui_scrollview::{ScrollView, ScrollViewState};
use crate::api_connection::ApiConnection;
use crate::collector::Collector;
use crate::config::Config;
use crate::data_structures::{CliArgs, RunState};
use crate::interactive_mode::tui;
use crate::interactive_mode::tui::Action;


#[derive(Eq, PartialEq, Copy, Clone)]
enum SelectedBlock {
    Commands,
    Subscriptions,
    Logs,
    Results,
}


#[derive(Clone)]
struct State {
    args: CliArgs,
    config: Config,
    logs: Vec<(String, Level)>,
    results: Vec<Vec<String>>,
    action_tx: UnboundedSender<Action>,
    interface_tx: UnboundedSender<Vec<String>>,
    should_quit: bool,
    api_connected: bool,
    general: bool,
    exchange: bool,
    sharepoint: bool,
    aad: bool,
    dlp: bool,
    selected_block: SelectedBlock,
    selected_list: usize,
    selected_list_max: usize,
    scroll_log: ScrollViewState,
    table_result: TableState,
    table_result_colum_start: usize,
    found_blobs: usize,
    successful_blobs: usize,
    awaiting_blobs: usize,
    error_blobs: usize,
    retry_blobs: usize,
    logs_retrieved: usize,
    logs_retrieval_speeds: Vec<(f64, f64)>,
    run_started: Option<Instant>,
    run_ended: Option<Instant>,
    run_progress: u16,
    rate_limit: bool,
}
impl State {
    pub fn new(args: CliArgs,
               config: Config,
               action_tx: UnboundedSender<Action>,
               interface_tx: UnboundedSender<Vec<String>>
    ) -> Self {
        Self {
            args,
            config,
            action_tx,
            interface_tx,
            logs: Vec::new(),
            results: Vec::new(),
            should_quit: false,
            api_connected: false,
            general: false,
            exchange: false,
            sharepoint: false,
            aad: false,
            dlp: false,
            selected_block: SelectedBlock::Commands,
            selected_list: 0,
            selected_list_max: 2,
            scroll_log: ScrollViewState::default(),
            table_result: TableState::default(),
            table_result_colum_start: 0,
            found_blobs: 0,
            error_blobs: 0,
            successful_blobs: 0,
            awaiting_blobs: 0,
            retry_blobs: 0,
            logs_retrieved: 0,
            logs_retrieval_speeds: Vec::new(),
            run_started: None,
            run_ended: None,
            run_progress: 0,
            rate_limit: false,
        }
    }
}

pub async fn run(args: CliArgs, config: Config, mut log_rx: UnboundedReceiver<(String, Level)>) -> Result<()> {
    let (action_tx, mut action_rx) = unbounded_channel();
    let (interface_tx, mut interface_rx) = unbounded_channel();
    let mut tui = tui::Tui::new()?.tick_rate(1.0).frame_rate(30.0);
    tui.enter()?;

    let mut state = State::new(args, config, action_tx.clone(), interface_tx);
    let api = Arc::new(Mutex::new(
        ApiConnection { args: state.args.clone(), config: state.config.clone(), headers: HeaderMap::new() }));

    loop {
        let e = tui.next().await.unwrap();
        match e {
            tui::Event::Tick => action_tx.send(Action::Tick)?,
            tui::Event::Render => action_tx.send(Action::Render)?,
            tui::Event::Key(_) => {
                let action = get_action(&state, e);
                action_tx.send(action.clone())?;
            }
            _ => {}
        };

        while let Ok(log) = log_rx.try_recv() {
            if state.logs.len() == 1000 {
                state.logs.remove(0);
            }
            state.logs.push(log);
        }
        while let Ok(result) = interface_rx.try_recv() {
            if state.results.len() == 1000 {
                state.results.remove(0);
            }
            state.results.push(result);
        }
        while let Ok(action) = action_rx.try_recv() {
            // application update
            update(&mut state, action.clone(), api.clone());
            // render only when we receive Action::Render
            if let Action::Render = action {
                tui.draw(|f| {
                    ui(f, &mut state);
                })?;
            }
        }

        // application exit
        if state.should_quit {
            break;
        }
    }
    tui.exit()?;

    Ok(())
}

fn get_action(_state: &State, event: tui::Event) -> Action {
    match event {
        tui::Event::Error => Action::None,
        tui::Event::Tick => Action::Tick,
        tui::Event::Render => Action::Render,
        tui::Event::Key(key) => {
            match key.code {
                Char('q') => Action::Quit,
                Char('c') => Action::GoToCommand,
                Char('l') => Action::GoToLogs,
                Char('s') => Action::GoToSubscriptions,
                Char('r') => Action::GoToResults,
                KeyCode::Enter => Action::HandleEnter,
                KeyCode::Up => Action::HandleUp,
                KeyCode::Down => Action::HandleDown,
                KeyCode::Left => Action::HandleLeft,
                KeyCode::Right => Action::HandleRight,
                KeyCode::PageUp => Action::ScrollPageUp,
                KeyCode::PageDown => Action::ScrollPageDown,
                _ => Action::None,
            }
        },
        _ => Action::None,
    }
}

fn update(state: &mut State, action: Action, api: Arc<Mutex<ApiConnection>>) {

    match action {
        Action::Quit => {
            state.should_quit = true;
        }
        Action::GoToCommand => {
            state.selected_block = SelectedBlock::Commands;
            state.selected_list = 0;
            state.selected_list_max = 2;
        }
        Action::GoToSubscriptions => {
            state.selected_block = SelectedBlock::Subscriptions;
            state.selected_list = 0;
            state.selected_list_max = 4;
        }
        Action::GoToLogs => {
            state.selected_block = SelectedBlock::Logs;
        }
        Action::GoToResults => {
            state.selected_block = SelectedBlock::Results;
        }
        Action::HandleEnter => {
            let new_state = state.clone();
            tokio::spawn(async move {
                handle_enter(new_state, api).await;
            });
        }
        Action::HandleUp => {
            if state.selected_block == SelectedBlock::Logs {
                state.scroll_log.scroll_up()
            } else if state.selected_block == SelectedBlock::Results{
                if let Some(index) = state.table_result.selected() {
                   if index > 0 {
                       state.table_result.select(Some(index - 1));
                   }
                }
            } else {
                state.selected_list = state.selected_list.saturating_sub(1);
            }
        }
        Action::HandleDown => {
            if state.selected_block == SelectedBlock::Logs {
                state.scroll_log.scroll_down()
            } else if state.selected_block == SelectedBlock::Results{
                if let Some(index) = state.table_result.selected() {
                    if index < 1000 {
                        state.table_result.select(Some(index + 1));
                    }
                } else {
                    state.table_result.select(Some(0))
                }
            } else {
                state.selected_list = if state.selected_list >= state.selected_list_max {
                    state.selected_list
                } else {
                    state.selected_list + 1
                }
            }
        },
        Action::HandleLeft => {
            if state.selected_block == SelectedBlock::Commands && state.selected_list == 2 {
                let mut current = state.config.collect.duplicate.unwrap_or(1);
                current = current.saturating_sub(max(1, current / 10));
                current = max(1, current);
                state.config.collect.duplicate = Some(current);
            } else if state.selected_block == SelectedBlock::Results {
                if state.table_result_colum_start > 0 {
                    state.table_result_colum_start -= 1;
                }
            }
        },
        Action::HandleRight => {
            if state.selected_block == SelectedBlock::Commands && state.selected_list == 2 {
                let current = state.config.collect.duplicate.unwrap_or(1);
                let increase = max(1, current / 10);
                state.config.collect.duplicate = Some(current + increase);
            } else if state.selected_block == SelectedBlock::Results {
                let mut max_col = state.results.first().unwrap_or(&Vec::new()).len();
                if max_col > 10 {
                    max_col -= 10;
                }
                if state.table_result_colum_start < max_col {
                    state.table_result_colum_start += 1;
                }
            }
        },
        Action::ScrollPageUp => {
            if state.selected_block == SelectedBlock::Logs {
                state.scroll_log.scroll_page_up()
            }
        }
        Action::ScrollPageDown => {
            if state.selected_block == SelectedBlock::Logs {
                state.scroll_log.scroll_page_down()
            }
        }
        Action::UpdateFoundBlobs(found) => {
            state.found_blobs = found;
        }
        Action::UpdateSuccessfulBlobs(found) => {
            state.successful_blobs = found;
        }
        Action::UpdateRetryBlobs(found) => {
            state.retry_blobs = found;
        }
        Action::UpdateErrorBlobs(found) => {
            state.error_blobs = found;
        }
        Action::UpdateAwaitingBlobs(found) => {
            state.awaiting_blobs = found;
        }
        Action::LogsRetrieved(found) => {
            state.logs_retrieved = found;
        }
        Action::LogsRetrievedSpeed(found) => {
            state.logs_retrieval_speeds.push(found);
        }
        Action::RunProgress(found) => {
            state.run_progress = found;
        }
        Action::RunStarted => {
            state.run_started = Some(Instant::now());
            state.logs_retrieval_speeds.clear();
        }
        Action::RunEnded => {
            state.run_ended = Some(Instant::now());
        }
        Action::RateLimited => {
            state.rate_limit = true;
        }
        Action::NotRateLimited => {
            state.rate_limit = false;
        }
        Action::ConnectApi => {
            state.api_connected = true;
        },
        Action::DisconnectApi => {
            state.api_connected = false;
        },
        Action::EnableSubscriptionGeneral => {
            state.general = true;
        },
        Action::DisableSubscriptionGeneral => {
            state.general = false;
        },
        Action::EnableSubscriptionAad => {
            state.aad = true;
        },
        Action::DisableSubscriptionAad => {
            state.aad = false;
        },
        Action::EnableSubscriptionExchange => {
            state.exchange = true;
        },
        Action::DisableSubscriptionExchange => {
            state.exchange = false;
        },
        Action::EnableSubscriptionSharePoint => {
            state.sharepoint = true;
        },
        Action::DisableSubscriptionSharePoint => {
            state.sharepoint = false;
        },
        Action::EnableSubscriptionDlp => {
            state.dlp = true;
        },
        Action::DisableSubscriptionDlp => {
            state.dlp = false;
        },
        _ => (),  // TODO
    }
}

fn ui(frame: &mut Frame, state: &mut State) {

    // Layouts
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(10),
            Constraint::Length(7),
            Constraint::Length(1),
            Constraint::Length(10),
            Constraint::Length(1),
            Constraint::Length(10),
        ])
        .split(frame.size());

    let horizontal_0 = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(60),
            Constraint::Length(60),
            Constraint::Min(1),
        ])
        .split(vertical[0]);

    let horizontal_1 = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(60),
            Constraint::Length(60),
            Constraint::Min(1),
        ])
        .split(vertical[1]);

    let horizontal_2 = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(frame.size().width),
            Constraint::Min(1),
        ])
        .split(vertical[2]);

    let horizontal_3 = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(frame.size().width),
            Constraint::Min(1),
        ])
        .split(vertical[3]);

    let horizontal_4 = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(frame.size().width),
            Constraint::Min(1),
        ])
        .split(vertical[4]);

    let horizontal_5 = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(frame.size().width),
            Constraint::Min(1),
        ])
        .split(vertical[5]);

    // Connection
    let settings_block = Block::default()
        .title(block::Title::from("Connection").alignment(Alignment::Center))
        .borders(Borders::ALL);

    let mut settings_list_items = Vec::<ListItem>::new();

    settings_list_items.push(ListItem::new(Line::from(Span::styled(
        format!("  Tenant ID: {}", state.args.tenant_id), Style::default().fg(
            if state.args.tenant_id.is_empty() { Color::Red } else { Color::Green }),
    ))));
    settings_list_items.push(ListItem::new(Line::from(Span::styled(
        format!("  Client ID: {}", state.args.client_id), Style::default().fg(
            if state.args.client_id.is_empty() { Color::Red } else { Color::Green }),
    ))));

    let secret_string = if state.args.secret_key.is_empty() {
        "Secret Key:".to_string()
    } else {
        format!("  Secret Key: {}{}",
                state.args.secret_key.clone().split_off(state.args.secret_key.len() - 5),
                "*".repeat(state.args.secret_key.len() - 5))
    };
    settings_list_items.push(ListItem::new(Line::from(Span::styled(
                secret_string, Style::default().fg(
            if state.args.secret_key.is_empty() { Color::Red } else { Color::Green }),
    ))));
    settings_list_items.push(ListItem::new(Line::from(Span::styled(
        format!("  Config: {}", state.args.config), Style::default().fg(
            if state.args.config.is_empty() { Color::Red } else { Color::Green }),
    ))));

    let settings_list = List::new(settings_list_items)
        .block(settings_block.clone());
    frame.render_widget(settings_list, horizontal_0[0]);

    // Commands
    let command_block_style = match state.selected_block {
        SelectedBlock::Commands => Style::new().underlined(),
        _ => Style::new(),
    };
    let commands_block = Block::new()
        .title_style(command_block_style)
        .title("<C> Commands")
        .title_alignment(Alignment::Center)
        .borders(Borders::ALL);

    let mut commands_list_items = Vec::<ListItem>::new();

    commands_list_items.push(ListItem::new(Line::from(Span::styled(
        "Test API connection", Style::default().fg(Color::Magenta),
    ))));
    commands_list_items.push(ListItem::new(Line::from(Span::styled(
        "Run Collector (using specified config)", Style::default().fg(Color::Magenta),
    ))));
    let duplicate = state.config.collect.duplicate.unwrap_or(1);
    commands_list_items.push(ListItem::new(Line::from(Span::styled(
        format!("< Load test ({}x) > (use arrow keys to increase load)", duplicate), Style::default().fg(Color::Magenta),
    ))));

    let mut command_state = ListState::default().with_selected(Some(state.selected_list));
    if state.selected_block == SelectedBlock::Commands {
        StatefulWidget::render(
            List::new(commands_list_items)
                .style(Style::new())
                .highlight_style(Style::new().on_yellow())
                .highlight_symbol(">>")
                .block(commands_block),
            horizontal_0[1],
            frame.buffer_mut(),
            &mut command_state,
        );
    } else {
        StatefulWidget::render(
            List::new(commands_list_items)
                .style(Style::new())
                .highlight_symbol("  ")
                .block(commands_block),
            horizontal_0[1],
            frame.buffer_mut(),
            &mut command_state,
        );
    }

    // Speed chart
    let chart_block = Block::default()
        .title(block::Title::from("Performance").alignment(Alignment::Center))
        .borders(Borders::ALL);
    let datasets = vec![
        Dataset::default()
            .name("Logs per second")
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().magenta())
            .data(state.logs_retrieval_speeds.as_slice()),
    ];

    let x_axis = Axis::default()
        .style(Style::default().white())
        .bounds([0.0, state.logs_retrieval_speeds.last().unwrap_or(&(10.0, 0.0)).0])
        .labels(vec![]);

    let top_speed = state.logs_retrieval_speeds
        .iter()
        .map(|(_, s)| *s as usize)
        .max()
        .unwrap_or(15);
    let y_labels = vec!(
        Span::from((top_speed / 3).to_string()),
        Span::from(((top_speed / 3) * 2).to_string()),
        Span::from(top_speed.to_string())
    );
    let y_axis = Axis::default()
        .title("Logs per second".red())
        .style(Style::default().white())
        .bounds([0.0, top_speed as f64])
        .labels(y_labels);

    let chart = Chart::new(datasets)
        .block(chart_block)
        .x_axis(x_axis)
        .y_axis(y_axis);

    frame.render_widget(chart, horizontal_0[2]);

    // Subscriptions
    let subscription_block_style = match state.selected_block {
        SelectedBlock::Subscriptions => Style::new().underlined(),
        _ => Style::new(),
    };
    let subscription_block = Block::new()
        .title("<S> Feed subscriptions")
        .title_alignment(Alignment::Center)
        .title_style(subscription_block_style)
        .borders(Borders::ALL);

    let mut subscription_list_items = Vec::<ListItem>::new();
    subscription_list_items.push(ListItem::new(Line::from(Span::styled(
        format!("Audit.General active: {}",
                if state.api_connected {state.general.to_string()} else { "Not connected".to_string() }),
        Style::default().fg(color_from_bool(state.general)),
    ))));
    subscription_list_items.push(ListItem::new(Line::from(Span::styled(
        format!("Audit.AzureActiveDirectory active: {}",
                if state.api_connected {state.aad.to_string()} else { "Not connected".to_string() }),
        Style::default().fg(color_from_bool(state.aad)),
    ))));
    subscription_list_items.push(ListItem::new(Line::from(Span::styled(
        format!("Audit.Exchange active: {}",
                if state.api_connected {state.exchange.to_string()} else { "Not connected".to_string() }),
        Style::default().fg(color_from_bool(state.exchange)),
    ))));
    subscription_list_items.push(ListItem::new(Line::from(Span::styled(
        format!("Audit.Sharepoint active: {}",
                if state.api_connected {state.sharepoint.to_string()} else { "Not connected".to_string() }),
        Style::default().fg(color_from_bool(state.sharepoint)),
    ))));
    subscription_list_items.push(ListItem::new(Line::from(Span::styled(
        format!("DLP.All active: {}",
                if state.api_connected {state.dlp.to_string()} else { "Not connected".to_string() }),
        Style::default().fg(color_from_bool(state.dlp)),
    ))));
    let mut list_state = ListState::default().with_selected(Some(state.selected_list));
    if state.selected_block == SelectedBlock::Subscriptions {
        StatefulWidget::render(
            List::new(subscription_list_items)
                .style(Style::new())
                .highlight_style(Style::new().on_yellow())
                .highlight_symbol(">>")
                .block(subscription_block),
            horizontal_1[0],
            frame.buffer_mut(),
            &mut list_state,
        );
    } else {
        StatefulWidget::render(
            List::new(subscription_list_items)
                .style(Style::new())
                .highlight_symbol("  ")
                .block(subscription_block),
            horizontal_1[0],
            frame.buffer_mut(),
            &mut list_state,
        );
    }

    // Status
    let status_block = Block::new()
        .title("Blobs")
        .title_alignment(Alignment::Center)
        .title_style(Style::new())
        .borders(Borders::ALL);

    let highest = *[state.found_blobs, state.successful_blobs, state.retry_blobs, state.error_blobs]
        .iter()
        .max()
        .unwrap();
    let bar = BarChart::default()
        .block(Block::default().title("Run stats").borders(Borders::ALL))
        .bar_width(10)
        .data(BarGroup::default().bars(&[Bar::default().value(state.found_blobs as u64).style(Style::default().fg(Color::Blue)).label(Line::from("Found"))]))
        .data(BarGroup::default().bars(&[Bar::default().value(state.successful_blobs as u64).style(Style::default().fg(Color::Green)).label(Line::from("Retrieved"))]))
        .data(BarGroup::default().bars(&[Bar::default().value(state.retry_blobs as u64).style(Style::default().fg(Color::Yellow)).label(Line::from("Retried"))]))
        .data(BarGroup::default().bars(&[Bar::default().value(state.error_blobs as u64).style(Style::default().fg(Color::Red)).label(Line::from("Error"))]))
        .max(max(highest as u64, 10))
        .block(status_block);

    frame.render_widget(bar, horizontal_1[1]);

    // Progress
    let progress_block = Block::new()
        .title("Progress")
        .title_alignment(Alignment::Center)
        .title_style(Style::new())
        .borders(Borders::ALL);
    let mut progress_list_items = Vec::<ListItem>::new();

    let (connect_string, color) = if state.api_connected {
        ("  API Connection: Connected".to_string(), Color::Green,)
    } else {
        ("  API Connection: Disconnected".to_string(), Color::Red,)
    };
    progress_list_items.push(ListItem::new(Line::from(Span::styled(
        connect_string, Style::default().fg(color),
    ))));

    if state.rate_limit {
        progress_list_items.push(ListItem::new(Line::from(Span::styled(
            "  Being rate limited!", Style::default().fg(Color::Red).rapid_blink(),
        ))));
    } else {
        progress_list_items.push(ListItem::new(Line::from(Span::styled(
            "  Not rate limited", Style::default().fg(Color::Green),
        ))));
    }

    let elapsed = if let Some(elapsed) = state.run_started {
        
        let end = state.run_ended.unwrap_or(Instant::now());
        let total = end.duration_since(elapsed).as_secs();
        let minutes = total / 60;
        let seconds = total % 60;
        format!("{}{}:{}{}",
            if minutes < 10 { "0" } else { "" },
            minutes,
            if seconds < 10 { "0" } else { "" },
            seconds,
        )
    } else {
        "  Not started".to_string()
    };
    progress_list_items.push(ListItem::new(Line::from(Span::styled(
        format!("  Time elapsed: {}", elapsed), Style::default().fg(Color::LightBlue),
    ))));

    progress_list_items.push(ListItem::new(Line::from(Span::styled(
        format!("  Blobs remaining: {}", state.awaiting_blobs), Style::default().fg(Color::LightBlue),
    ))));

    let progress_list = List::new(progress_list_items)
        .style(Style::new())
        .highlight_symbol("  ")
        .block(progress_block);
    frame.render_widget(progress_list, horizontal_1[2]);

    // Logs
    let mut logs_list_items = Vec::<ListItem>::new();
    for (log, level) in state.logs.iter() {
        logs_list_items.push(ListItem::new(Line::from(Span::styled(
            log, Style::default().fg(color_from_level(level)),
        ))));
    }
    let list_wid = List::new(logs_list_items)
        .style(Style::new())
        .highlight_symbol("  ");
    let size = Size::new(1000, 1000);
    let mut scroll_view = ScrollView::new(size);
    let area = Rect::new(0, 0, 1000 , 1000);
    scroll_view.render_widget(list_wid, area);

    let palette = tailwind::SLATE;
    let (fg, bg) = if state.selected_block == SelectedBlock::Logs {
        (palette.c900, Color::Yellow)
    } else {
        (palette.c900, palette.c300)
    };
    let keys_fg = palette.c50;
    let keys_bg = palette.c600;
    let title = Line::from(vec![
        "<L> Logs  ".into(),
        "| ↓ | ↑ | PageDown | PageUp |  "
            .fg(keys_fg)
            .bg(keys_bg),
    ])
        .style((fg, bg)).centered();
    frame.render_widget(title, horizontal_2[0]);
    frame.render_stateful_widget(scroll_view, horizontal_3[0], &mut state.scroll_log);

    // Results
    let mut results = state.results.clone();
    let mut header = if !results.is_empty() { results.remove(0) } else { Vec::new() };
    if header.len() > 10 {
        header = header[state.table_result_colum_start..state.table_result_colum_start + 10].to_vec();
    }
    let rows: Vec<Row> = results
        .clone()
        .into_iter()
        .map(|mut x|{
            x = x[state.table_result_colum_start..state.table_result_colum_start + 10].to_vec();
            Row::new(x)
        })
        .collect();
    let table = Table::default()
        .rows(rows)
        .highlight_style(Style::new().add_modifier(Modifier::REVERSED))
        .highlight_symbol(">>")
        .header(Row::new(header)
            .style(Style::new().bold().underlined())
            .bottom_margin(1),
        );
    

    let palette = tailwind::SLATE;
    let (fg, bg) = if state.selected_block == SelectedBlock::Results {
        (palette.c900, Color::Yellow)
    } else {
        (palette.c900, palette.c300)
    };
    let keys_fg = palette.c50;
    let keys_bg = palette.c600;
    let title = Line::from(vec![
        "<R> Results  ".into(),
        "  ↓ | ↑ | ← | → | "
            .fg(keys_fg)
            .bg(keys_bg),
    ])
        .style((fg, bg)).centered();
    frame.render_widget(title, horizontal_4[0]);
    frame.render_stateful_widget(table, horizontal_5[0], &mut state.table_result);

}

fn color_from_bool(val: bool) -> Color {
    return if val {
        Color::Green
    } else {
        Color::Red
    }
}
fn color_from_level(level: &Level) -> Color {
    match level {
        &Level::Trace => Color::Magenta,
        &Level::Debug => Color::White,
        &Level::Info => Color::LightBlue,
        &Level::Warn => Color::Yellow,
        &Level::Error => Color::Red,
    }
}
async fn handle_enter(state: State, api: Arc<Mutex<ApiConnection>>) {
    if let Err(e) = match state.selected_block {
        SelectedBlock::Commands => handle_enter_command(state, api).await,
        SelectedBlock::Subscriptions => handle_enter_subscription(state, api).await,
        _ => Ok(()),
    } {
        error!("Error connecting to API: {}", e);
    };
}

async fn handle_enter_command(state: State, api: Arc<Mutex<ApiConnection>>) -> AnyHowResult<()>{
    match state.selected_list {
        0 => handle_enter_command_connect(state, api).await?,
        1 => handle_enter_command_run(state, false, api).await?,
        2 => handle_enter_command_run(state, true, api).await?,
        _ => warn!("Invalid list choice"),
    }
    Ok(())
}

async fn handle_enter_command_connect(state: State, api: Arc<Mutex<ApiConnection>>) -> AnyHowResult<()> {

    state.action_tx.send(Action::DisconnectApi).unwrap();
    if api.lock().await.headers.is_empty() {
        api.lock().await.login().await?;
    }
    state.action_tx.send(Action::ConnectApi).unwrap();
    update_subscriptions(state, api).await?;
    Ok(())
}

async fn handle_enter_command_run(state: State,
                                  load_test: bool,
                                  api: Arc<Mutex<ApiConnection>>)
    -> AnyHowResult<()> {

    let args = state.args.clone();
    let mut config = state.config.clone();
    if !load_test {
        config.collect.duplicate = Some(1);
    } else {
        config.collect.skip_known_logs = Some(false);
    }
    let runs = config.get_needed_runs();
    let run_state = Arc::new(Mutex::new(RunState::default()));

    handle_enter_command_connect(state.clone(), api).await?;
    let mut collector = Collector::new(args,
                                       config,
                                       runs,
                                       run_state.clone(),
                                       Some(state.interface_tx.clone())).await?;
    state.action_tx.send(Action::RunStarted).unwrap();
    let mut elapsed_since_data_point = Instant::now();
    let run_start = elapsed_since_data_point.clone();
    let mut logs_retrieved: usize = 0;
    let mut rate_limited = false;
    loop {
        let stats = run_state.lock().await.stats;
        state.action_tx.send(Action::UpdateAwaitingBlobs(run_state.lock().await.awaiting_content_blobs)).unwrap();
        state.action_tx.send(Action::UpdateFoundBlobs(stats.blobs_found)).unwrap();
        state.action_tx.send(Action::UpdateFoundBlobs(stats.blobs_found)).unwrap();
        state.action_tx.send(Action::UpdateSuccessfulBlobs(stats.blobs_successful)).unwrap();
        state.action_tx.send(Action::UpdateErrorBlobs(stats.blobs_error)).unwrap();
        state.action_tx.send(Action::UpdateRetryBlobs(stats.blobs_retried)).unwrap();

        if !rate_limited && run_state.lock().await.rate_limited {
            rate_limited = true;
            state.action_tx.send(Action::RateLimited).unwrap();
        } else if rate_limited && !run_state.lock().await.rate_limited {
            rate_limited = false;
            state.action_tx.send(Action::NotRateLimited).unwrap();
        }

        let progress = if stats.blobs_found > 0 {
            ((stats.blobs_found - stats.blobs_successful) / stats.blobs_found) * 100
        } else {
            0
        };
        state.action_tx.send(Action::RunProgress(progress as u16)).unwrap();

        logs_retrieved += collector.check_results().await;
        let done = collector.check_stats().await;
        if done {
            logs_retrieved += collector.check_all_results().await;
            state.action_tx.send(Action::RunEnded).unwrap();
            state.action_tx.send(Action::RunProgress(100)).unwrap();
            state.action_tx.send(Action::LogsRetrieved(logs_retrieved)).unwrap();
            state.action_tx.send(Action::UpdateFoundBlobs(stats.blobs_found)).unwrap();
            state.action_tx.send(Action::UpdateSuccessfulBlobs(stats.blobs_successful)).unwrap();
            state.action_tx.send(Action::UpdateErrorBlobs(stats.blobs_error)).unwrap();
            state.action_tx.send(Action::UpdateRetryBlobs(stats.blobs_retried)).unwrap();
            break
        }
        state.action_tx.send(Action::LogsRetrieved(logs_retrieved)).unwrap();
        let since_last_data_point = elapsed_since_data_point.elapsed().as_secs();
        if since_last_data_point >= 1 {
            let t = run_start.elapsed().as_secs() as f64;
            let speed = logs_retrieved as f64 / t;
            for _ in 0..since_last_data_point {
                state.action_tx.send(Action::LogsRetrievedSpeed((t, speed))).unwrap();
            }
            elapsed_since_data_point = Instant::now();
        }
    }
    Ok(())
}

async fn handle_enter_subscription(state: State, api: Arc<Mutex<ApiConnection>>) -> AnyHowResult<()> {

    if api.lock().await.headers.is_empty() {
        api.lock().await.login().await?;
    }
    match state.selected_list {
        0 => api.lock().await.set_subscription("Audit.General".to_string(), !state.general).await,
        1 => api.lock().await.set_subscription("Audit.AzureActiveDirectory".to_string(), !state.aad).await,
        2 => api.lock().await.set_subscription("Audit.Exchange".to_string(), !state.exchange).await,
        3 => api.lock().await.set_subscription("Audit.SharePoint".to_string(), !state.sharepoint).await,
        4 => api.lock().await.set_subscription("DLP.All".to_string(), !state.dlp).await,
        _ => panic!(),
    }?;
    update_subscriptions(state, api).await?;
    Ok(())
}

async fn update_subscriptions(state: State, api: Arc<Mutex<ApiConnection>>) -> AnyHowResult<()> {
    let subscriptions = api.lock().await.get_feeds().await?;
    if subscriptions.contains(&"Audit.General".to_string()) {
        state.action_tx.send(Action::EnableSubscriptionGeneral).unwrap()
    } else {
        state.action_tx.send(Action::DisableSubscriptionGeneral).unwrap()
    }
    if subscriptions.contains(&"Audit.AzureActiveDirectory".to_string()) {
        state.action_tx.send(Action::EnableSubscriptionAad).unwrap()
    } else {
        state.action_tx.send(Action::DisableSubscriptionAad).unwrap()
    }
    if subscriptions.contains(&"Audit.Exchange".to_string()) {
        state.action_tx.send(Action::EnableSubscriptionExchange).unwrap()
    } else {
        state.action_tx.send(Action::DisableSubscriptionExchange).unwrap()
    }
    if subscriptions.contains(&"Audit.SharePoint".to_string()) {
        state.action_tx.send(Action::EnableSubscriptionSharePoint).unwrap()
    } else {
        state.action_tx.send(Action::DisableSubscriptionSharePoint).unwrap()
    }
    if subscriptions.contains(&"DLP.All".to_string()) {
        state.action_tx.send(Action::EnableSubscriptionDlp).unwrap()
    } else {
        state.action_tx.send(Action::DisableSubscriptionDlp).unwrap()
    }
    Ok(())
}
