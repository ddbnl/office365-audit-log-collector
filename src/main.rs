use std::sync::Arc;
use clap::Parser;
use crate::collector::Collector;
use crate::config::Config;
use log::{error, Level, LevelFilter, Log, Metadata, Record};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::Mutex;
use crate::data_structures::RunState;
use crate::interactive_mode::interactive;

mod collector;
mod api_connection;
mod data_structures;
mod config;
mod interfaces;
mod interactive_mode;


#[tokio::main]
async fn main() {

    let args = data_structures::CliArgs::parse();
    let config = Config::new(args.config.clone());
    let (log_tx, log_rx) = unbounded_channel();

    if args.interactive {
        init_interactive_logging(&config, log_tx);
        interactive::run(args, config, log_rx).await.unwrap();
    } else {
        init_non_interactive_logging(&config);
        let state = RunState::default();
        let wrapped_state = Arc::new(Mutex::new(state));
        let runs = config.get_needed_runs();
        match Collector::new(args, config, runs, wrapped_state.clone(), None).await {
            Ok(mut collector) => collector.monitor().await,
            Err(e) => {
                error!("Could not start collector: {}", e);
                panic!("Could not start collector: {}", e);
            }
        }
    }
}

fn init_non_interactive_logging(config: &Config) {

    let (path, level) = if let Some(log_config) = &config.log {
        let level = if log_config.debug { LevelFilter::Debug } else { LevelFilter::Info };
        (log_config.path.clone(), level)
    } else {
        ("".to_string(), LevelFilter::Info)
    };

    if !path.is_empty() {
        simple_logging::log_to_file(path, level).unwrap();
    } else {
        simple_logging::log_to_stderr(level);
    }
}

fn init_interactive_logging(config: &Config, log_tx: UnboundedSender<(String, Level)>) {

    let level = if let Some(log_config) = &config.log {
        if log_config.debug { LevelFilter::Debug } else { LevelFilter::Info }
    } else {
        LevelFilter::Info
    };
    log::set_max_level(level);
    log::set_boxed_logger(InteractiveLogger::new(log_tx)).unwrap();
}


pub struct  InteractiveLogger {
    log_tx: UnboundedSender<(String, Level)>,

}
impl InteractiveLogger {
    pub fn new(log_tx: UnboundedSender<(String, Level)>) -> Box<Self> {
        Box::new(InteractiveLogger { log_tx })
    }
}
impl Log for InteractiveLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info 
    }
    fn log(&self, record: &Record) {

        let date = chrono::Utc::now().to_string();
        let msg = format!("[{}] {}:{} -- {}",
                 date,
                 record.level(),
                 record.target(),
                 record.args());
        self.log_tx.send((msg, record.level())).unwrap()
    }
    fn flush(&self) {}
}

