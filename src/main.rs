use clap::Parser;
use crate::collector::Collector;
use crate::config::Config;
use log::LevelFilter;

mod collector;
mod api_connection;
mod data_structures;
mod config;
mod interfaces;


#[tokio::main]
async fn main() {

    let args = data_structures::CliArgs::parse();
    let config = Config::new(args.config.clone());
    init_logging(&config);

    let runs = config.get_needed_runs();
    let mut collector = Collector::new(args, config, runs).await;
    collector.monitor().await;
}

fn init_logging(config: &Config) {

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
