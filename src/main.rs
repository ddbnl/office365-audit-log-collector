use clap::Parser;
use crate::collector::Collector;
use crate::config::Config;

mod collector;
mod api_connection;
mod data_structures;
mod config;
mod interfaces;


fn main() {

    let args = data_structures::CliArgs::parse();
    let config = Config::new(args.config.clone());
    let runs = config.get_needed_runs();
    let mut collector = Collector::new(args, config, runs);
    collector.monitor();
}
