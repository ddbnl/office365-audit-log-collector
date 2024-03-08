use std::collections::HashMap;
use std::fs::{File, read};
use std::io::{BufReader, Read};
use chrono::DateTime;
use clap::Parser;
use crate::collector::Collector;
use crate::config::{Config, ContentTypesSubConfig};

mod collector;
mod api_connection;
mod data_structures;
mod config;
mod interface;
mod interfaces;


fn main() {

    let args = data_structures::CliArgs::parse();

    // Read config
    let open_file = File::open(args.config.clone())
        .unwrap_or_else(|e| panic!("Config path could not be opened: {}", e.to_string()));
    let reader = BufReader::new(open_file);
    let config: Config = serde_yaml::from_reader(reader)
        .unwrap_or_else(|e| panic!("Config could not be parsed: {}", e.to_string()));
    let runs = config.get_needed_runs();

    let mut collector = Collector::new(args, config, runs);

    collector.run_once();
}
