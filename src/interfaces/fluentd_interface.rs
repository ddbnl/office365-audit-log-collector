use std::collections::HashMap;
use std::path::Path;
use std::time::SystemTime;
use chrono::{DateTime, Duration, FixedOffset, NaiveDateTime, NaiveTime, Utc};
use core::time;
use csv::Writer;
use futures::future::Lazy;
use poston::{Client, Settings, WorkerPool};
use serde_json::Value;
use crate::config::Config;
use crate::data_structures::Caches;
use crate::interface::Interface;

pub struct FluentdInterface {
    config: Config,
    pool: WorkerPool
}
impl FluentdInterface {
    pub fn new(config: Config) -> Self {

        let pool = {
            let addr = format!("{}:{}",
                               config.output.fluentd.as_ref().unwrap().address,
                               config.output.fluentd.as_ref().unwrap().port
            );
            let settings = Settings {
                flush_period: time::Duration::from_millis(10),
                max_flush_entries: 1000,
                connection_retry_timeout: time::Duration::from_secs(60),
                write_timeout: time::Duration::from_secs(30),
                read_timeout: time::Duration::from_secs(30),
                ..Default::default()
            };
            WorkerPool::with_settings(&addr, &settings).expect("Couldn't create the worker pool.")
        };
        FluentdInterface {
            config,
            pool,
        }
    }
}

impl Interface for FluentdInterface {
    fn send_logs(&mut self, cache: Caches) {

        let all_logs = cache.get_all();
        for logs in all_logs {
            for log in logs {
                let time_string = log.get("CreationTime").unwrap().as_str().unwrap();
                let time = NaiveDateTime::parse_from_str(
                    time_string, "%Y-%m-%dT%H:%M:%S").unwrap();
                let time_utc = DateTime::<Utc>::from_naive_utc_and_offset(time, Utc);
                let timestamp = SystemTime::from(time_utc);
                self.pool.send(self.config.output.fluentd.as_ref().unwrap().tenantName.clone(),
                               log, timestamp).unwrap();
            }
        }
    }
}