use std::time::SystemTime;
use chrono::{DateTime, NaiveDateTime, Utc};
use core::time;
use async_trait::async_trait;
use poston::{Client, Settings, WorkerPool};
use crate::config::Config;
use crate::data_structures::{ArbitraryJson, Caches};
use crate::interfaces::interface::Interface;

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

    fn get_tenant_name(&self) -> String {
        self.config.output.fluentd.as_ref().unwrap().tenant_name.clone()
    }
}

#[async_trait]
impl Interface for FluentdInterface {
    async fn send_logs(&mut self, mut logs: Caches) {

        let all_logs = logs.get_all();
        for logs in all_logs {
            for log in logs {
                let timestamp = get_timestamp(log);
                self.pool.send(self.get_tenant_name(), log, timestamp).unwrap();
            }
        }
    }
}

fn get_timestamp(log: &ArbitraryJson) -> SystemTime {

    let time_string = log.get("CreationTime").unwrap().as_str().unwrap();
    let time = NaiveDateTime::parse_from_str(
        time_string, "%Y-%m-%dT%H:%M:%S").unwrap();
    let time_utc = DateTime::<Utc>::from_naive_utc_and_offset(time, Utc);
    SystemTime::from(time_utc)
}