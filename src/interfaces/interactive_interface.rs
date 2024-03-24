use std::collections::HashMap;
use async_trait::async_trait;
use csv::Writer;
use tokio::sync::mpsc::UnboundedSender;
use crate::data_structures::{Caches};
use crate::interfaces::interface::Interface;

pub struct InteractiveInterface {
    tx_log: UnboundedSender<Vec<String>>,
}

impl InteractiveInterface {

    pub fn new(tx_log: UnboundedSender<Vec<String>>) -> Self {

        let interface = InteractiveInterface {
            tx_log,
        };
        interface
    }
}

#[async_trait]
impl Interface for InteractiveInterface {

    async fn send_logs(&mut self, mut logs: Caches) {

        let mut all_logs = logs.get_all();
        let mut columns: Vec<String> = Vec::new();
        for content_type in all_logs.iter_mut() {
            columns.append(&mut crate::interfaces::file_interface::get_all_columns(content_type));
        }
        self.tx_log.send(columns.clone()).unwrap();

        for logs in all_logs.iter_mut() {
            for log in logs.iter_mut() {
                let new_log = crate::interfaces::file_interface::fill_log(log, &columns);
                self.tx_log.send(new_log).unwrap();
            }
        }
    }
}
