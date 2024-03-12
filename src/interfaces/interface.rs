use async_trait::async_trait;
use crate::data_structures::Caches;

#[async_trait]
pub trait Interface {
    async fn send_logs(&mut self, logs: Caches);
}