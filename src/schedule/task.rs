use async_trait::async_trait;

#[async_trait]
pub trait MySchedule {
    async fn execute(&self);
    fn cron_expression(&self) -> &str;
}
