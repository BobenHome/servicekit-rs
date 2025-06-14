use crate::PsnClass;
use anyhow::{Context, Result}; // 导入 anyhow::Result 和 Context trait
use chrono::{Duration, Local};
use log::info; // 导入 log 宏
use sqlx::Execute;
use sqlx::{MySql, MySqlPool, QueryBuilder};

pub struct PsntrainPushTask {
    pub pool: MySqlPool,
}

impl PsntrainPushTask {
    // 这只是一个普通的公共异步方法，不再是 trait 的一部分
    pub async fn execute(&self) -> Result<()> {
        info!("Running task via tokio-cron-scheduler at: {}", Local::now().format("%Y-%m-%d %H:%M:%S"));

        let mut query_builder =
            QueryBuilder::<MySql>::new(sqlx::query_file!("queries/trains.sql").sql());

        let today = Local::now().date_naive();
        let yesterday = today - Duration::days(1);
        let hit_date = yesterday.format("%Y-%m-%d").to_string();

        query_builder.push(" AND a.hitdate = ");
        query_builder.push_bind(&hit_date);
        query_builder.push(" LIMIT 1 ");

        let psns = query_builder
            .build_query_as::<PsnClass>()
            .fetch_all(&self.pool)
            .await
            .context("Failed to fetch trains from database")?; // 将数据库错误转换为 anyhow::Error 并添加上下文

        if psns.is_empty() {
            info!("No trains found for hitdate: {}", hit_date);
        } else {
            for psn in psns {
                info!("Found psn: {:?}", psn);
                // 这里可以添加处理每个 psn 的逻辑，例如推送消息、更新状态等
                // 如果这里的逻辑也可能失败，并且你希望捕获这些错误，
                // 那么处理函数也应该返回 Result。
            }
        }
        info!("PsntrainPushTask completed successfully.");
        Ok(()) // 如果一切正常，返回 Ok(())
    }

    // 我们保留这个方法，以便在 main.rs 中方便地获取 Cron 表达式
    pub fn cron_expression(&self) -> &str {
        // tokio-cron-scheduler 完美支持这种格式。
        "0 */1 * * * *" // 每5分钟执行一次
    }
}
