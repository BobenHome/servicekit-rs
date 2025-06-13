use super::task::MySchedule;
use crate::PsnClass;
use async_trait::async_trait;
use chrono::{Duration, Local};
use sqlx::Execute;
use sqlx::{MySql, MySqlPool, QueryBuilder};

pub struct PsntrainPushTask {
    pub pool: MySqlPool,
}

// 在结构体 impl 中定义普通方法
impl PsntrainPushTask {
    
}

#[async_trait]
impl MySchedule for PsntrainPushTask {
    async fn execute(&self) {
        println!("Running scheduled task at: {}", chrono::Local::now());

        let mut query_builder =
            QueryBuilder::<MySql>::new(sqlx::query_file!("queries/trains.sql").sql());

        // 获取当前本地日期
        let today = Local::now().date_naive();
        // 减去一天
        let yesterday = today - Duration::days(1);
        // 格式化为 "YYYY-MM-DD"
        let hit_date = yesterday.format("%Y-%m-%d").to_string();

        query_builder.push(" AND a.hitdate = ");
        query_builder.push_bind(hit_date);
        query_builder.push(" LIMIT 1 ");
        // 执行查询
        match query_builder
            .build_query_as::<PsnClass>()
            .fetch_all(&self.pool)
            .await
        {
            Ok(psns) => {
                for psn in psns {
                    println!("Found psn: {:?}", psn);
                }
            }
            Err(e) => eprintln!("Database error: {}", e),
        }
    }

    fn cron_expression(&self) -> &str {
        "0 0 5 * * * *" // 每天凌晨5点执行一次
    }
}
