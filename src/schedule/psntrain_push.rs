use crate::PsnClass;
use chrono::{Duration, Local};
use sqlx::Execute;
use sqlx::{MySql, MySqlPool, QueryBuilder};

pub struct PsntrainPushTask {
    pub pool: MySqlPool,
}

impl PsntrainPushTask {
    // 这只是一个普通的公共异步方法，不再是 trait 的一部分
    pub async fn execute(&self) {
        // 这里的业务逻辑完全不需要改变！
        println!(
            "Running task via tokio-cron-scheduler at: {}",
            chrono::Local::now()
        );

        let mut query_builder =
            QueryBuilder::<MySql>::new(sqlx::query_file!("queries/trains.sql").sql());

        let today = Local::now().date_naive();
        let yesterday = today - Duration::days(1);
        let hit_date = yesterday.format("%Y-%m-%d").to_string();

        query_builder.push(" AND a.hitdate = ");
        query_builder.push_bind(hit_date);
        query_builder.push(" LIMIT 1 ");

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

    // 我们保留这个方法，以便在 main.rs 中方便地获取 Cron 表达式
    pub fn cron_expression(&self) -> &str {
        // 注意：你原来的 "0 0 5 * * * *" 包含7个字段（带秒），
        // tokio-cron-scheduler 完美支持这种格式。
        "0 30,40 * * * *" // 每小时30和40分钟执行一次
    }
}
