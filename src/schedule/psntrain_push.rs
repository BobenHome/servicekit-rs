use crate::PsnClass;
use async_trait::async_trait;
use sqlx::{MySql, MySqlPool, QueryBuilder};
use sqlx::Execute;
use super::task::MySchedule;

pub struct PsntrainPushTask {
    pub pool: MySqlPool,
}

#[async_trait]
impl MySchedule for PsntrainPushTask {
    async fn execute(&self) {
        println!("Running scheduled task at: {}", chrono::Local::now());

        let mut query_builder =
            QueryBuilder::<MySql>::new(sqlx::query_file!("queries/trains.sql").sql());

        // 处理 hitDate 条件
        let hit_date = Some("2023-11-22");
        if let Some(date) = hit_date {
            query_builder.push(" AND a.hitdate = ");
            query_builder.push_bind(date);
        }

        // 处理 train_ids 条件
        let train_ids = Some(vec![
            "009b6addc3394455a645938b5bc981b7",
            "015dc7e8a2ec46ac958d1713617ffd1e",
        ]);
        if let Some(ids) = train_ids {
            if !ids.is_empty() {
                query_builder.push(" AND a.TRAINID IN (");
                let mut separated = query_builder.separated(", ");
                for id in ids {
                    separated.push_bind(id);
                }
                separated.push_unseparated(")");
            }
        }
        // 执行查询
        match query_builder
            .build_query_as::<PsnClass>()
            .fetch_all(&self.pool)
            .await
        {
            Ok(orgs) => {
                for org in orgs {
                    println!("Found orgs: {:?}", org);
                }
            }
            Err(e) => eprintln!("Database error: {}", e),
        }
    }

    fn cron_expression(&self) -> &str {
        "0 */10 * * * * *" // 每分钟执行一次
    }
}
