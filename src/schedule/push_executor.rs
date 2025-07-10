// src/schedule/push_executor.rs
use anyhow::{Context, Result};
use chrono::{Duration, Local};
use sqlx::{Database, FromRow, MySql, QueryBuilder}; // 导入 Database trait
use std::fmt::Debug;
use std::marker::Unpin;
use tracing::{error, info};

use crate::schedule::BasePsnPushTask;
use crate::utils::mss_client::psn_dos_push;
use crate::DynamicPsnData;

pub trait PsnDataWrapper: Send + Sync + 'static {
    // 修正：在 DataType 的 trait bound 中添加 Unpin
    type DataType: for<'r> FromRow<'r, <MySql as Database>::Row> + Debug + Send + Sync + Unpin;
    fn wrap_data(data: Self::DataType) -> DynamicPsnData;
    fn get_final_query_builder(hit_date: &str) -> QueryBuilder<'static, MySql>;
}

// 核心的通用执行逻辑函数
pub async fn execute_push_task_logic<W: PsnDataWrapper>(base_task: &BasePsnPushTask) -> Result<()> {
    info!(
        "Running {} via tokio-cron-scheduler at: {}",
        base_task.task_name,
        Local::now().format("%Y-%m-%d %H:%M:%S")
    );

    let today = Local::now().date_naive();
    let yesterday = today - Duration::days(1);
    let hit_date = yesterday.format("%Y-%m-%d").to_string();

    // <-- 从 PsnDataWrapper 获取预构建的 QueryBuilder
    let mut query_builder = W::get_final_query_builder(&hit_date);

    let datas = query_builder
        .build_query_as::<W::DataType>()
        .fetch_all(&base_task.pool)
        .await
        .context(format!(
            "Failed to fetch {} data from database",
            base_task.task_name
        ))?;

    if datas.is_empty() {
        info!("No {} found for hitdate: {}", base_task.task_name, hit_date);
    } else {
        for data in datas {
            info!("Found {}: {:?}", base_task.task_name, data);
            let psn_data_enum = W::wrap_data(data);
            if let Err(e) = psn_dos_push(
                &base_task.http_client,
                &base_task.mss_info_config,
                &base_task.archiving_mapper,
                &base_task.push_result_parser,
                &psn_data_enum,
            )
            .await
            {
                error!(
                    "Failed to send data of type '{}' to third party: {:?}. Task: {}",
                    psn_data_enum.get_key_name(),
                    e,
                    base_task.task_name
                );
            } else {
                info!(
                    "Successfully sent data of type '{}' to third party. Task: {}",
                    psn_data_enum.get_key_name(),
                    base_task.task_name
                );
            }
        }
    }
    info!("{} completed successfully.", base_task.task_name);

    Ok(())
}
