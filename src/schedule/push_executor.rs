// src/schedule/push_executor.rs
use anyhow::{Context, Result};
use chrono::{Duration, Local};
use sqlx::{Database, FromRow, MySql, QueryBuilder}; // 导入 Database trait
use std::fmt::Debug;
use std::marker::Unpin;
use tracing::{error, info};

use crate::schedule::BasePsnPushTask;
use crate::utils::mss_client::psn_dos_push;
use crate::{DynamicPsnData, PsnDataKind};
use serde_json::json;

pub trait PsnDataWrapper: Send + Sync + 'static {
    // 修正：在 DataType 的 trait bound 中添加 Unpin
    type DataType: for<'r> FromRow<'r, <MySql as Database>::Row> + Debug + Send + Sync + Unpin;
    fn wrap_data(data: Self::DataType) -> DynamicPsnData;
    fn get_final_query_builder(hit_date: &str) -> QueryBuilder<'static, MySql>;

    // 新增：获取此 Wrapper 处理的 DynamicPsnData 的种类
    fn get_psn_data_kind_for_wrapper() -> PsnDataKind;
}

// 辅助函数：根据 PsnDataKind 类型获取 ClickHouse 表名
fn get_clickhouse_table_name(kind: PsnDataKind) -> &'static str {
    match kind {
        PsnDataKind::Class => "DXXY_LOCAL.TRAIN_SOURCE_DATA_ZTK_ALL",
        PsnDataKind::Lecturer => "DXXY_LOCAL.TRAIN_COURSE_DATA_ZTK_ALL",
        PsnDataKind::Archive => "DXXY_LOCAL.TRAIN_USER_DATA_ZTK_ALL",
        PsnDataKind::Training => {
            error!("Attempted to get ClickHouse table name for DynamicPsnData::Training. If this type requires ClickHouse update, define its table here.");
            "UNKNOWN_TABLE_FOR_TRAINING"
        }
    }
}

// 辅助函数：根据 PsnDataKind 类型获取 ID 字段名
fn get_clickhouse_id_column(kind: PsnDataKind) -> &'static str {
    match kind {
        PsnDataKind::Class => "T_TRAINID",
        PsnDataKind::Lecturer => "id",
        PsnDataKind::Archive => "id",
        PsnDataKind::Training => {
            error!("Attempted to get ClickHouse ID column for DynamicPsnData::Training. If this type requires ClickHouse update, define its ID column here.");
            "UNKNOWN_ID_COLUMN_FOR_TRAINING"
        }
    }
}

// 核心的通用执行逻辑函数
pub async fn execute_push_task_logic<W: PsnDataWrapper>(base_task: &BasePsnPushTask) -> Result<()> {
    let psn_data_kind = W::get_psn_data_kind_for_wrapper(); // 获取当前任务处理的数据类型种类
    let task_display_name = psn_data_kind.to_task_display_name(); // 获取任务名称
    info!(
        "Running {} via tokio-cron-scheduler at: {}",
        task_display_name,
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
            task_display_name
        ))?;

    // 存储成功和失败的 ID
    let mut success_ids: Vec<String> = Vec::new();
    let mut failed_ids: Vec<(String, Option<String>)> = Vec::new();

    if datas.is_empty() {
        info!("No {} found for hitdate: {}", task_display_name, hit_date);
    } else {
        for data in datas {
            info!("Found {}: {:?}", task_display_name, data);
            let psn_data_enum = W::wrap_data(data);

            let current_id = psn_data_enum.get_data_id().to_string();

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
                    task_display_name
                );
                if matches!(psn_data_enum, DynamicPsnData::Lecturer(_)) {
                    failed_ids.push((current_id, Some(e.to_string())));
                } else {
                    failed_ids.push((current_id, None));
                }
            } else {
                info!(
                    "Successfully sent data of type '{}' to third party. Task: {}",
                    psn_data_enum.get_key_name(),
                    task_display_name
                );
                success_ids.push(current_id);
                // 成功后调用小助手接口，写入归档成功的班级
                if let DynamicPsnData::Class(class_data) = psn_data_enum {
                    let payload =
                        vec![json!({&class_data.training_id: &class_data.training_status})];
                    let _ = base_task
                        .gateway_client
                        .invoke_gateway_service("bj.bjglinfo.gettrainstatusbyid", payload)
                        .await;
                } else {
                    info!(
                        "Skipping gateway service invocation for data of type '{}'. Only 'Class' data is processed by gateway.",
                        psn_data_enum.get_key_name()
                    );
                }
            }
        }
    }

    // Process successful IDs
    if psn_data_kind != PsnDataKind::Training {
        // 在数据处理前，直接从 PsnDataWrapper 获取 ClickHouse 的表和ID字段
        let clickhouse_table = get_clickhouse_table_name(psn_data_kind);
        let clickhouse_id_column = get_clickhouse_id_column(psn_data_kind);
        info!(
            "Processing data for ClickHouse table: '{}' using ID column: '{}' for task: {}",
            clickhouse_table, clickhouse_id_column, task_display_name
        );
        const BATCH_SIZE: usize = 500;

        if !success_ids.is_empty() {
            for chunk in success_ids.chunks(BATCH_SIZE) {
                let ids_for_query = chunk
                    .iter()
                    .map(|id| format!("'{}'", id)) // Prepare IDs for SQL IN clause
                    .collect::<Vec<String>>()
                    .join(",");

                let status = "1"; // Success status
                let query_sql = format!(
                    "ALTER TABLE {} UPDATE trainNotifyMss = '{}' WHERE {} IN ({})",
                    clickhouse_table, status, clickhouse_id_column, ids_for_query
                );
                info!("Attempting to update success status in ClickHouse.");
                match base_task
                    .clickhouse_client
                    .execute_on_all_nodes(&query_sql)
                    .await
                {
                    Ok(_) => info!("ClickHouse update for success batch successful."),
                    Err(e) => error!("Failed to update ClickHouse for success batch: {:?}", e),
                }
            }
        }
        // Process error IDs
        if !failed_ids.is_empty() {
            for chunk in failed_ids.chunks(BATCH_SIZE) {
                let ids_for_query = chunk
                    .iter()
                    .map(|(id, _)| format!("'{}'", id))
                    .collect::<Vec<String>>()
                    .join(",");
                let status = "2"; // Error status

                // Log detailed error reasons for this batch
                for (id, reason_opt) in chunk.iter() {
                    if let Some(reason) = reason_opt {
                        error!("Failed Lecturer ID: {}, Reason: {}", id, reason);
                    } else {
                        error!("Failed ID (other type): {}", id);
                    }
                }
                let query_sql = format!(
                    "ALTER TABLE {} UPDATE trainNotifyMss = '{}' WHERE {} IN ({})",
                    clickhouse_table, status, clickhouse_id_column, ids_for_query
                );
                info!("Attempting to update error status in ClickHouse.");
                match base_task
                    .clickhouse_client
                    .execute_on_all_nodes(&query_sql)
                    .await
                {
                    Ok(_) => info!("ClickHouse update for error batch successful."),
                    Err(e) => error!("Failed to update ClickHouse for error batch: {:?}", e),
                }
            }
        }
    } else {
        info!("Skipping ClickHouse updates for PsnDataKind::Training.");
    }

    info!("{} completed successfully.", task_display_name);

    Ok(())
}
