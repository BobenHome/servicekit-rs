use anyhow::{Context, Result};
use chrono::{Duration, Local};
use sqlx::{Database, Execute, FromRow, MySql, MySqlPool, QueryBuilder};
use std::fmt::Debug;
use std::marker::Unpin;
use std::sync::Arc;
use tracing::{error, info};

use crate::schedule::BasePsnPushTask;
use crate::utils::mss_client::psn_dos_push;
use crate::{DynamicPsnData, PsnDataKind};
use serde_json::json;

pub const BATCH_SIZE: usize = 1000;

// 定义查询类型枚举
pub enum QueryType {
    ByDate(String),
    ByIds(Vec<String>),
}

pub trait PsnDataWrapper: Send + Sync + 'static {
    // 修正：在 DataType 的 trait bound 中添加 Unpin
    type DataType: for<'r> FromRow<'r, <MySql as Database>::Row> + Debug + Send + Sync + Unpin;
    fn wrap_data(data: Self::DataType) -> DynamicPsnData;
    fn get_query_builder(query_type: QueryType) -> QueryBuilder<'static, MySql>;

    // 新增：获取此 Wrapper 处理的 DynamicPsnData 的种类
    fn get_psn_data_kind_for_wrapper() -> PsnDataKind;
}

// 辅助函数：根据 PsnDataKind 类型获取 ClickHouse 表名
fn get_clickhouse_table_name(kind: PsnDataKind) -> &'static str {
    match kind {
        PsnDataKind::Class => "DXXY_LOCAL.TRAIN_SOURCE_DATA_ZTK_ALL",
        PsnDataKind::Lecturer => "DXXY_LOCAL.TRAIN_COURSE_DATA_ZTK_ALL",
        PsnDataKind::Archive => "DXXY_LOCAL.TRAIN_USER_DATA_ZTK_ALL",
        PsnDataKind::Training => "UNKNOWN_TABLE_FOR_TRAINING",
        _ => "UNKNOWN_TABLE",
    }
}

// 辅助函数：根据 PsnDataKind 类型获取 ID 字段名
fn get_clickhouse_id_column(kind: PsnDataKind) -> &'static str {
    match kind {
        PsnDataKind::Class => "T_TRAINID",
        PsnDataKind::Lecturer => "id",
        PsnDataKind::Archive => "id",
        PsnDataKind::Training => "UNKNOWN_ID_COLUMN_FOR_TRAINING",
        _ => "UNKNOWN_ID_COLUMN",
    }
}

// 新增辅助函数：根据 PsnDataKind 类型获取 MySQL 表名
fn get_mysql_table_name(kind: PsnDataKind) -> &'static str {
    match kind {
        PsnDataKind::Class | PsnDataKind::ClassSc => "NU_trainSourceData_ztk",
        PsnDataKind::Lecturer | PsnDataKind::LecturerSc => "NU_TRAINCOURSESOURCEDATA_ZTK",
        PsnDataKind::Archive | PsnDataKind::ArchiveSc => "nu_trainusersourcedata_ztk",
        PsnDataKind::Training | PsnDataKind::TrainingSc => {
            "TABLE_NOT_APPLICABLE_FOR_TRAINING_MYSQL" // 占位符
        }
    }
}

// 新增辅助函数：根据 PsnDataKind 类型获取 MySQL ID 字段名
fn get_mysql_id_column(kind: PsnDataKind) -> &'static str {
    match kind {
        PsnDataKind::Class | PsnDataKind::ClassSc => "TRAINID",
        PsnDataKind::Lecturer | PsnDataKind::LecturerSc => "id",
        PsnDataKind::Archive | PsnDataKind::ArchiveSc => "id",
        PsnDataKind::Training | PsnDataKind::TrainingSc => {
            "ID_COLUMN_NOT_APPLICABLE_FOR_TRAINING_MYSQL" // 占位符
        }
    }
}

// 核心的通用执行逻辑函数
pub async fn execute_push_task_logic<W: PsnDataWrapper>(base_task: &BasePsnPushTask) -> Result<()> {
    let psn_data_kind = W::get_psn_data_kind_for_wrapper(); // 获取当前任务处理的数据类型种类
    let task_display_name = psn_data_kind.to_task_display_name(); // 获取任务名称
    info!(
        "Running {} via execute_push_task_logic at: {}",
        task_display_name,
        Local::now().format("%Y-%m-%d %H:%M:%S")
    );

    let query_type = if let Some(date_str) = base_task.hit_date.clone() {
        // <--- 克隆 String 以便 QueryType 拥有
        info!("Processing data for specific date: {}", date_str);
        QueryType::ByDate(date_str) // <--- 传递拥有所有权的 String
    } else if let Some(ids) = base_task.train_ids.clone() {
        // <--- 克隆 Vec<String> 以便 QueryType 拥有
        info!("Processing data for specific IDs: {:?}", ids);
        QueryType::ByIds(ids) // <--- 传递拥有所有权的 Vec<String>
    } else {
        // 如果没有提供 train_ids 和 hit_date，则回退到计算“昨天”的日期
        let today = Local::now().date_naive();
        let yesterday = today - Duration::days(1);
        let hit_date_calculated = yesterday.format("%Y-%m-%d").to_string(); // <--- 创建拥有所有权的 String
        info!(
            "Processing data for calculated hit_date: {}",
            hit_date_calculated
        );
        QueryType::ByDate(hit_date_calculated) // <--- 传递拥有所有权的 String
    };

    let datas = W::get_query_builder(query_type)
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
        info!("No data found for task: {}", task_display_name);
        return Ok(());
    }
    for data in datas {
        info!("Found {}: {:?}", task_display_name, data);
        let psn_data_enum = W::wrap_data(data);

        let current_id = psn_data_enum.get_data_id().to_string();

        if let Err(e) = psn_dos_push(
            &base_task.http_client,
            Arc::clone(&base_task.mss_info_config),
            &base_task.archiving_mapper,
            &base_task.push_result_parser,
            &psn_data_enum,
        )
        .await
        {
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
                let payload = vec![json!({&class_data.training_id: &class_data.training_status})];
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

    // --- ClickHouse Updates ---
    if matches!(
        psn_data_kind,
        PsnDataKind::Training
            | PsnDataKind::ClassSc
            | PsnDataKind::LecturerSc
            | PsnDataKind::TrainingSc
            | PsnDataKind::ArchiveSc
    ) {
        // 不更新 ClickHouse
        info!(
            "Skipping ClickHouse updates for PsnDataKind: {:?}.",
            psn_data_kind
        );
    } else {
        // 在数据处理前，直接从 PsnDataWrapper 获取 ClickHouse 的表和ID字段
        let clickhouse_table = get_clickhouse_table_name(psn_data_kind);
        let clickhouse_id_column = get_clickhouse_id_column(psn_data_kind);
        info!(
            "Processing data for ClickHouse table: '{}' using ID column: '{}' for task: {}",
            clickhouse_table, clickhouse_id_column, task_display_name
        );

        if !success_ids.is_empty() {
            for chunk in success_ids.chunks(BATCH_SIZE) {
                let ids_for_query = chunk
                    .iter()
                    .map(|id| format!("'{}'", id))
                    .collect::<Vec<String>>()
                    .join(",");

                let status = "1"; // Success status
                let query_sql = format!(
                    "ALTER TABLE {} UPDATE trainNotifyMss = '{}' WHERE {} IN ({})",
                    clickhouse_table, status, clickhouse_id_column, ids_for_query
                );
                info!("Attempting to update success status in ClickHouse.");
                base_task
                    .clickhouse_client
                    .execute_on_all_nodes(&query_sql)
                    .await;
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
                base_task
                    .clickhouse_client
                    .execute_on_all_nodes(&query_sql)
                    .await;
            }
        }
    }

    // --- MySQL Updates ---
    if matches!(
        psn_data_kind,
        PsnDataKind::Training | PsnDataKind::TrainingSc
    ) {
        // 不更新 MySQL
        info!(
            "Skipping MySQL updates for PsnDataKind: {:?}.",
            psn_data_kind
        );
    } else {
        let mysql_table = get_mysql_table_name(psn_data_kind);
        let mysql_id_column = get_mysql_id_column(psn_data_kind);

        // 只有 PsnDataKind::Lecturer 类型需要更新 trainNotifyMssMessage 字段
        let update_message_field = psn_data_kind == PsnDataKind::Lecturer; // <--- 根据类型设置此标志
        info!(
                "Attempting MySQL updates for PsnDataKind::{:?} (Table: '{}', ID Column: '{}', Update message field: {}).",
                psn_data_kind, mysql_table, mysql_id_column, update_message_field
            );

        // 处理成功 ID 的 MySQL 更新
        if !success_ids.is_empty() {
            // 将成功 ID 转换为 (String, Option<String>) 格式，消息为 None
            let success_items: Vec<(String, Option<String>)> =
                success_ids.iter().map(|id| (id.clone(), None)).collect();

            for chunk in success_items.chunks(BATCH_SIZE) {
                update_notify_mss_mysql(
                    &base_task.pool,
                    mysql_table,
                    mysql_id_column,
                    "1",
                    chunk,
                    update_message_field,
                )
                .await;
            }
        }

        // 处理失败 ID 的 MySQL 更新
        if !failed_ids.is_empty() {
            // failed_ids 已经是 Vec<(String, Option<String>)>，可以直接使用
            for chunk in failed_ids.chunks(BATCH_SIZE) {
                update_notify_mss_mysql(
                    &base_task.pool,
                    mysql_table,
                    mysql_id_column,
                    "2",
                    chunk,
                    update_message_field,
                )
                .await;
            }
        }
    }

    info!("{} completed successfully.", task_display_name);

    Ok(())
}

// 更新 MySQL 表的 `trainNotifyMss` 字段和可选的 `trainNotifyMssMessage` 字段。
///
/// 根据传入的 `table_name` 和 `id_column` 来构建更新语句。
/// `items` 参数是 `(ID, Option<Message>)` 的元组列表。
/// `update_message_field` 参数指示是否应更新 `trainNotifyMssMessage` 字段。
pub async fn update_notify_mss_mysql(
    pool: &MySqlPool,
    table_name: &str,
    id_column: &str,
    status: &str,
    items: &[(String, Option<String>)],
    update_message_field: bool,
) {
    if items.is_empty() {
        return;
    }

    // 构建 UPDATE ... SET trainNotifyMss = CASE <id_column> WHEN <id_value> THEN <status> ... END
    let mut query_builder: QueryBuilder<MySql> = QueryBuilder::new(format!(
        "UPDATE {} SET trainNotifyMss = CASE {} ",
        table_name, id_column
    ));

    // 为每个 item 构建 WHEN ... THEN ... 部分
    for (id, _) in items {
        query_builder.push(" WHEN "); // 推送 SQL 关键字
        query_builder.push_bind(id.clone()); // 绑定 ID 值，sqlx 会为其生成一个 ?
        query_builder.push(" THEN "); // 推送 SQL 关键字
        query_builder.push_bind(status); // 绑定状态值，sqlx 会为其生成一个 ?
    }
    query_builder.push(" END"); // 结束 CASE 语句

    // 根据 `update_message_field` 参数来决定是否包含 `trainNotifyMssMessage` 的更新逻辑
    if update_message_field {
        query_builder.push(format!(", trainNotifyMssMessage = CASE {} ", id_column));
        for (id, msg_opt) in items {
            query_builder.push(" WHEN "); // 推送 SQL 关键字
            query_builder.push_bind(id.clone()); // 绑定 ID 值
            query_builder.push(" THEN "); // 推送 SQL 关键字

            if status == "2" {
                // 失败状态，绑定消息
                query_builder.push_bind(msg_opt.clone()); // 绑定消息值
            } else {
                // 成功状态，明确设置为 NULL
                query_builder.push("NULL ");
            }
        }
        query_builder.push(" END"); // 结束 CASE 语句
    }

    // 构建 WHERE id IN (...)
    query_builder.push(format!(" WHERE {} IN (", id_column));
    let mut separated = query_builder.separated(", ");
    for (id, _) in items {
        separated.push_bind(id);
    }
    separated.push_unseparated(")");

    let query = query_builder.build();

    info!(
        "Executing MySQL update query for table '{}', ID column '{}'. Status: {}, Items count: {}, Update message field: {}",
        table_name, id_column, status, items.len(), update_message_field
    );
    // 打印构建的 SQL 语句和绑定参数，便于调试验证
    info!("Built MySQL update query: {}", query.sql());

    match query.execute(pool).await {
        Ok(result) => {
            info!(
                "MySQL update for table '{}' completed. Rows affected: {}",
                table_name,
                result.rows_affected()
            );
        }
        Err(e) => {
            error!(
                "Failed to update MySQL table '{}' (status: {}, items: {:?}): {:?}",
                table_name, status, items, e
            );
        }
    }
}
