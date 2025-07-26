use std::sync::Arc;

use crate::{
    schedule::{
        CompositeTask, PsnArchivePushTask, PsnLecturerPushTask, PsnTrainPushTask,
        PsnTrainingPushTask,
    },
    utils::{ClickHouseClient, GatewayClient},
    web::{models::ApiResponse, PushDataParams},
    AppConfig, TaskExecutor,
};
use actix_web::{post, web, HttpResponse, Result};
use chrono::{Duration, NaiveDate};
use sqlx::MySqlPool;
use tracing::{error, info};

#[post("/pxb/pushByDate")]
pub async fn push_by_date(
    pool: web::Data<MySqlPool>,
    app_config: web::Data<AppConfig>, // 注入 AppConfig
    body: web::Json<PushDataParams>,  // <--- 接收 JSON 请求体
) -> Result<HttpResponse> {
    // 1. 验证请求参数
    if let Err(e) = body.validate() {
        return Ok(HttpResponse::BadRequest().json(ApiResponse::<()>::error(e)));
    }
    // 根据参数类型分发任务
    let begin_date_opt = body.begin_date.clone();
    let end_date_opt = body.end_date.clone();
    let train_ids_opt = body.train_ids.clone();

    // 克隆必要的配置和连接池，以便在异步任务中使用
    let pool_clone = pool.get_ref().clone();
    let telecom_config_clone = app_config.telecom_config.clone();

    tokio::spawn(async move {
        info!("********pxb mss pushByDate begin********");

        // 使用从配置中读取配置
        let gateway_client = Arc::new(GatewayClient::new(&telecom_config_clone));
        // --- Initialize ClickHouseClient ---
        let clickhouse_client =
            Arc::new(ClickHouseClient::new(&app_config.clickhouse_config).unwrap());
        info!("ClickHouseClient initialized.");

        if let Some(ids) = train_ids_opt {
            // 对于 ID 推送，我们只执行一次任务，因为所有 ID 可以在一个任务中处理
            let push_train_task = Arc::new(PsnTrainPushTask::new(
                pool_clone.clone(),
                app_config.mss_info_config.clone(),
                Arc::clone(&gateway_client),
                Arc::clone(&clickhouse_client),
                None,
                Some(ids.clone()),
            ));
            // 6. 创建 PsnLecturerPushTask 实例
            let push_lecturer_task = Arc::new(PsnLecturerPushTask::new(
                pool_clone.clone(),
                app_config.mss_info_config.clone(),
                Arc::clone(&gateway_client),
                Arc::clone(&clickhouse_client),
                None,
                Some(ids.clone()),
            ));
            // 7. 创建 PsnTrainingPushTask 实例
            let push_training_task = Arc::new(PsnTrainingPushTask::new(
                pool_clone.clone(),
                app_config.mss_info_config.clone(),
                Arc::clone(&gateway_client),
                Arc::clone(&clickhouse_client),
                None,
                Some(ids.clone()),
            ));
            // 8. 创建 PsnArchivePushTask 实例
            let push_archive_task = Arc::new(PsnArchivePushTask::new(
                pool_clone.clone(),
                app_config.mss_info_config.clone(),
                Arc::clone(&gateway_client),
                Arc::clone(&clickhouse_client),
                None,
                Some(ids.clone()),
            ));
            // --- 将需要串行执行的任务打包进 Vec ---
            let composite_tasks: Vec<Arc<dyn TaskExecutor + Send + Sync + 'static>> = vec![
                push_train_task,
                push_lecturer_task,
                push_training_task,
                push_archive_task,
            ];
            // --- 创建 CompositeTask 实例 ---
            let composite_task = Arc::new(CompositeTask::new(
                composite_tasks,
                "培训班数据归档到MSS根据ID".to_string(),
            ));
            // 执行 CompositeTask
            let _ = composite_task.execute().await;
        } else if let (Some(begin_date_str), Some(end_date_str)) = (begin_date_opt, end_date_opt) {
            // 尝试解析为标准日期
            let parse_result_begin = NaiveDate::parse_from_str(&begin_date_str, "%Y-%m-%d");
            let parse_result_end = NaiveDate::parse_from_str(&end_date_str, "%Y-%m-%d");
            // 最终要处理的日期
            let mut dates_to_process: Vec<String> = Vec::new();

            let mut is_special_date_range = false;
            if let Some(month_str) = begin_date_str.split('-').nth(1) {
                if let Ok(month) = month_str.parse::<u32>() {
                    if month > 12 {
                        is_special_date_range = true;
                    }
                }
            }
            if is_special_date_range {
                // 特殊日期范围处理 (如 2025-13-08)
                let begin_splits: Vec<&str> = begin_date_str.split('-').collect();
                let end_splits: Vec<&str> = end_date_str.split('-').collect();

                if let (Some(year), Some(month), Some(begin_day_str), Some(end_day_str)) = (
                    begin_splits.get(0),
                    begin_splits.get(1),
                    begin_splits.get(2),
                    end_splits.get(2),
                ) {
                    if let (Ok(begin_day), Ok(end_day)) =
                        (begin_day_str.parse::<u32>(), end_day_str.parse::<u32>())
                    {
                        for i in begin_day..=end_day {
                            dates_to_process.push(format!(
                                "{}-{:02}-{:02}",
                                year,
                                month.parse::<u32>().unwrap_or(1),
                                i
                            ));
                        }
                    } else {
                        error!(
                            "pushByDate: Failed to parse special date range days from {} and {}",
                            begin_date_str, end_date_str
                        );
                    }
                } else {
                    error!(
                        "pushByDate: Invalid format for special date range: {} and {}",
                        begin_date_str, end_date_str
                    );
                }
            } else if parse_result_begin.is_ok() && parse_result_end.is_ok() {
                // 标准日期范围处理
                let mut current_date = parse_result_begin.unwrap();
                let end_date = parse_result_end.unwrap();

                while current_date <= end_date {
                    dates_to_process.push(current_date.format("%Y-%m-%d").to_string());
                    current_date += Duration::days(1);
                }
            } else {
                // 如果解析失败，且不是特殊月份格式，则记录错误
                error!(
                    "pushByDate: Invalid date format or range: beginDate={}, endDate={}",
                    begin_date_str, end_date_str
                );
                // 在异步任务中，我们不能直接返回 HTTP 响应。
                // 这种情况下，日志是主要的反馈机制。
                info!("********pxb mss pushByDate end (with errors)********");
                return; // 提前退出异步任务
            }

            // 如果 beginDate.equals(endDate)，确保只处理一天
            if begin_date_str == end_date_str && !dates_to_process.contains(&begin_date_str) {
                dates_to_process = vec![begin_date_str.clone()];
            }

            // 遍历需要处理的每个日期
            for current_date in dates_to_process {
                info!("=================={}=======================", current_date);
                // 创建 PsnTrainPushTask 实例
                let push_train_task = Arc::new(PsnTrainPushTask::new(
                    pool_clone.clone(),
                    app_config.mss_info_config.clone(),
                    Arc::clone(&gateway_client),
                    Arc::clone(&clickhouse_client),
                    Some(current_date.clone()),
                    None,
                ));
                // 6. 创建 PsnLecturerPushTask 实例
                let push_lecturer_task = Arc::new(PsnLecturerPushTask::new(
                    pool_clone.clone(),
                    app_config.mss_info_config.clone(),
                    Arc::clone(&gateway_client),
                    Arc::clone(&clickhouse_client),
                    Some(current_date.clone()),
                    None,
                ));
                // 7. 创建 PsnTrainingPushTask 实例
                let push_training_task = Arc::new(PsnTrainingPushTask::new(
                    pool_clone.clone(),
                    app_config.mss_info_config.clone(),
                    Arc::clone(&gateway_client),
                    Arc::clone(&clickhouse_client),
                    Some(current_date.clone()),
                    None,
                ));
                // 8. 创建 PsnArchivePushTask 实例
                let push_archive_task = Arc::new(PsnArchivePushTask::new(
                    pool_clone.clone(),
                    app_config.mss_info_config.clone(),
                    Arc::clone(&gateway_client),
                    Arc::clone(&clickhouse_client),
                    Some(current_date.clone()),
                    None,
                ));
                // --- 将需要串行执行的任务打包进 Vec ---
                let composite_tasks: Vec<Arc<dyn TaskExecutor + Send + Sync + 'static>> = vec![
                    push_train_task,
                    push_lecturer_task,
                    push_training_task,
                    push_archive_task,
                ];
                // --- 创建 CompositeTask 实例 ---
                let composite_task = Arc::new(CompositeTask::new(
                    composite_tasks,
                    "培训班数据归档到MSS根据日期".to_string(),
                ));
                // 执行 CompositeTask
                let _ = composite_task.execute().await;
            }
        }
        info!("********pxb mss pushByDate end********");
    });

    // 立即返回成功响应，因为处理是异步的
    Ok(HttpResponse::Ok().json(ApiResponse::<String>::success(
        "pushing, check logs for progress.".to_string(),
    )))
}
