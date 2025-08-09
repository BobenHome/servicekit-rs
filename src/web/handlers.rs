use std::sync::Arc;

use crate::{
    schedule::{
        CompositeTask, PsnArchivePushTask, PsnArchiveScPushTask, PsnLecturerPushTask,
        PsnLecturerScPushTask, PsnTrainPushTask, PsnTrainScPushTask, PsnTrainingPushTask,
        PsnTrainingScPushTask,
    },
    web::{models::ApiResponse, PushDataParams},
    AppContext, TaskExecutor,
};
use actix_web::{post, web, HttpResponse, Result};
use chrono::{Duration, NaiveDate};
use tracing::{error, info, warn};

#[post("/pxb/pushMss")]
pub async fn push_msss(
    app_context: web::Data<Arc<AppContext>>, // 注入 AppContext
    body: web::Json<PushDataParams>,         // 接收 JSON 请求体
) -> Result<HttpResponse> {
    // 验证请求参数
    if let Err(e) = body.validate() {
        return Ok(HttpResponse::BadRequest().json(ApiResponse::<()>::error(e)));
    }

    // 克隆必要的配置和连接池，以便在异步任务中使用
    let app_context = Arc::clone(&app_context);

    tokio::spawn(async move {
        info!("--------pxb mss pushByDate begin--------");

        // 直接从 `body` 结构体中获取数据，不再需要额外的 `clone()`
        let begin_date_opt = &body.begin_date;
        let end_date_opt = &body.end_date;
        let train_ids_opt = &body.train_ids;
        let is_sichuan_data = &body.is_sichuan_data;

        if let Some(ids) = train_ids_opt {
            // 情况 1: 提供了 train_ids
            process_push_tasks(
                Arc::clone(&app_context),
                None,
                Some(ids.to_vec()),
                *is_sichuan_data,
            )
            .await;
        } else if let (Some(begin_date_str), Some(end_date_str)) = (begin_date_opt, end_date_opt) {
            // 情况 2: 未提供 train_ids，根据日期处理
            let dates_to_process: Vec<String> =
                match parse_date_range_strings(begin_date_str, end_date_str) {
                    Ok(dates) => dates, // 直接返回 dates，赋给 dates_to_process
                    Err(e) => {
                        error!("日期解析错误: {e}");
                        // 如果解析失败，返回一个空的 Vec，确保 dates_to_process 始终是 Vec<String>
                        Vec::new()
                    }
                };
            info!("解析到的日期范围: {dates_to_process:?}");
            if dates_to_process.is_empty() {
                warn!("解析日期后没有要处理的日期。");
            }
            // 遍历需要处理的每个日期
            for current_date in dates_to_process {
                info!("=================={current_date} 开始处理=======================");
                process_push_tasks(
                    Arc::clone(&app_context),
                    Some(current_date.clone()),
                    None,
                    *is_sichuan_data,
                )
                .await;
                info!("=================={current_date} 处理完成=======================");
            }
        }
        info!("--------pxb mss pushByDate end--------");
    });

    // 立即返回成功响应，因为处理是异步的
    Ok(HttpResponse::Ok().json(ApiResponse::<String>::success(
        "pushing, check logs for progress.".to_string(),
    )))
}

// --- 辅助函数：封装了创建和执行推送任务的逻辑 ---
async fn process_push_tasks(
    app_context: Arc<AppContext>,
    hit_date: Option<String>,
    train_ids: Option<Vec<String>>,
    is_sichuan_data: bool,
) {
    let task_name_suffix = if train_ids.is_some() {
        "根据培训班ID"
    } else if hit_date.is_some() {
        "根据日期"
    } else {
        "UNKNOWN"
    };

    let composite_task_name = if is_sichuan_data {
        format!("四川省培训班数据归档到MSS{task_name_suffix}")
    } else {
        format!("培训班数据归档到MSS{task_name_suffix}")
    };

    let composite_tasks: Vec<Arc<dyn TaskExecutor + Send + Sync + 'static>> = if is_sichuan_data {
        vec![
            Arc::new(PsnTrainScPushTask::new(
                Arc::clone(&app_context),
                hit_date.clone(),
                train_ids.clone(),
            )),
            Arc::new(PsnLecturerScPushTask::new(
                Arc::clone(&app_context),
                hit_date.clone(),
                train_ids.clone(),
            )),
            Arc::new(PsnArchiveScPushTask::new(
                Arc::clone(&app_context),
                hit_date.clone(),
                train_ids.clone(),
            )),
            Arc::new(PsnTrainingScPushTask::new(
                Arc::clone(&app_context),
                hit_date.clone(),
                train_ids.clone(),
            )),
        ]
    } else {
        vec![
            Arc::new(PsnTrainPushTask::new(
                Arc::clone(&app_context),
                hit_date.clone(),
                train_ids.clone(),
            )),
            Arc::new(PsnLecturerPushTask::new(
                Arc::clone(&app_context),
                hit_date.clone(),
                train_ids.clone(),
            )),
            Arc::new(PsnArchivePushTask::new(
                Arc::clone(&app_context),
                hit_date.clone(),
                train_ids.clone(),
            )),
            Arc::new(PsnTrainingPushTask::new(
                Arc::clone(&app_context),
                hit_date.clone(),
                train_ids.clone(),
            )),
        ]
    };
    // 创建 CompositeTask 实例
    let composite_task = Arc::new(CompositeTask::new(composite_tasks, composite_task_name));

    // 执行 CompositeTask，错误会在 CompositeTask 内部日志记录
    let _ = composite_task.execute().await;
}

// --- 辅助函数：解析日期范围，包括特殊月份格式 ---
fn parse_date_range_strings(
    begin_date_str: &str,
    end_date_str: &str,
) -> std::result::Result<Vec<String>, String> {
    let mut dates_to_process = Vec::new();
    // 尝试解析为标准日期
    let parse_result_begin = NaiveDate::parse_from_str(begin_date_str, "%Y-%m-%d");
    let parse_result_end = NaiveDate::parse_from_str(end_date_str, "%Y-%m-%d");

    if parse_result_begin.is_ok() && parse_result_end.is_ok() {
        // 情况 A: 均为标准有效日期
        let mut current_date = parse_result_begin.unwrap();
        let end_date = parse_result_end.unwrap();

        if current_date > end_date {
            return Err(format!(
                "起始日期 {begin_date_str} 晚于结束日期 {end_date_str}"
            ));
        }
        while current_date <= end_date {
            dates_to_process.push(current_date.format("%Y-%m-%d").to_string());
            // 正常递增日期，考虑到 chrono 的 Duration::days 足够安全
            current_date += Duration::days(1);
        }
    } else {
        // 情况 B: 至少有一个日期不是标准格式，检查是否是特殊月份格式 (MM > 12)
        let begin_splits: Vec<&str> = begin_date_str.split('-').collect();
        let end_splits: Vec<&str> = end_date_str.split('-').collect();

        if let (Some(year_str), Some(month_str), Some(begin_day_str), Some(end_day_str)) = (
            begin_splits.first(),
            begin_splits.get(1),
            begin_splits.get(2),
            end_splits.get(2),
        ) {
            if let (Ok(year), Ok(month), Ok(begin_day), Ok(end_day)) = (
                year_str.parse::<u32>(),
                month_str.parse::<u32>(),
                begin_day_str.parse::<u32>(),
                end_day_str.parse::<u32>(),
            ) {
                if month > 12 {
                    // 确认是特殊月份格式
                    if begin_day > end_day {
                        return Err(format!(
                            "特殊日期范围中，起始日 {begin_day} 晚于结束日 {end_day}"
                        ));
                    }
                    for i in begin_day..=end_day {
                        dates_to_process.push(format!("{year}-{month:02}-{i:02}"));
                        // 使用原始的非标准月份
                    }
                } else {
                    // 月份在 1-12 范围内，但 `NaiveDate::parse_from_str` 失败，说明是其他格式错误
                    return Err(format!(
                        "日期格式无效或解析失败：{begin_date_str} 或 {end_date_str}"
                    ));
                }
            } else {
                return Err(format!(
                    "日期组件解析失败 (非数字)：{begin_date_str} 或 {end_date_str}"
                ));
            }
        } else {
            return Err(format!(
                "日期格式不完整或无效：{begin_date_str} 或 {end_date_str}"
            ));
        }
    }

    Ok(dates_to_process)
}
