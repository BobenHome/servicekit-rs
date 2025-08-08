use anyhow::{Context, Result};
use chrono::Local;
use servicekit::{
    config::AppConfig,
    logging,
    schedule::{
        CompositeTask, PsnArchivePushTask, PsnArchiveScPushTask, PsnLecturerPushTask,
        PsnLecturerScPushTask, PsnTrainPushTask, PsnTrainScPushTask, PsnTrainingPushTask,
        PsnTrainingScPushTask,
    },
    AppContext, TaskExecutor, WebServer,
}; //servicekit是crate 名称（在 Cargo.toml 中定义），代表了库。db::mysql_pool, schedule::PsntrainPushTask, WebServer 这些都是从 lib.rs 中 pub use 或 pub mod 导出的项。如果 lib.rs 不存在或者没有正确地导出这些模块，main.rs 将无法直接通过 servicekit:: 路径来访问它们
use std::sync::Arc;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    // 1. 初始化日志系统
    logging::init_logging().context("Failed to initialize logging")?;

    info!("Application starting...");

    // 2. 加载应用程序配置
    let app_config = AppConfig::new().context("Failed to load application configuration")?;
    info!("Application configuration loaded successfully: {app_config:?}");

    // 3. 初始化任务调度器
    // --- 使用 tokio-cron-scheduler 启动调度器 ---
    let scheduler = JobScheduler::new()
        .await
        .context("Failed to create scheduler")?;
    info!("Scheduler initialized.");

    // 4. 创建AppContext实例
    let app_context = AppContext::new(
        &app_config.database_url,
        Arc::clone(&app_config.mss_info_config),
        Arc::clone(&app_config.telecom_config),
        Arc::clone(&app_config.clickhouse_config),
    )
    .await?;
    let app_context_arc = Arc::new(app_context);

    // 5. 创建 PsnTrainPushTask 实例
    let push_train_task = Arc::new(PsnTrainPushTask::new(
        Arc::clone(&app_context_arc),
        None,
        None,
    ));

    // 6. 创建 PsnLecturerPushTask 实例
    let push_lecturer_task = Arc::new(PsnLecturerPushTask::new(
        Arc::clone(&app_context_arc),
        None,
        None,
    ));

    // 7. 创建 PsnTrainingPushTask 实例
    let push_training_task = Arc::new(PsnTrainingPushTask::new(
        Arc::clone(&app_context_arc),
        None,
        None,
    ));

    // 8. 创建 PsnArchivePushTask 实例
    let push_archive_task = Arc::new(PsnArchivePushTask::new(
        Arc::clone(&app_context_arc),
        None,
        None,
    ));

    // 9. 创建 PsnTrainScPushTask 实例
    let push_train_sc_task = Arc::new(PsnTrainScPushTask::new(
        Arc::clone(&app_context_arc),
        None,
        None,
    ));

    // 10. 创建 PsnLecturerScPushTask 实例
    let push_lecturer_sc_task = Arc::new(PsnLecturerScPushTask::new(
        Arc::clone(&app_context_arc),
        None,
        None,
    ));

    // 11. 创建 PsnTrainingScPushTask 实例
    let push_training_sc_task = Arc::new(PsnTrainingScPushTask::new(
        Arc::clone(&app_context_arc),
        None,
        None,
    ));

    // 12. 创建 PsnArchiveScPushTask 实例
    let push_archive_sc_task = Arc::new(PsnArchiveScPushTask::new(
        Arc::clone(&app_context_arc),
        None,
        None,
    ));

    // --- 将需要串行执行的任务打包进 Vec ---
    let composite_tasks: Vec<Arc<dyn TaskExecutor + Send + Sync + 'static>> = vec![
        push_train_task,
        push_lecturer_task,
        push_archive_task,
        push_training_task,
        push_train_sc_task,
        push_lecturer_sc_task,
        push_archive_sc_task,
        push_training_sc_task,
    ];
    // --- 创建 CompositeTask 实例 ---
    let main_scheduled_composite_task = Arc::new(CompositeTask::new(
        composite_tasks,
        "培训班数据归档到MSS定时任务".to_string(),
    ));

    // 13. 使用辅助函数创建并添加 CompositeTask 的 Cron Job
    create_and_schedule_task_job(
        &scheduler,
        main_scheduled_composite_task, // Arc<CompositeTask> 会自动转换为 Arc<dyn TaskExecutor>
        app_config.tasks.psn_push.cron_schedule.as_str(),
        vec![], // 作为依赖任务传入
    )
    .await?;

    // 14. 在后台启动调度器，这样它就不会阻塞 Web 服务器的启动
    tokio::spawn(async move {
        // 显式处理 scheduler.start().await 的 Result
        if let Err(e) = scheduler.start().await {
            error!("Failed to start scheduler in background: {e:?}");
            // 这里你可以选择根据错误类型执行更多操作，例如尝试重新启动或记录更详细的错误信息
        } else {
            info!("Scheduler successfully started in background.");
        }
    });

    // 15.启动 Web 服务器
    let server = WebServer::new(app_config.web_server_port, Arc::clone(&app_context_arc));
    server.start().await.context("Failed to start web server")?;

    info!("Application shut down cleanly.");

    Ok(())
}

// 这个函数接收一个实现了 TaskExecutor trait 的任务实例
// 辅助函数：创建并调度一个任务的 Cron Job
async fn create_and_schedule_task_job(
    scheduler: &JobScheduler,
    primary_task: Arc<dyn TaskExecutor + Send + Sync + 'static>, // 主任务
    cron_schedule: &str,
    dependent_tasks: Vec<Arc<dyn TaskExecutor + Send + Sync + 'static>>, // 依赖任务
) -> Result<()> {
    let primary_task_clone = Arc::clone(&primary_task);
    let job_name = primary_task_clone.name().to_string();

    // 克隆依赖任务（如果存在）给 Job 闭包
    let dependent_tasks_clone_for_job = dependent_tasks.clone();

    let job = Job::new_async_tz(
        cron_schedule,
        chrono_tz::Asia::Shanghai,
        move |uuid, mut scheduler| {
            let primary_task_for_future = Arc::clone(&primary_task_clone);
            let job_name_for_future = primary_task_for_future.name().to_string();
            // 克隆依赖任务给 inner async block
            let dependent_tasks_for_future = dependent_tasks_clone_for_job.clone();

            Box::pin(async move {
                let next_scheduled_time_str = match scheduler.next_tick_for_job(uuid).await {
                    Ok(Some(dt)) => dt
                        .with_timezone(&Local)
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string(),
                    Ok(None) => "No next tick".to_string(),
                    Err(e) => {
                        error!("Error getting next tick for job {uuid:?}: {e:?}");
                        "Error getting next tick".to_string()
                    }
                };
                info!(
                    "Job '{job_name_for_future}' ({uuid:?}) is running. Next scheduled time (local): {next_scheduled_time_str}"
                );

                // --- 执行主任务 ---
                if let Err(e) = primary_task_for_future.execute().await {
                    error!(
                        "Error executing primary job '{job_name_for_future}' {uuid:?}: {e:?}"
                    );
                } else {
                    info!(
                        "Primary job '{job_name_for_future}' ({uuid:?}) completed successfully."
                    );
                    // --- 遍历并执行所有依赖任务 ---
                    if dependent_tasks_for_future.is_empty() {
                        info!(
                            "No dependent tasks to execute for '{job_name_for_future}'."
                        );
                    } else {
                        info!(
                            "Starting {} dependent tasks for '{job_name_for_future}'.",
                            dependent_tasks_for_future.len()
                        );
                        for (i, dep_task) in dependent_tasks_for_future.iter().enumerate() {
                            info!(
                                "Executing dependent task #{} for '{job_name_for_future}'.",
                                i + 1
                            );
                            // 依赖任务本身会打印其执行状态的日志
                            if let Err(e) = dep_task.execute().await {
                                error!(
                                    "Error executing dependent task #{} for '{job_name_for_future}': {e:?}",
                                    i + 1
                                );
                                // 如果某个依赖任务失败，你可以选择是中断后续依赖任务，还是继续
                                // 这里我们选择继续执行其他依赖任务，但会记录错误
                            } else {
                                info!(
                                    "Dependent task #{} for '{job_name_for_future}' completed successfully.",
                                    i + 1
                                );
                            }
                        }
                    }
                }
            })
        },
    )
    .context(format!("Failed to create cron job '{job_name}'"))?;

    scheduler
        .add(job)
        .await
        .context(format!("Failed to add job '{job_name}' to scheduler"))?;
    info!("Job '{job_name}' added to scheduler.");
    Ok(())
}
