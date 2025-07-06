use anyhow::{Context, Result};
use chrono::Local;
use log::{error, info};
//servicekit是crate 名称（在 Cargo.toml 中定义），代表了库。db::pool, schedule::PsntrainPushTask, WebServer 这些都是从 lib.rs 中 pub use 或 pub mod 导出的项。如果 lib.rs 不存在或者没有正确地导出这些模块，main.rs 将无法直接通过 servicekit:: 路径来访问它们
use servicekit::config::AppConfig;
use servicekit::schedule::PsnLecturerPushTask;
use servicekit::{db::pool, schedule::PsnTrainPushTask, WebServer};
use servicekit::{logging, TaskExecutor};
use std::sync::Arc;
use tokio_cron_scheduler::{Job, JobScheduler};

#[tokio::main]
async fn main() -> Result<()> {
    // 1. 初始化日志系统
    logging::init_logging().context("Failed to initialize logging")?;

    info!("Application starting...");

    // 2. 加载应用程序配置
    let app_config = AppConfig::new().context("Failed to load application configuration")?;
    info!(
        "Application configuration loaded successfully: {:?}",
        app_config
    );

    // 3. 创建数据库连接池 (使用配置中的 database_url)
    let pool = pool::create_pool(&app_config.database_url) // <--- 使用 app_config.database_url
        .await
        .context("Failed to create database connection pool")?;
    info!("Database connection pool created.");

    // 4. 初始化任务调度器
    // --- 使用 tokio-cron-scheduler 启动调度器 ---
    let scheduler = JobScheduler::new()
        .await
        .context("Failed to create scheduler")?;
    info!("Scheduler initialized.");

    // 5. 创建 PsnTrainPushTask 实例
    // 使用从配置中读取的第三方配置
    let push_train_task = Arc::new(PsnTrainPushTask::new(
        pool.clone(),
        app_config.mss_info_config.clone(),
    ));

    // 6. 使用辅助函数创建并添加 PsnTrainPushTask 的 Cron Job
    create_and_schedule_task_job(
        &scheduler,
        push_train_task,
        app_config.tasks.psn_push.cron_schedule.as_str(),
        "PsnTrainPushTask",
    )
    .await?; // 使用 ? 传播错误

    // 7. 创建 PsnLecturerPushTask 实例
    let push_lecturer_task = Arc::new(PsnLecturerPushTask::new(
        pool.clone(),
        app_config.mss_info_config.clone(),
    ));
    // 8. 使用辅助函数创建并添加 PsnLecturerPushTask 的 Cron Job
    create_and_schedule_task_job(
        &scheduler,
        push_lecturer_task,
        app_config.tasks.psn_push.cron_schedule.as_str(),
        "PsnLecturerPushTask",
    )
    .await?; // 使用 ? 传播错误

    // 9. 在后台启动调度器，这样它就不会阻塞 Web 服务器的启动
    tokio::spawn(async move {
        info!("Attempting to start scheduler in background...");
        // 显式处理 scheduler.start().await 的 Result
        if let Err(e) = scheduler.start().await {
            error!("Failed to start scheduler in background: {:?}", e);
            // 这里你可以选择根据错误类型执行更多操作，例如尝试重新启动或记录更详细的错误信息
        } else {
            info!("Scheduler successfully started in background.");
        }
    });

    // 9.启动 Web 服务器
    let server = WebServer::new(app_config.web_server_port, pool);
    server.start().await.context("Failed to start web server")?;

    info!("Application shut down cleanly.");

    // main 函数的 Result 成功变体
    Ok(())
}

// 定义一个通用的辅助函数，用于创建和调度任务 Job
// 这个函数接收一个实现了 TaskExecutor trait 的任务实例
async fn create_and_schedule_task_job<T>(
    scheduler: &JobScheduler,
    task: Arc<T>,
    schedule_str: &str,
    task_name: &str,
) -> Result<()>
where
    T: TaskExecutor, // T 必须实现 TaskExecutor
{
    let job_name_for_context = task_name.to_string(); // 用于在闭包中捕获任务名称
    let job = Job::new_async(schedule_str, move |uuid, mut scheduler_clone| {
        let task_clone = Arc::clone(&task); // 克隆 Arc 以在异步闭包中使用
        let job_name_inner = job_name_for_context.clone(); // 再次克隆任务名称

        Box::pin(async move {
            info!("Job '{}' ({:?}) is running.", job_name_inner, uuid);
            let next_scheduled_time_str = match scheduler_clone.next_tick_for_job(uuid).await {
                Ok(Some(ts)) => ts
                    .with_timezone(&Local)
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string(),
                Ok(None) => "No next scheduled time".to_string(),
                Err(e) => {
                    error!(
                        "Error getting next tick for job '{}' ({:?}): {:?}",
                        job_name_inner, uuid, e
                    );
                    "Error getting next tick".to_string()
                }
            };
            info!(
                "Job '{}' ({:?}) is running. Next scheduled time (local): {:?}\n",
                job_name_inner, uuid, next_scheduled_time_str
            );
            if let Err(e) = task_clone.execute().await {
                // 调用 TaskExecutor trait 的 execute 方法
                error!(
                    "Error executing job '{}' ({:?}) execution: {:?}",
                    job_name_inner, uuid, e
                );
            }
        })
    })
    .context(format!("Failed to create {} cron job", task_name))?; // 更具体的错误上下文

    scheduler
        .add(job)
        .await
        .context(format!("Failed to add {} job to scheduler", task_name))?; // 更具体的错误上下文

    info!("{} cron job created and added to scheduler.", task_name);
    Ok(())
}
