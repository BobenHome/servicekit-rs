use anyhow::{Context, Result};
use chrono::Local;
use log::{error, info};
//servicekit是crate 名称（在 Cargo.toml 中定义），代表了库。db::pool, schedule::PsntrainPushTask, WebServer 这些都是从 lib.rs 中 pub use 或 pub mod 导出的项。如果 lib.rs 不存在或者没有正确地导出这些模块，main.rs 将无法直接通过 servicekit:: 路径来访问它们
use servicekit::config::AppConfig;
use servicekit::logging;
use servicekit::{db::pool, schedule::PsnTrainPushTask, WebServer};
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
    let push_task = Arc::new(PsnTrainPushTask::new(
        pool.clone(),
        app_config.mss_info_config.clone(),
    ));

    // 6. 定义 Cron Job (使用 PsnTrainPushTask 自己的 cron_schedule 配置)
    let job = Job::new_async(
        app_config.tasks.psn_train_push.cron_schedule.as_str(),
        move |uuid, mut scheduler| {
            // <--- 使用具体的任务 cron_schedule
            let task_clone = Arc::clone(&push_task);
            Box::pin(async move {
                info!("Job {:?} is running.", uuid);
                let next_scheduled_time_str = match scheduler.next_tick_for_job(uuid).await {
                    Ok(Some(ts)) => ts
                        .with_timezone(&Local)
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string(),
                    Ok(None) => "No next scheduled time".to_string(),
                    Err(e) => {
                        error!("Error getting next tick for job {:?}: {:?}", uuid, e);
                        "Error getting next tick".to_string()
                    }
                };
                info!(
                    "Job {:?} is running. Next scheduled time (local): {:?}\n",
                    uuid, next_scheduled_time_str
                );
                if let Err(e) = task_clone.execute().await {
                    error!("Error executing job {:?}: {:?}", uuid, e);
                }
            })
        },
    )
    .context("Failed to create cron job")?;

    info!("Cron job created.");

    // 7. 将 Job 添加到调度器
    scheduler
        .add(job)
        .await
        .context("Failed to add job to scheduler")?; // 添加上下文并传播错误

    info!("Job added to scheduler.");

    // 8. 在后台启动调度器，这样它就不会阻塞 Web 服务器的启动
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
