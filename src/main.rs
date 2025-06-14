use anyhow::{Context, Result};
use log::{error, info};
use servicekit::{db::pool, schedule::PsntrainPushTask, WebServer};
use std::sync::Arc;
use tokio_cron_scheduler::{Job, JobScheduler}; // 导入日志宏

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    env_logger::init();
    info!("Application starting...");

    // 创建数据库连接池
    let pool = pool::create_pool("mysql://dxxy:dreamsoft@2020@172.25.1.65:33063/dxxy")
        .await
        .context("Failed to create database pool")?;

    info!("Database pool created successfully.");

    // --- 使用 tokio-cron-scheduler 启动调度器 ---
    // 1. 创建一个新的 JobScheduler 实例
    let scheduler = JobScheduler::new()
        .await
        .context("Failed to create JobScheduler instance")?; // 添加上下文并传播错误

    info!("JobScheduler instance created.");

    // 2. 创建你的任务实例，并用 Arc 包裹以便安全地在多线程间共享
    let task = Arc::new(PsntrainPushTask { pool: pool.clone() });

    // 3. 使用从 task 中获取的 cron 表达式来创建一个异步 Job
    // 3.1. 调用方法获取 cron 字符串，并立即使用 .to_string()
    //      将其转换为一个拥有所有权的 String。
    //      这样，对 `task` 的借用在这一行代码结束后就真正地、彻底地结束了。
    let cron_str = task.cron_expression().to_string();
    // 3.2. 现在 task 没有被借用，可以安全地 move 进闭包了
    // 3.3. 如果时间不对，那么考虑使用 new_async_tz
    let job = Job::new_async(cron_str, move |uuid, mut l| {
        // 闭包内部的逻辑保持不变，它捕获了 task (Arc) 的所有权
        let task_clone = Arc::clone(&task); // 克隆 Arc 以在闭包内部使用
        Box::pin(async move {
            info!(
                "Job {:?} is running. Next scheduled time: {:?}",
                uuid,
                l.next_tick_for_job(uuid).await
            );

            // 执行任务，并处理可能的错误
            if let Err(e) = task_clone.execute().await {
                error!("Error executing job {:?}: {:?}", uuid, e);
            }
        })
    })
    .context("Failed to create cron job")?; // 添加上下文并传播错误

    info!("Cron job created.");

    // 4. 将 Job 添加到调度器
    scheduler
        .add(job)
        .await
        .context("Failed to add job to scheduler")?; // 添加上下文并传播错误

    info!("Job added to scheduler.");

    // 5. 在后台启动调度器，这样它就不会阻塞 Web 服务器的启动
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

    // 启动 Web 服务器
    let server = WebServer::new(8084, pool);
    server.start().await.context("Failed to start web server")?;

    info!("Web Server started and application running.");

    // main 函数的 Result 成功变体
    Ok(())
}
