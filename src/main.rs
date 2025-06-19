use anyhow::{Context, Result};
use chrono::Local;
use env_logger::Builder;
use log::{error, info, LevelFilter};
//servicekit是crate 名称（在 Cargo.toml 中定义），代表了库。db::pool, schedule::PsntrainPushTask, WebServer 这些都是从 lib.rs 中 pub use 或 pub mod 导出的项。如果 lib.rs 不存在或者没有正确地导出这些模块，main.rs 将无法直接通过 servicekit:: 路径来访问它们
use servicekit::{
    db::pool,
    schedule::{psntrain_push::MssInfoConfig, PsnTrainPushTask},
    WebServer,
};
use std::io::Write; // 导入 Write trait，用于 format_timestamp 函数，writeln! 宏所需要的 trait
use std::sync::Arc;
use tokio_cron_scheduler::{Job, JobScheduler}; // 导入日志宏 // 导入 Local, Utc, TimeZone

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    Builder::new()
        .filter_level(LevelFilter::Info) // 设置日志级别为 Info
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] {}:{} - {}", // 格式：[本地时间] [级别] 模块::方法:行号 - 消息
                Local::now().format("%Y-%m-%d %H:%M:%S%.3f"), // 精确到毫秒的本地时间
                record.level(),
                record.module_path().unwrap_or("unknown"), // 获取模块路径
                record.line().unwrap_or(0),                // 获取行号
                record.args()                              // 实际的日志消息
            )
        })
        .init(); // 初始化 env_logger
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

    // 定义第三方服务的配置
    let mss_info_config = MssInfoConfig::new(
        "http://10.141.134.30:12500/serviceAgent/rest/hrapi/HrTrainInfo/pushTrainingInfo".to_string(), // 你的实际 URL
        "c17eb77644576d28251383c9fc25124d".to_string(),                      // 你的实际 APP_ID
        "bf1685e2184903789d0be9a0f2c8b91f".to_string(),                     // 你的实际 APP_KEY
    );

    // 2. 创建你的任务实例，并用 Arc 包裹以便安全地在多线程间共享
    let task = Arc::new(PsnTrainPushTask::new(pool.clone(), mss_info_config));

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
            // 获取下一次执行时间
            let next_tick_result = l.next_tick_for_job(uuid).await;
            // 将 UTC 时间转换为本地时间
            let next_scheduled_time_str = match next_tick_result {
                Ok(Some(utc_time)) => {
                    // 使用 .with_timezone(&Local) 进行转换
                    // 如果是 Option<DateTime<Utc>>，你需要先解包 Some，再转换
                    let local_time = utc_time.with_timezone(&Local);
                    // 格式化为字符串，只包含你想要显示的部分
                    format!("{}", local_time.format("%Y-%m-%d %H:%M:%S"))
                    // format!("{}", local_time.format("%Y-%m-%d %H:%M:%S%z")) // 包含时区偏移 +08:00
                }
                Ok(None) => "N/A (No next tick)".to_string(), // 没有下一次调度时间
                Err(e) => {
                    error!("Error getting next tick for job {:?}: {:?}", uuid, e);
                    "Error getting next tick".to_string() // 出错时显示错误信息
                }
            };
            info!(
                "Job {:?} is running. Next scheduled time (local): {:?}",
                uuid,
                next_scheduled_time_str // 使用转换后的本地时间
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
