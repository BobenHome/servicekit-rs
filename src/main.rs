use servicekit::{db::pool, schedule::PsntrainPushTask, WebServer};
use std::sync::Arc;
use tokio_cron_scheduler::{Job, JobScheduler};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // 初始化日志
    env_logger::init();

    // 创建数据库连接池
    let pool = pool::create_pool("mysql://dxxy:dreamsoft@2020@172.25.1.65:33063/dxxy")
        .await
        .expect("Failed to create database pool");

    // --- 使用 tokio-cron-scheduler 启动调度器 ---
    // 1. 创建一个新的 JobScheduler 实例
    let scheduler = JobScheduler::new()
        .await
        .expect("Failed to create scheduler");

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
        let task_clone = Arc::clone(&task);
        Box::pin(async move {
            println!(
                "Job {:?} is running. Next scheduled time: {:?}",
                uuid,
                l.next_tick_for_job(uuid).await
            );

            task_clone.execute().await;
        })
    })
    .expect("Failed to create cron job");

    // 4. 将 Job 添加到调度器
    scheduler
        .add(job)
        .await
        .expect("Failed to add job to scheduler");

    // 5. 在后台启动调度器，这样它就不会阻塞 Web 服务器的启动
    tokio::spawn(async move {
        println!("Scheduler started.");
        scheduler.start().await.expect("Failed to start scheduler");
    });

    // 启动 Web 服务器
    let server = WebServer::new(8084, pool);
    server.start().await
}
