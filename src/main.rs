use servicekit::{WebServer, MySchedule, scheduler::Scheduler};
use async_trait::async_trait;

// 示例定时任务
struct ExampleTask;

#[async_trait]
impl MySchedule for ExampleTask {
    async fn execute(&self) {
        println!("Running scheduled task at: {}", chrono::Local::now());
    }

    fn cron_expression(&self) -> &str {
        "0 */1 * * * * *"  // 每分钟执行一次
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // 初始化日志
    env_logger::init();

    // 启动调度器
    let mut scheduler = Scheduler::new();
    scheduler.add_task(ExampleTask);
    tokio::spawn(async move {
        scheduler.start().await;
    });

    // 启动 Web 服务器
    let server = WebServer::new(8084);
    server.start().await
}
