use async_trait::async_trait;
use servicekit::{db, scheduler::Scheduler, sql, PsnClass, MySchedule, WebServer};
use sqlx::MySqlPool;

// 示例定时任务
struct ExampleTask {
    pool: MySqlPool,
}

#[async_trait]
impl MySchedule for ExampleTask {
    async fn execute(&self) {
        println!("Running scheduled task at: {}", chrono::Local::now());

        // 加载并执行 SQL
        let sql = sql::load_sql("train_query");
        match sqlx::query_as::<_, PsnClass>(&sql)
            .bind("2023-11-22") // 绑定参数
            .fetch_all(&self.pool)
            .await
        {
            Ok(orgs) => {
                for org in orgs {
                    println!("Found orgs: {:?}", org);
                }
            }
            Err(e) => eprintln!("Database error: {}", e),
        }
    }

    fn cron_expression(&self) -> &str {
        "0 */10 * * * * *" // 每分钟执行一次
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // 初始化日志
    env_logger::init();

    // 创建数据库连接池
    let pool = db::create_pool("mysql://dxxy:dreamsoft@2020@172.25.2.87:3306/dxxy")
        .await
        .expect("Failed to create database pool");

    // 启动调度器
    let mut scheduler = Scheduler::new();
    scheduler.add_task(ExampleTask { pool });
    tokio::spawn(async move {
        scheduler.start().await;
    });

    // 启动 Web 服务器
    let server = WebServer::new(8084);
    server.start().await
}
