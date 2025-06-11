use async_trait::async_trait;
use servicekit::{db, scheduler::Scheduler, sql, MySchedule, PsnClass, WebServer};
use sqlx::{MySql, MySqlPool, QueryBuilder};

// 示例定时任务
struct ExampleTask {
    pool: MySqlPool,
}

#[async_trait]
impl MySchedule for ExampleTask {
    async fn execute(&self) {
        println!("Running scheduled task at: {}", chrono::Local::now());

        let hit_date = Some("2023-11-22");
        let train_ids = Some(vec![
            "009b6addc3394455a645938b5bc981b7",
            "015dc7e8a2ec46ac958d1713617ffd1e",
        ]);

        // 构建基础SQL
        let sql = sql::load_sql("train_query");

        // 动态构建查询条件
        let mut query_builder = QueryBuilder::<MySql>::new(&sql);

        // 处理 hitDate 条件
        if let Some(date) = hit_date {
            query_builder.push(" AND a.hitdate = ");
            query_builder.push_bind(date);
        }

        // 处理 list 条件
        if let Some(ids) = train_ids {
            if !ids.is_empty() {
                query_builder.push(" AND a.TRAINID IN (");
                let mut separated = query_builder.separated(", ");
                for id in ids {
                    separated.push_bind(id);
                }
                separated.push_unseparated(")");
            }
        }

        // 执行查询
        match query_builder
            .build_query_as::<PsnClass>()
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
