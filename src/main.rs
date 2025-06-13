use servicekit::{
    db::pool,
    schedule::{PsntrainPushTask, Scheduler},
    WebServer,
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // 初始化日志
    env_logger::init();

    // 创建数据库连接池
    let pool = pool::create_pool("mysql://dxxy:dreamsoft@2020@172.25.1.65:33063/dxxy")
        .await
        .expect("Failed to create database pool");

    // 启动调度器
    let mut scheduler = Scheduler::new();
    scheduler.add_task(PsntrainPushTask { pool: pool.clone() });
    tokio::spawn(async move {
        scheduler.start().await;
    });

    // 启动 Web 服务器
    let server = WebServer::new(8084, pool);
    server.start().await
}
