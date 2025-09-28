use anyhow::{Context, Result};
use servicekit::{logging, schedule::TaskSchedulerManager, AppConfig, AppContext, WebServer};
//servicekit是crate 名称（在 Cargo.toml 中定义），代表了库。logging,  WebServer 这些都是从 lib.rs 中 pub use 或 pub mod 导出的项。如果 lib.rs 不存在或者没有正确地导出这些模块，main.rs 将无法直接通过 servicekit:: 路径来访问它们
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    info!("Application starting...");
    // 1. 初始化日志系统
    // 主线程需持有guard，不然guard会在init_logging调用完后drop掉导致 worker 线程立即停止（不会写日志到文件中）
    let _guard = logging::init_logging().context("Failed to initialize logging")?;

    // 2. 加载应用程序配置
    let app_config = AppConfig::new().context("Failed to load application configuration")?;
    info!("Application configuration loaded successfully: {app_config:?}");

    // 3. 创建AppContext实例
    let app_context = AppContext::new(
        &app_config.database_url,
        Arc::clone(&app_config.mss_info_config),
        Arc::clone(&app_config.telecom_config),
        Arc::clone(&app_config.clickhouse_config),
        Arc::clone(&app_config.redis_config),
        app_config.provinces,
    )
    .await?;
    let app_context_arc = Arc::new(app_context);

    // 4. 初始化和启动任务调度器
    let scheduler = TaskSchedulerManager::new().await?;
    scheduler
        .initialize_tasks(Arc::clone(&app_context_arc), &app_config.tasks)
        .await?;
    scheduler.start().await;

    // 5.启动 Web 服务器
    let server = WebServer::new(app_config.web_server_port, Arc::clone(&app_context_arc));
    server.start().await.context("Failed to start web server")?;

    info!("Application shut down cleanly.");

    Ok(())
}
