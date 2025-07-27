use crate::{web::handlers, AppConfig};
use actix_web::{middleware, web, App, HttpServer};
use anyhow::{Context, Result}; // 导入 anyhow::Result 和 Context trait
use sqlx::MySqlPool;
use tracing::info;

pub struct WebServer {
    port: u16,
    pool: MySqlPool,
    app_config: AppConfig,
}

impl WebServer {
    pub fn new(pool: MySqlPool, app_config: AppConfig) -> Self {
        WebServer {
            port: app_config.web_server_port,
            pool,
            app_config,
        }
    }

    pub async fn start(&self) -> Result<()> {
        info!("Starting web server on port {}", self.port); // 使用 info! 宏

        let pool = self.pool.clone();
        let app_config = self.app_config.clone();

        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(pool.clone())) // 在每个 worker 线程中克隆一次 pool
                .app_data(web::Data::new(app_config.clone()))
                .wrap(middleware::Logger::default()) // 启用请求日志
                .wrap(middleware::Compress::default()) // 启用响应压缩
                .service(
                    web::scope("/api") // 创建一个 /api 范围
                        .service(handlers::push_msss), // 注册处理函数
                )
        })
        .bind(("127.0.0.1", self.port))
        .context(format!("Failed to bind web server to port {}", self.port))? // 添加上下文信息
        .run()
        .await
        .context("Web server failed to run or shut down unexpectedly")?; // 添加上下文信息
        info!("Web server shut down cleanly.");
        Ok(()) // 返回 Ok(()) 表示服务器成功启动并完成（通常是外部信号关闭）
    }
}
