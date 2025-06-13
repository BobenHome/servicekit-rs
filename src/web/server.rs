use actix_web::{web, App, HttpServer, middleware};
use sqlx::MySqlPool;
use crate::web::handlers;

pub struct WebServer {
    port: u16,
    pool: MySqlPool,
}

impl WebServer {
    pub fn new(port: u16, pool: MySqlPool) -> Self {
        WebServer { port, pool }
    }

    pub async fn start(&self) -> std::io::Result<()> {
        println!("Starting web server on port {}", self.port);
        
        let pool = self.pool.clone();
        
        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(pool.clone()))
                .wrap(middleware::Logger::default())
                .wrap(middleware::Compress::default())
                .service(
                    web::scope("/api")
                        .service(handlers::get_trains)
                        .service(handlers::get_train_by_id)
                )
        })
        .bind(("127.0.0.1", self.port))?
        .run()
        .await
    }
}
