use actix_web::{App, HttpServer, middleware};

pub struct WebServer {
    port: u16,
}

impl WebServer {
    pub fn new(port: u16) -> Self {
        WebServer { port }
    }

    pub async fn start(&self) -> std::io::Result<()> {
        println!("Starting web server on port {}", self.port);
        
        HttpServer::new(|| {
            App::new()
                .wrap(middleware::Logger::default())
                .wrap(middleware::Compress::default())
        })
        .bind(("127.0.0.1", self.port))?
        .run()
        .await
    }
}
