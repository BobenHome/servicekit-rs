use anyhow::Result;
use std::sync::Arc;
use tracing::{error, info};

use clickhouse::Client;

use crate::ClickhouseConfig;
/// 封装 ClickHouse 客户端，支持连接到多个节点和端口。
pub struct ClickHouseClient {
    // 存储多个 ClickHouse 客户端实例，每个实例对应一个 host:port 组合
    clients: Vec<(String, Arc<Client>)>,
}

impl ClickHouseClient {
    pub fn new(config: &ClickhouseConfig) -> Result<Self> {
        let mut clients = Vec::new();
        for host in &config.hosts {
            for port in config.ports.clone() {
                let client_url = format!("tcp://{}:{}", host, port);
                info!("Initializing ClickHouse client for: {}", client_url);
                let client = Client::default()
                    .with_url(client_url)
                    .with_user(&config.user)
                    .with_password(&config.password)
                    .with_database(&config.database);
                clients.push((host.to_string(), Arc::new(client)));
            }
        }
        if clients.is_empty() {
            anyhow::bail!("No ClickHouse hosts or ports configured.");
        }
        Ok(ClickHouseClient { clients })
    }
    /// 在所有配置的 ClickHouse 节点上执行 SQL 查询。
    /// 这里的实现会尝试在每个客户端上执行查询，如果某个客户端失败，会记录错误但继续尝试其他客户端。
    pub async fn execute_on_all_nodes(&self, sql: &str) {
        let mut all_success = true;
        for (host, client_arc) in &self.clients {
            info!("Executing query on ClickHouse node: {}", host);
            match client_arc.query(sql).execute().await {
                Ok(_) => info!("Query executed successfully on node."),
                Err(e) => {
                    error!(
                        "Failed to execute query on ClickHouse node: {}. Error: {:?}",
                        host, e
                    );
                    all_success = false;
                }
            }
        }
        if all_success {
            info!("All ClickHouse nodes executed the query successfully.");
        } else {
            // 如果至少有一个节点执行失败，则返回错误
            error!("Some ClickHouse nodes failed to execute the query.");
        }
    }
}
