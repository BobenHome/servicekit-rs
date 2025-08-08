use anyhow::Result;
use std::sync::Arc;
use tracing::{error, info};

use clickhouse_rs::Pool;

use crate::ClickhouseConfig;
/// 封装 ClickHouse 客户端，支持连接到多个节点和端口。
pub struct ClickHouseClient {
    // 存储多个 ClickHouse 客户端实例，每个实例对应一个 host:port 组合
    clients: Vec<(String, Arc<Pool>)>,
}

impl ClickHouseClient {
    pub fn new(config: Arc<ClickhouseConfig>) -> Result<Self> {
        let mut clients = Vec::new();

        for host in &config.hosts {
            for port in &config.ports {
                let url = format!(
                    "tcp://{user}:{pass}@{host}:{port}/{db}?compression=lz4",
                    user = config.user,
                    pass = config.password,
                    host = host,
                    port = port,
                    db = config.database,
                );
                info!("Initializing ClickHouse client for: {url}");
                let ck_pool = Pool::new(url);
                clients.push((format!("{host}:{port}"), Arc::new(ck_pool)));
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

        for (addr, ck_pool) in &self.clients {
            match ck_pool.get_handle().await {
                Ok(mut client) => {
                    info!("Executing query on ClickHouse node: {addr}");
                    if let Err(e) = client.execute(sql).await {
                        error!("Failed to execute query on {addr}: {e:?}");
                        all_success = false;
                    } else {
                        info!("Query executed successfully on: {addr}");
                    }
                }
                Err(e) => {
                    error!("Failed to get connection handle for {addr}: {e:?}");
                    all_success = false;
                }
            }
        }

        if all_success {
            info!("All ClickHouse nodes executed the query successfully.");
        } else {
            error!("Some ClickHouse nodes failed to execute the query.");
        }
    }
}
