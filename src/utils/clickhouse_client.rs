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
    pub fn new(config: &ClickhouseConfig) -> Result<Self> {
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
                info!("Initializing ClickHouse client for: {}", url);
                let pool = Pool::new(url);
                clients.push((format!("{}:{}", host, port), Arc::new(pool)));
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
        let client_pools: Vec<(String, Arc<Pool>)> = self
            .clients
            .iter()
            .map(|(addr, pool_arc)| (addr.clone(), pool_arc.clone()))
            .collect();
        for (addr, pool) in client_pools {
            let sql_clone = sql.to_string(); // 克隆 SQL 字符串，供 spawn 任务使用
            let addr_clone = addr.clone(); // 克隆地址，供 spawn 任务中的日志使用

            let task_result = tokio::spawn(async move {
                // 尝试获取连接句柄
                let mut client_handle = match pool.get_handle().await {
                    Ok(client) => {
                        info!(
                            "Successfully obtained ClickHouse Node {} connection handle",
                            addr_clone
                        );
                        client
                    }
                    Err(e) => {
                        error!(
                            "Failed to obtain ClickHouse Node {} connection handle: {:?}",
                            addr_clone, e
                        );
                        return false;
                    }
                };
                // 尝试执行查询
                if let Err(e) = client_handle.execute(&sql_clone).await {
                    error!("Failed to execute query on {}: {:?}", addr_clone, e);
                    return false;
                } else {
                    info!("Successfully executed query on {}", addr_clone);
                    true
                }
            })
            .await;
            match task_result {
                Ok(inner_success) => {
                    // inner_success 是 spawn 任务返回的布尔值
                    if !inner_success {
                        all_success = false;
                    }
                }
                Err(join_error) => {
                    // 这里的 Err 表示 spawn 任务本身 panic 了
                    error!("ClickHouse 操作任务 {} 发生 panic：{:?}", addr, join_error);
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
