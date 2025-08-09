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
        // 1. Create a vector of futures. Each future represents an async operation.
        // 创建 Futures: self.clients.iter().map(|(addr, ck_pool)| async move { ... }).collect() 这一步会立即创建出一个 Vec，其中包含了所有节点的查询任务，但这些任务此时都还没有被执行。它们是被称为 "future" 的惰性异步任务。
        let futures: Vec<_> = self
            .clients
            .iter()
            .map(|(addr, ck_pool)| async move {
                match ck_pool.get_handle().await {
                    Ok(mut client) => {
                        info!("Executing query on ClickHouse node: {addr}");
                        if let Err(e) = client.execute(sql).await {
                            error!("Failed to execute query on {addr}: {e:?}");
                            false
                        } else {
                            info!("Query executed successfully on: {addr}");
                            true
                        }
                    }
                    Err(e) => {
                        error!("Failed to get connection handle for {addr}: {e:?}");
                        false
                    }
                }
            })
            .collect::<Vec<_>>();
        // 2. Use a library function to await all the futures concurrently.
        // 并发执行: futures::future::join_all(futures).await 会将这些 future 都提交给 Tokio 运行时并发执行。运行时会同时处理所有任务，当一个任务在等待 I/O（例如网络请求）时，运行时会切换到另一个任务，而不是闲置等待。
        // 等待所有完成: join_all 会一直等待，直到所有 future 都执行完成并返回结果，然后将所有结果收集到一个 Vec 中。
        let results: Vec<bool> = futures::future::join_all(futures).await;

        // 3. Check if all results are true.
        if results.iter().all(|&res| res) {
            info!("All ClickHouse nodes executed the query successfully.");
        } else {
            error!("Some ClickHouse nodes failed to execute the query.");
        }
    }
}
