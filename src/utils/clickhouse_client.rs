use anyhow::Result;
use std::sync::Arc;
use tracing::{error, info};

use crate::ClickhouseConfig;
use clickhouse_rs::Pool;
use futures::future;

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

    /// 这里的实现会尝试在每个客户端上执行查询
    pub async fn execute_on_all_nodes(&self, sql: &str) {
        let mut tasks = Vec::new();
        // 收集所有节点的查询任务
        for (addr, pool_arc) in self.clients.iter() {
            let sql_clone = sql.to_string(); // 克隆 SQL 字符串，供 spawn 任务使用
            let addr_clone = addr.clone(); // 克隆地址，供 spawn 任务中的日志使用
            let pool_arc_clone = pool_arc.clone(); // 克隆 Arc，共享 Pool 的所有权

            // 使用 tokio::spawn 创建一个独立的异步任务，它将在后台执行，并返回一个 JoinHandle
            tasks.push(tokio::spawn(async move {
                // 这个异步块内部的任何 Panic 都会被 tokio 捕获并转换为 JoinError
                let operation_result = match pool_arc_clone.get_handle().await {
                    Ok(mut client_handle) => {
                        info!(
                            "Successfully obtained ClickHouse Node {} connection handle.",
                            addr_clone
                        );
                        // 尝试执行查询
                        if let Err(e) = client_handle.execute(&sql_clone).await {
                            error!("Failed to execute query on {}: {:?}", addr_clone, e);
                            false // 操作失败
                        } else {
                            info!("Successfully executed query on {}.", addr_clone);
                            true // 操作成功
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to obtain ClickHouse Node {} connection handle: {:?}",
                            addr_clone, e
                        );
                        false // 操作失败
                    }
                };
                // 返回节点的地址和操作结果（成功或失败）
                (addr_clone, operation_result)
            }));
        }

        if tasks.is_empty() {
            info!("No available ClickHouse nodes for query.");
            return; // 直接返回 ()
        }

        // <--- 使用 join_all 并发等待所有任务完成
        // join_all 返回一个 Vec<Result<(String, bool), JoinError>>
        let results = future::join_all(tasks).await;

        let mut overall_success = true; // 用于标记整体操作是否完全成功

        // 遍历所有任务的结果并记录日志
        for task_result in results {
            match task_result {
                Ok((_, operation_result)) => {
                    if !operation_result {
                        overall_success = false;
                    }
                }
                Err(join_error) => {
                    // 这里的 Err 表示由 tokio::spawn 捕获的异步任务内部的 Panic
                    error!("A ClickHouse operation task panicked: {:?}", join_error);
                    overall_success = false; // 标记整体操作失败
                }
            }
        }
        if overall_success {
            info!("Query executed successfully on all ClickHouse nodes.");
        } else {
            error!("Query execution failed on some ClickHouse nodes (please check the above logs for details).");
        }
    }
}
