use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sqlx::{MySqlPool, Row};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::utils::redis::{RedisLock, RedisMgr};
use crate::{AppContext, TaskExecutor};

// 定义常量
const BINLOG_SYNC_LOCK_KEY: &str = "binlog:sync:lock";

// 定义binlog类型枚举
/// 数据类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataType {
    /// 基准岗位
    StandardStation,
    /// 机构
    Org,
    /// 用户
    User,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResultSet {
    pub page: Page,
    pub items: Option<Vec<ModifyOperationLog>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Page {
    pub current_page: u32,
    pub page_size: u32,
    #[serde(skip_serializing)]
    pub total_size: u32,
    #[serde(skip_serializing)]
    pub total_page: u32,
    #[serde(skip_serializing)]
    pub limit: u32,
    #[serde(skip_serializing)]
    pub start: u32,
}

impl Page {
    pub fn new(current_page: u32, page_size: u32) -> Self {
        Self {
            current_page,
            page_size,
            total_size: 0,
            total_page: 0,
            limit: 0,
            start: 0,
        }
    }

    pub fn has_next_page(&self) -> bool {
        self.current_page < self.total_page
    }

    pub fn next_page(&self) -> Self {
        Self {
            current_page: self.current_page + 1,
            page_size: self.page_size,
            total_size: self.total_size,
            total_page: self.total_page,
            limit: self.limit,
            start: self.start,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModifyOperationLog {
    pub id: String,
    pub app_id: u32,
    pub domain: String,
    pub model: String,
    pub operation: String,
    pub cid: Option<String>,
    pub rid: Option<String>,
    #[serde(rename = "type")]
    pub type_: u8,
    pub data_modify_time: i64,
    #[serde(rename = "entityMetaInfo")]
    pub entity_meta_info: EntityMetaInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityMetaInfo {
    #[serde(rename = "dateCreated")]
    pub date_created: Option<i64>,
}

pub struct BinlogSyncTimestampHolder {
    mysql_pool: MySqlPool,
    redis_mgr: RedisMgr,
    /// 如果成功获取锁就把 RedisLock 放到这里，save_timestamp 会读取并释放它
    lock_holder: Mutex<Option<RedisLock>>,
}

impl BinlogSyncTimestampHolder {
    pub fn new(mysql_pool: MySqlPool, redis_mgr: RedisMgr) -> Self {
        Self {
            mysql_pool,
            redis_mgr,
            lock_holder: Mutex::new(None),
        }
    }
    async fn get_timestamp(&self) -> Result<i64> {
        // 尝试获取锁
        match RedisLock::try_acquire(
            &self.redis_mgr,
            BINLOG_SYNC_LOCK_KEY,
            14400_000, // 14400s
        )
        .await?
        {
            Some(lock) => {
                // 成功获取锁：把 lock 放到 holder 中，供 save_timestamp 释放
                {
                    let mut guard = self.lock_holder.lock().await;
                    *guard = Some(lock);
                }

                let row = sqlx::query("SELECT timestamp FROM binlog_sync_timestamp")
                    .fetch_one(&self.mysql_pool)
                    .await
                    .context("Failed to get timestamp")?;

                Ok(row.get("timestamp"))
            }
            None => {
                // 获取锁失败 -> 按你的需求直接返回
                warn!("Did not acquire redis lock for binlog timestamp holder; skipping.");
                Ok(-1)
            }
        }
    }

    async fn save_timestamp(&self, timestamp: i64) -> Result<()> {
        sqlx::query("UPDATE binlog_sync_timestamp SET timestamp = ?")
            .bind(timestamp)
            .execute(&self.mysql_pool)
            .await
            .context("Failed to update timestamp")?;

        info!("Updated timestamp to {}", timestamp);

        // 从 holder 中取出锁并释放（如果有）
        let opt_lock = {
            let mut guard = self.lock_holder.lock().await;
            guard.take()
        };

        if let Some(lock) = opt_lock {
            match lock.release(&self.redis_mgr).await {
                Ok(true) => {
                    info!("Successfully released redis lock for binlog timestamp holder");
                }
                Ok(false) => {
                    warn!("Redis lock release returned false (lock not deleted) — it may have expired or been replaced");
                }
                Err(e) => {
                    error!("Failed to release redis lock: {:?}", e);
                }
            }
        } else {
            // 没有锁：可能之前根本没获取到，或者已经释放
            info!("No redis lock found when saving timestamp (nothing to release).");
        }
        Ok(())
    }
}

pub struct BinlogSyncTask {
    app_context: Arc<AppContext>,
    timestamp_holder: BinlogSyncTimestampHolder,
}

impl BinlogSyncTask {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        let timestamp_holder = BinlogSyncTimestampHolder::new(
            app_context.mysql_pool.clone(),
            app_context.redis_mgr.clone(),
        );
        Self {
            app_context,
            timestamp_holder,
        }
    }

    pub async fn sync_data(&self) -> Result<()> {
        let timestamp = self.timestamp_holder.get_timestamp().await?;
        info!("Current timestamp: {}", timestamp);
        if -1 == timestamp {
            return Ok(());
        }
        let start_time = timestamp - 30_000; // 30 秒前
        let end_time = std::cmp::min(
            timestamp + 300_000,                   // 5 分钟后
            chrono::Utc::now().timestamp_millis(), // 时间戳全球统一不区分时区
        );
        for data_type in &[DataType::User, DataType::Org] {
            let mut current_page = None;
            loop {
                match self
                    .app_context
                    .gateway_client
                    .binlog_find(*data_type, start_time, end_time, current_page)
                    .await?
                {
                    Some(result_set) => {
                        // 处理当前页的数据
                        if let Some(items) = result_set.items {
                            // 处理日志项
                            for log in items {
                                // 处理每个 ModifyOperationLog
                                info!("Processing log: {:?}", log);
                            }
                        }
                        // 检查是否还有下一页
                        if !result_set.page.has_next_page() {
                            break;
                        }
                        current_page = Some(result_set.page.next_page());
                    }
                    None => break,
                }
            }
        }
        // 更新时间戳
        self.timestamp_holder.save_timestamp(end_time).await?;
        Ok(())
    }
}

impl TaskExecutor for BinlogSyncTask {
    fn execute(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            info!("Starting binlog sync task...");
            // Execute User sync
            if let Err(e) = self.sync_data().await {
                error!("Failed to sync organization data: {}", e);
            }
            info!("Binlog sync task completed.");
            Ok(())
        })
    }
}
