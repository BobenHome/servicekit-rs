use anyhow::{Context, Result};
use futures::FutureExt; // 引入 catch_unwind
use serde::{Deserialize, Serialize};
use sqlx::{MySqlPool, Row};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::binlog::{OrgDataProcessor, UserDataProcessor};
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
    pub entity_meta_info: Option<EntityMetaInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityMetaInfo {
    #[serde(rename = "dateCreated")]
    pub date_created: Option<i64>,
    #[serde(rename = "dateLastModified")]
    pub date_last_modified: Option<i64>,
}

// 用于记录永久失败的日志和原因
pub struct PermanentFailure {
    pub log: ModifyOperationLog,
    pub reason: String,
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

    /// 获取锁
    async fn acquire_lock(&self) -> Result<bool> {
        // 设置1小时后锁失效，4小时太长
        match RedisLock::try_acquire(&self.redis_mgr, BINLOG_SYNC_LOCK_KEY, 3_600_000).await? {
            Some(lock) => {
                // 成功获取锁，将lock存入 holder，在以后释放
                let mut guard = self.lock_holder.lock().await;
                *guard = Some(lock);
                info!("Successfully acquired redis lock for binlog timestamp holder.");
                Ok(true)
            }
            None => {
                // 获取锁失败
                warn!("Did not acquire redis lock for binlog timestamp holder; skipping.");
                Ok(false)
            }
        }
    }
    async fn get_timestamp(&self) -> Result<i64> {
        let row = sqlx::query("SELECT timestamp FROM binlog_sync_timestamp")
            .fetch_one(&self.mysql_pool)
            .await
            .context("Failed to get timestamp")?;

        Ok(row.get("timestamp"))
    }

    async fn save_timestamp(&self, timestamp: i64) -> Result<()> {
        sqlx::query("UPDATE binlog_sync_timestamp SET timestamp = ?")
            .bind(timestamp)
            .execute(&self.mysql_pool)
            .await
            .context("Failed to update timestamp")?;

        info!("Updated timestamp to {timestamp}");
        Ok(())
    }

    /// 释放锁
    async fn release_lock(&self) -> Result<()> {
        let opt_lock = {
            let mut guard = self.lock_holder.lock().await;
            guard.take()
        };

        if let Some(lock) = opt_lock {
            info!("Releasing redis lock successfully.");
            if let Err(e) = lock.release(&self.redis_mgr).await {
                error!("Failed to release redis lock during error recovery: {e:?}");
            }
        }
        Ok(())
    }

    /// "受保护的作用域执行"
    /// 接收一个异步闭包，安全地执行它，并确保锁总是被释放。
    pub async fn run_scoped_sync<F, Fut>(&self, operation: F) -> Result<()>
    where
        // 闭包接收 i64 (start_time)，返回一个 Future
        F: FnOnce(i64) -> Fut,
        // Future 的输出是 Result<i64>，其中 i64 是新的 end_time
        Fut: Future<Output = Result<i64>>,
    {
        // 1. 先尝试获取锁
        if !self.acquire_lock().await? {
            // 如果获取锁失败，直接返回，不再执行后续逻辑
            warn!("Current task acquire lock is not acquired.");
            return Ok(());
        }

        // 2. 将所有获取锁之后的操作，全部放入一个新的 async 块中
        let protected_logic = async {
            // 2.1. 在安全区域内获取时间戳
            let start_timestamp = self.get_timestamp().await?;
            // 2.2. 执行传入的业务逻辑
            let end_time = operation(start_timestamp).await?;
            self.save_timestamp(end_time).await?;
            Ok(()) // 如果所有步骤都成功，返回 Ok
        };

        // 3. 将业务逻辑（Future）包装在 AssertUnwindSafe 和 catch_unwind 中
        // AssertUnwindSafe 是必要的，因为我们跨越了 panic 边界
        let future_result = AssertUnwindSafe(protected_logic).catch_unwind().await;

        // 4. 无论上面的结果如何（成功、失败或Panic），都无条件执行锁释放
        if let Err(e) = self.release_lock().await {
            // 如果释放锁本身也失败了，只能记录一个错误，但不能阻止后续的错误传递
            error!("CRITICAL: Failed to release lock during cleanup: {e:?}");
        }

        // 4. 根据执行结果进行处理
        match future_result {
            // 1: 所有工作都成功完成
            Ok(Ok(_)) => {
                info!("Scoped operation and cleanup completed successfully.");
                Ok(())
            }
            // 2: 工作中发生了可恢复的错误 (Err)
            Ok(Err(e)) => {
                error!("Scoped operation failed with an error, lock has been released. Propagating error.");
                Err(e) // 将原始错误传递给上层
            }
            // 3: 业务逻辑发生了 Panic
            Err(panic_payload) => {
                error!("Scoped operation panicked! Lock has been released. Resuming unwind.");
                // 重新抛出 Panic
                std::panic::resume_unwind(panic_payload);
            }
        }
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

    /// 辅助函数：为指定的数据类型获取并处理所有 binlog 数据。
    async fn process_data_for_type(
        &self,
        data_type: DataType,
        start_time: i64,
        end_time: i64,
    ) -> Result<()> {
        let mut current_page = None;
        let mut all_items_for_type = Vec::new();

        // 1. 获取当前类型的所有分页数据
        while let Some(result_set) = self
            .app_context
            .gateway_client
            .binlog_find(data_type, start_time, end_time, current_page)
            .await?
        {
            // 处理当前页的数据
            if let Some(mut items) = result_set.items {
                // 处理日志项
                all_items_for_type.append(&mut items);
            }
            // 检查是否还有下一页
            if !result_set.page.has_next_page() {
                break;
            }
            current_page = Some(result_set.page.next_page());
        }

        // 2. 获取完所有数据后，分发给对应的处理器
        if !all_items_for_type.is_empty() {
            let items_len = all_items_for_type.len();
            info!("Retrieved {items_len} records for type {data_type:?}, starting processing...");
            match data_type {
                DataType::Org => {
                    let org_processor = OrgDataProcessor::new(self.app_context.clone());
                    // 返回Result，让上层决定如何处理错误
                    org_processor.process_orgs(all_items_for_type).await?;
                }
                DataType::User => {
                    let user_processor = UserDataProcessor::new(self.app_context.clone());
                    user_processor.process_users(all_items_for_type).await?;
                }
                _ => {
                    warn!("Unknown or unsupported DataType for processing: {data_type:?}");
                }
            }
        } else {
            warn!("No results set for type {data_type:?}");
        }
        Ok(())
    }

    pub async fn sync_data(&self) -> Result<()> {
        // 一个业务逻辑的闭包
        let business_logic = |timestamp: i64| async move {
            info!("Executing sync logic with start_timestamp: {}", timestamp);
            let start_time = timestamp - 30_000; // 30 秒前
            let end_time = std::cmp::min(
                timestamp + 300_000,                   // 5 分钟后
                chrono::Utc::now().timestamp_millis(), // 时间戳全球统一不区分时区
            );

            // 1. 为 Org 和 User 分别创建一个异步任务 Future
            let org_processing_future =
                self.process_data_for_type(DataType::Org, start_time, end_time);
            let user_processing_future =
                self.process_data_for_type(DataType::User, start_time, end_time);

            // 2. 使用 tokio::join! 并发地执行这两个 Future
            info!("Starting concurrent processing for Org and User data...");
            let (org_result, user_result) =
                tokio::join!(org_processing_future, user_processing_future);

            // 3. 分别处理两个任务的结果
            //    注意：我们只记录错误，不中断整个同步流程，这与您之前的逻辑一致
            if let Err(e) = org_result {
                error!("Error occurred while processing organization data: {e:?}");
            } else {
                info!("Organization data processing completed.");
            }

            if let Err(e) = user_result {
                error!("Error occurred while processing user data: {e:?}");
            } else {
                info!("User data processing completed.");
            }
            // 业务逻辑成功完成，返回新的时间戳
            Ok(end_time)
        };
        // 调用“受保护的执行”
        self.timestamp_holder.run_scoped_sync(business_logic).await
    }
}

impl TaskExecutor for BinlogSyncTask {
    fn execute(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            info!("Starting binlog sync task.");
            // Execute Binlog sync
            if let Err(e) = self.sync_data().await {
                error!("Failed to sync data: {e:?}");
            }
            info!("Binlog sync task completed.");
            Ok(())
        })
    }
}
