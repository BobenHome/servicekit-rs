use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use redis::Script;
use std::time::Duration;
use uuid::Uuid;

pub type RedisMgr = ConnectionManager;

/// 初始化 Redis ConnectionManager（在程序启动时调用一次并放到共享 state）
pub async fn init_redis(redis_url: &str) -> Result<RedisMgr> {
    let client = redis::Client::open(redis_url).context("failed to open redis client")?;
    // 取得 ConnectionManager（需要 redis 开启 feature connection-manager）
    let mgr = client
        .get_connection_manager()
        .await
        .context("failed to get redis connection manager")?;
    Ok(mgr)
}

pub async fn set_kv(mgr: &RedisMgr, key: &str, val: &str, ttl_sec: Option<u64>) -> Result<()> {
    let mut conn = mgr.clone();
    if let Some(sec) = ttl_sec {
        let _res: String = redis::cmd("SET")
            .arg(key)
            .arg(val)
            .arg("EX")
            .arg(sec)
            .query_async(&mut conn)
            .await
            .context("redis SET with EX failed")?; // 明确指明从 Redis 返回 String（SET 返回 "OK"）
    } else {
        let _unit: () = conn.set(key, val).await.context("redis SET failed")?; // 明确把 conn.set 的结果视为 unit `()`
    }
    Ok(())
}

pub async fn get_kv(mgr: &RedisMgr, key: &str) -> Result<Option<String>> {
    let mut conn = mgr.clone();
    let v: Option<String> = conn.get(key).await?;
    Ok(v)
}

/// 分布式锁的实现（返回 token，调用者持有 token 用于释放）
pub struct RedisLock {
    pub key: String,
    pub token: String,
}

impl RedisLock {
    /// 尝试获取锁（一次性尝试），返回 Some(RedisLock) 表示获取成功
    /// 使用 SET key token PX ttl_ms NX
    pub async fn try_acquire(mgr: &RedisMgr, key: &str, ttl_ms: u64) -> Result<Option<RedisLock>> {
        let token = Uuid::new_v4().to_string();
        let mut conn = mgr.clone();

        // 使用原生命令，SET <key> <token> PX <ttl> NX
        // 返回 OK 表示成功，否则为 Nil
        let resp: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg(&token)
            .arg("PX")
            .arg(ttl_ms)
            .arg("NX")
            .query_async(&mut conn)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        if resp.as_deref() == Some("OK") {
            Ok(Some(RedisLock {
                key: key.to_string(),
                token,
            }))
        } else {
            Ok(None)
        }
    }

    /// 带重试和超时的获取（可选用）
    pub async fn acquire_with_retry(
        mgr: &RedisMgr,
        key: &str,
        ttl_ms: u64,
        try_timeout: Duration,
        retry_interval: Duration,
    ) -> Result<Option<RedisLock>> {
        let deadline = tokio::time::Instant::now() + try_timeout;
        while tokio::time::Instant::now() < deadline {
            if let Some(lock) = Self::try_acquire(mgr, key, ttl_ms).await? {
                return Ok(Some(lock));
            }
            tokio::time::sleep(retry_interval).await;
        }
        Ok(None)
    }

    /// 安全释放：只有 token 匹配时才删除（用 Lua 原子脚本）
    pub async fn release(self, mgr: &RedisMgr) -> Result<bool> {
        // Lua 脚本（标准做法）：
        // if redis.call("get",KEYS[1]) == ARGV[1] then return redis.call("del",KEYS[1]) else return 0 end
        const RELEASE_SCRIPT: &str = r#"
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
        "#;

        let mut conn = mgr.clone();
        let script = Script::new(RELEASE_SCRIPT);
        // 返回值是删除的数量（1 成功；0 失败）
        let deleted: i32 = script
            .key(&self.key)
            .arg(&self.token)
            .invoke_async(&mut conn)
            .await?;
        Ok(deleted == 1)
    }
}
