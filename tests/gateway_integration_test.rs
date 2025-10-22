use anyhow::{Context, Result};
use servicekit::{
    logging::LocalTimer, schedule::binlog_sync::BinlogSyncTask, AppConfig, AppContext,
};

use servicekit::context::RedisContext;
use servicekit::utils::redis::{del_kv, get_kv, set_kv, RedisLock, RedisMgr};
use servicekit::utils::MapToProcessError;
use std::sync::{Arc, Once};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::info;
use tracing_subscriber::{self, fmt::TestWriter, FmtSubscriber};

// 定义日志初始化函数
// 使用 Once 确保日志订阅者只设置一次，避免重复初始化错误
// 用于确保日志只初始化一次
static INIT_LOGGING: Once = Once::new();

fn setup_logging_for_tests() {
    INIT_LOGGING.call_once(|| {
        // 构建一个 FmtSubscriber，它会将日志输出到标准错误（被 cargo test 捕获）
        let subscriber = FmtSubscriber::builder()
            .with_writer(TestWriter::default()) // 专门用于测试环境，将日志输出到 `cargo test` 的输出流
            .with_max_level(tracing::Level::DEBUG) // 设置您希望看到的最大日志级别，例如 INFO 或 DEBUG
            .with_line_number(true) // 显示行号
            .with_file(true) // 显示文件名
            .with_timer(LocalTimer)
            .finish();

        // 设置全局默认的日志订阅者
        tracing::subscriber::set_global_default(subscriber)
            .expect("Failed to set tracing subscriber for tests");

        info!("--- Logging initialized for integration tests ---"); // 打印一条初始化成功的消息
    });
}

// #[tokio::test] 宏用于运行异步测试
#[tokio::test]
#[ignore]
async fn test_invoke_gateway_service_real_success() -> Result<()> {
    // 1. 在每个测试函数的开头调用日志初始化
    setup_logging_for_tests();
    // 2. 加载应用程序配置
    let app_config = AppConfig::new().context("Failed to load application configuration")?;

    // 3. 创建AppContext实例
    let app_context = AppContext::new(
        &app_config.database_url,
        Arc::clone(&app_config.mss_info_config),
        Arc::clone(&app_config.telecom_config),
        Arc::clone(&app_config.clickhouse_config),
        Arc::clone(&app_config.redis_config),
        app_config.provinces,
    )
    .await?;
    let app_context_arc = Arc::new(app_context);

    let binlog_sync_task = BinlogSyncTask::new(app_context_arc.clone());
    match binlog_sync_task.sync_data().await.map_gateway_err() {
        Ok(_result) => {}
        Err(err) => {
            panic!("Failed to sync data: {err}");
        }
    }

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_redis_set_kev() -> Result<()> {
    setup_logging_for_tests();
    let app_config = AppConfig::new().context("Failed to load application configuration")?;
    let redis_context = RedisContext::new(Arc::clone(&app_config.redis_config)).await?;

    set_kv(&redis_context.redis_mgr, "name", "apple", Some(60)).await?;
    let redis_value = get_kv(&redis_context.redis_mgr, "name").await?;
    info!("Redis value: {redis_value:?}");
    tokio::time::sleep(Duration::from_millis(10000)).await;
    let del_num = del_kv(&redis_context.redis_mgr, "name").await?;
    info!("Redis deleted del_num: {del_num:?}");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_redislock_concurrent_acquire_and_release() -> Result<()> {
    // 日志、配置、AppContext 初始化（按你项目里已有代码）
    setup_logging_for_tests();
    let app_config = AppConfig::new().context("Failed to load application configuration")?;
    // let app_config_arc = Arc::new(app_config);

    let app_context = AppContext::new(
        &app_config.database_url,
        Arc::clone(&app_config.mss_info_config),
        Arc::clone(&app_config.telecom_config),
        Arc::clone(&app_config.clickhouse_config),
        Arc::clone(&app_config.redis_config),
        app_config.provinces,
    )
    .await?;
    let app_context_arc = Arc::new(app_context);

    let redis_mgr = app_context_arc.redis_mgr.clone();
    let lock_key = "test:binlog:lock";

    // 确保开始前锁键被清理（DEL）
    {
        let mut conn = redis_mgr.clone();
        let _: i32 = redis::cmd("DEL")
            .arg(lock_key)
            .query_async::<i32>(&mut conn)
            .await
            .unwrap_or(0);
    }

    // 第一轮并发：5 个任务同时尝试，成功者持有 300ms
    let got1 = run_concurrent_try_once(redis_mgr.clone(), lock_key, 5, 300).await;
    tracing::info!("Round1 acquired by tasks: {:?}", got1);
    assert_eq!(got1.len(), 1, "Expected exactly 1 holder in round1");

    // 等短时间以确保锁被释放（持有 300ms，给点余量）
    tokio::time::sleep(Duration::from_millis(350)).await;

    // 第二轮并发再试一次，验证锁释放后可被重新获取
    let got2 = run_concurrent_try_once(redis_mgr.clone(), lock_key, 5, 200).await;
    tracing::info!("Round2 acquired by tasks: {got2:?}");
    assert_eq!(got2.len(), 1, "Expected exactly 1 holder in round2");

    // 清理
    {
        let mut conn = redis_mgr.clone();
        let _: i32 = redis::cmd("DEL")
            .arg(lock_key)
            .query_async::<i32>(&mut conn)
            .await
            .unwrap_or(0);
    }

    Ok(())
}

/// 并发尝试若干个任务去拿同一把 RedisLock（只尝试一次），记录拿到锁的 task index。
async fn run_concurrent_try_once(
    redis_mgr: RedisMgr,
    lock_key: &'static str,
    n_tasks: usize,
    hold_ms: u64,
) -> Vec<usize> {
    let successes = Arc::new(tokio::sync::Mutex::new(Vec::<usize>::new()));
    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(n_tasks);

    for i in 0..n_tasks {
        let mgr = redis_mgr.clone();
        let successes = successes.clone();
        let lock_key = lock_key;

        let h = tokio::spawn(async move {
            match RedisLock::try_acquire(&mgr, lock_key, 5_000).await {
                Ok(Some(lock)) => {
                    // 成功：记录并持有一定时间，然后释放（release consumes lock）
                    {
                        let mut g = successes.lock().await;
                        g.push(i);
                    }
                    // 模拟工作
                    tokio::time::sleep(Duration::from_millis(hold_ms)).await;

                    match lock.release(&mgr).await {
                        Ok(true) => {
                            tracing::info!("task {} released lock successfully", i);
                        }
                        Ok(false) => {
                            tracing::warn!("task {} release returned false (maybe expired)", i);
                        }
                        Err(e) => {
                            tracing::error!("task {} failed to release lock: {:?}", i, e);
                        }
                    }
                }
                Ok(None) => {
                    // 没拿到锁：直接退出
                    tracing::debug!("task {} did not acquire lock", i);
                }
                Err(e) => {
                    tracing::error!("task {} acquire error: {:?}", i, e);
                }
            }
        });
        handles.push(h);
    }

    // 等待所有任务结束
    for h in handles {
        let _ = h.await;
    }

    // 取出成功者列表并返回
    let res = {
        let g = successes.lock().await;
        g.clone()
    };
    res
}
