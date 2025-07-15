// 请根据您 Cargo.toml 中的 `name` 字段来替换 `servicekit`
use anyhow::{Context, Result};
use serde_json::json;
use servicekit::{utils::gateway_client::GatewayClient, AppConfig};

use std::sync::Once;
use tracing::info;
use tracing_subscriber::{self, fmt::TestWriter, FmtSubscriber}; // 用于确保日志只初始化一次

// 定义日志初始化函数
// 使用 Once 确保日志订阅者只设置一次，避免重复初始化错误
static INIT_LOGGING: Once = Once::new();

fn setup_logging_for_tests() {
    INIT_LOGGING.call_once(|| {
        // 构建一个 FmtSubscriber，它会将日志输出到标准错误（被 cargo test 捕获）
        let subscriber = FmtSubscriber::builder()
            .with_writer(TestWriter::default()) // 专门用于测试环境，将日志输出到 `cargo test` 的输出流
            .with_max_level(tracing::Level::INFO) // 设置您希望看到的最大日志级别，例如 INFO 或 DEBUG
            // .with_line_number(true) // 可选：显示行号
            // .with_file(true) // 可选：显示文件名
            .finish();

        // 设置全局默认的日志订阅者
        tracing::subscriber::set_global_default(subscriber)
            .expect("Failed to set tracing subscriber for tests");

        info!("*** Logging initialized for integration tests ***"); // 打印一条初始化成功的消息
    });
}

// #[tokio::test] 宏用于运行异步测试
#[tokio::test]
async fn test_invoke_gateway_service_real_success() -> Result<()> {
    // 1. 在每个测试函数的开头调用日志初始化
    setup_logging_for_tests();
    // 2. 加载应用程序配置
    let app_config = AppConfig::new().context("Failed to load application configuration")?;
    let telecom_config = &app_config.telecom_config;
    let client = GatewayClient::new(
        telecom_config.gateway_url.clone(),
        telecom_config.source_app_id,
        telecom_config.target_app_id,
        telecom_config.mode,
        telecom_config.is_sync,
    );
    // 3. 准备测试用的 payload 数据
    let test_payload = vec![json!({"111111": 1})];
    // 4. 调用您要测试的方法。它现在会向真实的网关发送请求
    let result = client
        .invoke_gateway_service("bj.bjglinfo.gettrainstatusbyid", test_payload)
        .await;
    // 5. 断言结果
    assert!(
        result.is_ok(),
        "调用真实网关服务预期成功，但发生了错误: {:?}",
        result.err()
    );
    let reply = result.unwrap();
    assert_eq!(
        reply.header.message_code, 10000,
        "预期 message_code 为 10000，但实际为 {}",
        reply.header.message_code
    );
    assert_eq!(
        reply.header.description, "success",
        "预期 description 为 'success'，但实际为 {}",
        reply.header.description
    );

    Ok(())
}
