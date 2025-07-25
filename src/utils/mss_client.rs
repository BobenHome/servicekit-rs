use anyhow::{anyhow, Context, Result};
use chrono::Local;
use reqwest::{Client, StatusCode};
use serde_json::{from_str, json, Value};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{ArchivingMssMapper, DynamicPsnData, MssInfoConfig, PushResultParser, RecordMssReply};

/// 通用的 PSN DOS 推送方法。
/// 接收所需的所有依赖（HTTP 客户端、配置、数据映射器和解析器）作为参数。
// 将其设为 pub，以便其他模块可以调用
pub async fn psn_dos_push(
    http_client: &Client,                  // 引用类型，避免所有权转移
    mss_info_config: &MssInfoConfig,       // 引用类型
    archiving_mapper: &ArchivingMssMapper, // 引用类型
    push_result_parser: &PushResultParser, // 引用类型
    psn_data: &DynamicPsnData,             // 引用类型
) -> Result<()> {
    let mut request_attempt = 0;
    const MAX_RETRIES: u32 = 5;

    let dynamic_key_name = psn_data.get_key_name();

    let request_json_data_value = json!({
        dynamic_key_name: [psn_data]
    });

    let request_json_data = serde_json::to_string(&request_json_data_value)
        .context("Failed to serialize dynamic JSON payload")?;

    let mut final_http_body_str: String = String::new(); // 最终的响应体

    // 引入一个 Result 来封装循环体内的逻辑，以便统一错误处理
    let result_of_send_loop: Result<(), anyhow::Error> = loop {
        request_attempt += 1;
        info!(
            "Attempting to send data to {} (Attempt {}), key: {}",
            mss_info_config.app_url, request_attempt, dynamic_key_name
        );

        if request_attempt > 1 {
            info!("********Resting 1 minute before retry********");
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        } else {
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }

        let request = http_client
            .post(&mss_info_config.app_url)
            .header("X-APP-ID", &mss_info_config.app_id)
            .header("X-APP-KEY", &mss_info_config.app_key)
            .header("Content-Type", "application/json")
            .body(request_json_data.clone());

        let response_result = request.send().await;

        let current_http_body_str: String;
        let current_status: StatusCode;

        match response_result {
            Ok(response) => {
                current_status = response.status();
                match response.text().await {
                    Ok(body) => current_http_body_str = body,
                    Err(e) => {
                        // 读取响应体失败
                        error!(
                            "Failed to read response body for {}: {:?}",
                            mss_info_config.app_url, e
                        );
                        break Err(anyhow!(
                            "Failed to read response body for {}: {:?}",
                            mss_info_config.app_url,
                            e
                        ));
                    }
                }
            }
            Err(e) => {
                // 发送请求失败 (网络不通, DNS 查找失败等)
                error!(
                    "Failed to send HTTP request to {}: {:?}",
                    mss_info_config.app_url, e
                );
                break Err(anyhow!(
                    "Failed to send HTTP request to {}: {:?}",
                    mss_info_config.app_url,
                    e
                ));
            }
        }

        info!(
            "Received response for {} (Attempt {}): Status={}, Body={}",
            mss_info_config.app_url, request_attempt, current_status, current_http_body_str
        );

        if current_status.is_success() {
            if have_rest(&current_http_body_str) {
                if request_attempt >= MAX_RETRIES {
                    error!(
                        "Max retries reached. Still have 'rest' condition. Body: {}",
                        current_http_body_str
                    );
                    break Err(anyhow!(
                        "Max retries reached for {}. Still requires rest.",
                        mss_info_config.app_url
                    ));
                }
                info!("Response indicates 'rest' required. Retrying...");
                continue; // 继续循环进行重试
            } else {
                info!(
                    "Request to {} successful and no 'rest' required.",
                    mss_info_config.app_url
                );
                final_http_body_str = current_http_body_str; // 成功时赋值
                break Ok(()); // 成功并退出重试循环
            }
        } else {
            // HTTP 状态码表示失败
            error!(
                "HTTP request to {} failed with status: {}. Body: {}",
                mss_info_config.app_url, current_status, current_http_body_str
            );
            break Err(anyhow!(
                "HTTP request failed with status: {}. Body: {}",
                current_status,
                current_http_body_str
            ));
        }
    };

    // 统一的错误处理和记录逻辑
    let current_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

    let primary_result = match result_of_send_loop {
        Ok(_) => {
            // 请求成功，记录成功信息
            let record_reply = RecordMssReply {
                id: Uuid::new_v4().to_string().replace("-", ""),
                datas: format!(
                    "X-APP_ID{}|X-APP-KEY{}|DATA:{}",
                    mss_info_config.app_id, mss_info_config.app_key, request_json_data
                ),
                send_time: current_time,
                msg: final_http_body_str.clone(), // 成功时使用 final_http_body_str
            };
            // 尝试记录成功信息，如果记录失败，将记录的错误链到主结果上
            archiving_mapper
                .record_mss_reply(&record_reply)
                .await
                .context("Failed to record SUCCESS MSS reply")?; // 使用 ? 传播数据库写入错误

            // 只有成功时才调用 parser.parse
            push_result_parser
                .parse(&request_json_data, &final_http_body_str)
                .await;
            Ok(()) // 主请求和记录都成功
        }
        Err(e) => {
            // 请求失败，记录失败信息
            let error_message = format!("ERROR: {:?}", e); // 捕获并格式化错误

            let record_reply_error = RecordMssReply {
                id: Uuid::new_v4().to_string().replace("-", ""),
                datas: format!("sendDATA:{}", request_json_data), // 记录发送的数据
                send_time: current_time,
                msg: error_message, // 记录错误消息
            };
            // 尝试记录失败信息，如果记录失败，将记录的错误链到主结果上
            archiving_mapper
                .record_mss_reply(&record_reply_error)
                .await
                .context("Failed to record FAILED MSS reply")?; // 使用 ? 传播数据库写入错误
                                                                // 返回原始的失败结果，以便 execute 方法能知道发生了错误
            Err(e)
        }
    };
    primary_result // 返回主结果，它包含了 send_loop 的结果以及记录的结果
}

/// 检查 HTTP 响应体是否指示需要“休息”（重试）
fn have_rest(http_body: &str) -> bool {
    // 1. 检查 httpBody 是否为空 JSON 对象字符串
    if "{}" == http_body.trim() {
        // 使用 .trim() 处理可能的空白字符
        return false;
    }
    // 2. 尝试将 httpBody 解析为 JSON
    let json_value: Value = match from_str(http_body) {
        Ok(val) => val,
        Err(e) => {
            // 如果解析失败，说明不是有效的 JSON，则不认为需要“休息”
            warn!(
                "Failed to parse http_body as JSON in have_rest: {:?}. Body: '{}'",
                e, http_body
            );
            return false;
        }
    };

    // 3. 判断是否是 dcoos 返回的那种 json，并检查 "code" 字段
    if let Some(obj) = json_value.as_object() {
        // 确保是 JSON 对象
        if let Some(code_value) = obj.get("code") {
            // 获取 "code" 字段的值
            if let Some(code_str) = code_value.as_str() {
                // 确保 "code" 是字符串
                // 4. 检查 "code" 字段的值是否为 "9019"
                if code_str == "9019" {
                    return true;
                }
            }
        }
    }
    // 默认情况或不满足条件时返回 false
    false
}
