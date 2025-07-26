use anyhow::{anyhow, Context, Result};
use chrono::Utc; // 用于获取当前毫秒时间戳
use reqwest::Client;
use tracing::{error, info};
use uuid::Uuid; // 用于生成 UUID

use crate::config::TelecomConfig;

// 导入我们定义的请求和响应结构
use super::gateway_types::{
    Destination, MessageHeader, ServiceMessage, ServiceMessageBody, ServiceMessageReplyBuffer,
};
use serde_json::Value; // 用于 payload

/// 网关客户端，封装了与电信服务网关的 HTTP 通信。
pub struct GatewayClient {
    pub client: Client,
    pub config: TelecomConfig,
}

impl GatewayClient {
    /// 创建一个新的 GatewayClient 实例。
    ///
    /// 参数 `source`, `target`, `mode`, `is_sync` 通常来自应用程序的配置。
    pub fn new(telecom_config: &TelecomConfig) -> Self {
        GatewayClient {
            client: Client::new(),
            config: telecom_config.clone(),
        }
    }

    /// 调用网关上的特定服务。
    ///
    /// `payload_data`: 请求体 `body.payload` 数组中的内容。
    ///                 它是一个 `Vec<serde_json::Value>`，允许传递任意 JSON 数据，
    ///                 通常是 QueryDTO 转换为的 JSON 对象。
    pub async fn invoke_gateway_service(
        &self,
        service_name: &str,
        payload_data: Vec<Value>, // 传入 payload 数组中的具体数据
    ) -> Result<ServiceMessageReplyBuffer> {
        let message_id = Uuid::new_v4().to_string(); // 生成新的 UUID
        let timestamp = Utc::now().timestamp_millis(); // 获取当前毫秒时间戳

        let destination = Destination {
            source: self.config.source_app_id,
            target: self.config.target_app_id,
            service: service_name.to_string(),
            mode: self.config.mode,
            is_sync: self.config.is_sync,
        };

        let header = MessageHeader {
            message_id,
            op_code: 1, // 对应 Java 中的 OPCode.send
            timestamp,
            destination,
        };

        let body = ServiceMessageBody {
            payload: payload_data,
        };

        let service_message = ServiceMessage { header, body };
        info!(
            "Sending ServiceMessage to gateway: {}. Service: {}. ServiceMessage: {:?}",
            self.config.gateway_url, service_name, service_message
        );

        let response = self
            .client
            .post(&self.config.gateway_url) // 发送 POST 请求到网关 URL
            .json(&service_message) // 自动将 `service_message` 序列化为 JSON 并设置 Content-Type: application/json
            .send()
            .await
            .context(format!(
                "Failed to send HTTP request to gateway at {}",
                self.config.gateway_url
            ))?;

        let status = response.status();
        info!("Gateway response status: {}", status);

        let response_text = response
            .text()
            .await
            .context("Failed to read response body from gateway")?;
        if status.is_success() {
            info!(
                "Gateway call successful. Status: {}. Response: {}",
                status, response_text
            );
            // 尝试将 JSON 响应体反序列化为 ServiceMessageReplyBuffer
            serde_json::from_str(&response_text).context(format!(
                "Failed to parse successful gateway response JSON from '{}'",
                response_text
            ))
        } else {
            error!(
                "Gateway call failed with status: {} and body: {}",
                status, response_text
            );
            Err(anyhow!(
                "Gateway call failed: Status={}, Body={}",
                status,
                response_text
            ))
        }
    }
}
