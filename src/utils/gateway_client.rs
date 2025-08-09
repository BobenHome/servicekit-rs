use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use reqwest::Client;
use tracing::{error, info};
use uuid::Uuid;

use crate::config::TelecomConfig;

// 导入我们定义的请求和响应结构
use super::gateway_types::{
    Destination, MessageHeader, ServiceMessage, ServiceMessageBody, ServiceMessageReplyBuffer,
};
use serde_json::Value;

/// 网关客户端，封装了与电信服务网关的 HTTP 通信。
pub struct GatewayClient {
    pub http_client: Client,
    pub telecom_config: Arc<TelecomConfig>,
}

impl GatewayClient {
    pub fn new(http_client: Client, telecom_config: Arc<TelecomConfig>) -> Self {
        GatewayClient {
            http_client,
            telecom_config,
        }
    }

    /// 调用网关上的特定服务。
    /// `payload_data`: 请求体 `body.payload` 数组中的内容。它是一个 `Vec<serde_json::Value>`，允许传递任意 JSON 数据
    pub async fn invoke_gateway_service(
        &self,
        service_name: &str,
        payload_data: Vec<Value>, // 传入 payload 数组中的具体数据
    ) -> Result<ServiceMessageReplyBuffer> {
        let message_id = Uuid::new_v4().to_string(); // 生成新的 UUID
        let timestamp = Utc::now().timestamp_millis(); // 获取当前毫秒时间戳

        let destination = Destination {
            source: self.telecom_config.source_app_id,
            target: self.telecom_config.target_app_id,
            service: service_name.to_string(),
            mode: self.telecom_config.mode,
            is_sync: self.telecom_config.is_sync,
        };

        let header = MessageHeader {
            message_id,
            op_code: 1,
            timestamp,
            destination,
        };

        let body = ServiceMessageBody {
            payload: payload_data,
        };

        let service_message = ServiceMessage { header, body };
        let gateway_url = &self.telecom_config.gateway_url;
        info!(
            "Sending ServiceMessage to gateway: {gateway_url}. Service: {service_name}. ServiceMessage: {service_message:?}"
        );

        let response = self
            .http_client
            .post(gateway_url) // 发送 POST 请求到网关 URL
            .json(&service_message) // 自动将 `service_message` 序列化为 JSON 并设置 Content-Type: application/json
            .send()
            .await
            .context(format!(
                "Failed to send HTTP request to gateway at {gateway_url}"
            ))?;

        let status = response.status();
        info!("Gateway response status: {status}");

        let response_text = response
            .text()
            .await
            .context("Failed to read response body from gateway")?;
        if status.is_success() {
            info!("Gateway call successful. Status: {status}. Response: {response_text}");
            // 尝试将 JSON 响应体反序列化为 ServiceMessageReplyBuffer
            serde_json::from_str(&response_text).context(format!(
                "Failed to parse successful gateway response JSON from '{response_text}'"
            ))
        } else {
            error!("Gateway call failed with status: {status} and body: {response_text}");
            Err(anyhow!(
                "Gateway call failed: Status={status}, Body={response_text}",
            ))
        }
    }
}
