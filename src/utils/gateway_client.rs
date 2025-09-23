use anyhow::{anyhow, Context, Ok, Result};
use chrono::Utc;
use reqwest::Client;
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use crate::{config::TelecomConfig, schedule::binlog_sync::ResultSet};

// 导入我们定义的请求和响应结构
use super::gateway_types::{
    Destination, MessageHeader, ServiceMessage, ServiceMessageBody, ServiceMessageReplyBuffer,
};
use crate::binlog::{
    TelecomMssOrg, TelecomMssOrgMapping, TelecomMssUser, TelecomMssUserMapping, TelecomOrg,
    TelecomOrgTree, TelecomUser,
};
use crate::schedule::binlog_sync::{DataType, Page};
use serde_json::{json, Value};

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
        target_app_id: u32,
        payload_data: Vec<Value>, // 传入 payload 数组中的具体数据
    ) -> Result<ServiceMessageReplyBuffer> {
        let message_id = Uuid::new_v4().to_string(); // 生成新的 UUID
        let timestamp = Utc::now().timestamp_millis(); // 获取当前毫秒时间戳

        let destination = Destination {
            source: self.telecom_config.source_app_id,
            target: target_app_id,
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
            .await?;

        let status = response.status();

        let response_text = response
            .text()
            .await
            .context("Failed to read response body from gateway")?;
        if status.is_success() {
            info!("Gateway call successful with status: {status}.");
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

    pub async fn update_newtca_train_status(
        &self,
        training_id: &str,
        training_status: Option<&str>,
    ) -> Result<ServiceMessageReplyBuffer> {
        let payload = vec![json!({training_id: training_status})];
        self.invoke_gateway_service(
            "bj.bjglinfo.gettrainstatusbyid",
            self.telecom_config.targets.newtca,
            payload,
        )
        .await
    }

    pub async fn binlog_find(
        &self,
        data_type: DataType,
        start_time: i64,
        end_time: i64,
        current_page: Option<Page>,
    ) -> Result<Option<ResultSet>> {
        let page = current_page.unwrap_or_else(|| Page::new(1, 20));

        let payload: Vec<Value> = vec![
            json!(1),
            json!("telecom"),
            json!(data_type),
            json!(start_time),
            json!(end_time),
            json!(page),
        ];

        let reply_buffer = self
            .invoke_gateway_service("binlog.find", self.telecom_config.targets.basedata, payload)
            .await?;

        if reply_buffer.header.message_code != 10000 {
            error!(
                "Invalid message code: {}, description: {}",
                reply_buffer.header.message_code, reply_buffer.header.description
            );
            return Ok(None);
        }

        // 解析响应
        match &reply_buffer.body.payload {
            Value::Object(payload_obj) => {
                let parse_result =
                    serde_json::from_value::<ResultSet>(Value::Object(payload_obj.clone()));
                match parse_result {
                    Result::Ok(result_set) => Ok(Some(result_set)),
                    Err(e) => {
                        error!("Failed to parse ResultSet from response: {e:?}");
                        Ok(None)
                    }
                }
            }
            _ => {
                error!(
                    "Unexpected response payload format: {:?}",
                    reply_buffer.body.payload
                );
                Ok(None)
            }
        }
    }

    pub async fn org_loadbyid(&self, cid: &str) -> Result<Option<TelecomOrg>> {
        let payload: Vec<Value> = vec![json!("telecom"), json!(cid)];

        let reply_buffer = self
            .invoke_gateway_service(
                "org.loadbyid",
                self.telecom_config.targets.basedata,
                payload,
            )
            .await?;

        if reply_buffer.header.message_code != 10000 {
            error!(
                "Invalid message code: {}, description: {}",
                reply_buffer.header.message_code, reply_buffer.header.description
            );
            return Ok(None);
        }

        // 解析响应
        match &reply_buffer.body.payload {
            Value::Object(payload_obj) => {
                let parse_result =
                    serde_json::from_value::<TelecomOrg>(Value::Object(payload_obj.clone()));
                match parse_result {
                    Result::Ok(telecom_org) => Ok(Some(telecom_org)),
                    Err(e) => {
                        error!("Failed to parse TelecomOrg from response: {e:?}");
                        Ok(None)
                    }
                }
            }
            _ => {
                error!(
                    "Unexpected response payload format: {:?}",
                    reply_buffer.body.payload
                );
                Ok(None)
            }
        }
    }

    pub async fn org_tree_loadbyid(&self, cid: &str) -> Result<Option<TelecomOrgTree>> {
        let payload: Vec<Value> = vec![json!("telecom"), json!(cid)];

        let reply_buffer = self
            .invoke_gateway_service(
                "org.tree_loadbyid",
                self.telecom_config.targets.basedata,
                payload,
            )
            .await?;

        if reply_buffer.header.message_code != 10000 {
            error!(
                "Invalid message code: {}, description: {}",
                reply_buffer.header.message_code, reply_buffer.header.description
            );
            return Ok(None);
        }

        match &reply_buffer.body.payload {
            Value::Object(payload_obj) => {
                let parse_result =
                    serde_json::from_value::<TelecomOrgTree>(Value::Object(payload_obj.clone()));
                match parse_result {
                    Result::Ok(telecom_org_tree) => Ok(Some(telecom_org_tree)),
                    Err(e) => {
                        error!("Failed to parse TelecomOrgTree from response: {e:?}");
                        Ok(None)
                    }
                }
            }
            _ => {
                error!(
                    "Unexpected response payload format: {:?}",
                    reply_buffer.body.payload
                );
                Ok(None)
            }
        }
    }

    pub async fn mss_organization_translate(
        &self,
        cid: &str,
    ) -> Result<Option<TelecomMssOrgMapping>> {
        let payload: Vec<Value> = vec![Value::Null, json!(cid)];

        let reply_buffer = self
            .invoke_gateway_service(
                "mss.organization.translate",
                self.telecom_config.targets.basedata,
                payload,
            )
            .await?;

        if reply_buffer.header.message_code != 10000 {
            error!(
                "Invalid message code: {}, description: {}",
                reply_buffer.header.message_code, reply_buffer.header.description
            );
            return Ok(None);
        }

        match &reply_buffer.body.payload {
            Value::Object(payload_obj) => {
                let parse_result = serde_json::from_value::<TelecomMssOrgMapping>(Value::Object(
                    payload_obj.clone(),
                ));
                match parse_result {
                    Result::Ok(telecom_mss_org_mapping) => Ok(Some(telecom_mss_org_mapping)),
                    Err(e) => {
                        error!("Failed to parse TelecomMssOrgMapping from response: {e:?}");
                        Ok(None)
                    }
                }
            }
            _ => {
                error!(
                    "Unexpected response payload format: {:?}",
                    reply_buffer.body.payload
                );
                Ok(None)
            }
        }
    }

    pub async fn mss_organization_query(
        &self,
        mss_code: &str,
    ) -> Result<Option<Vec<TelecomMssOrg>>> {
        let payload: Vec<Value> = vec![
            json!(vec![json!(mss_code)]), // 嵌套数组
        ];

        let reply_buffer = self
            .invoke_gateway_service(
                "mss.organization.query",
                self.telecom_config.targets.mss,
                payload,
            )
            .await?;

        if reply_buffer.header.message_code != 10000 {
            error!(
                "Invalid message code: {}, description: {}",
                reply_buffer.header.message_code, reply_buffer.header.description
            );
            return Ok(None);
        }

        match &reply_buffer.body.payload {
            Value::Array(arr) => {
                let parse_result =
                    serde_json::from_value::<Vec<TelecomMssOrg>>(Value::Array(arr.clone()));
                match parse_result {
                    Result::Ok(vec_telecom_mss_org) => Ok(Some(vec_telecom_mss_org)),
                    Err(e) => {
                        error!("Failed to parse TelecomMssOrg from response: {e:?}");
                        Ok(None)
                    }
                }
            }
            _ => {
                error!(
                    "Unexpected response payload format: {:?}",
                    reply_buffer.body.payload
                );
                Ok(None)
            }
        }
    }

    pub async fn user_loadbyid(&self, cid: &str) -> Result<Option<TelecomUser>> {
        let payload: Vec<Value> = vec![json!("telecom"), json!(cid)];

        let reply_buffer = self
            .invoke_gateway_service(
                "user.loadbyid",
                self.telecom_config.targets.basedata,
                payload,
            )
            .await?;

        if reply_buffer.header.message_code != 10000 {
            error!(
                "Invalid message code: {}, description: {}",
                reply_buffer.header.message_code, reply_buffer.header.description
            );
            return Ok(None);
        }

        // 解析响应
        match &reply_buffer.body.payload {
            Value::Object(payload_obj) => {
                let parse_result =
                    serde_json::from_value::<TelecomUser>(Value::Object(payload_obj.clone()));
                match parse_result {
                    Result::Ok(telecom_user) => Ok(Some(telecom_user)),
                    Err(e) => {
                        error!("Failed to parse TelecomUser from response: {e:?}");
                        Ok(None)
                    }
                }
            }
            _ => {
                error!(
                    "Unexpected response payload format: {:?}",
                    reply_buffer.body.payload
                );
                Ok(None)
            }
        }
    }

    pub async fn mss_user_translate(&self, cid: &str) -> Result<Option<TelecomMssUserMapping>> {
        let payload: Vec<Value> = vec![Value::Null, json!(cid)];

        let reply_buffer = self
            .invoke_gateway_service(
                "mss.user.translate",
                self.telecom_config.targets.mss,
                payload,
            )
            .await?;

        if reply_buffer.header.message_code != 10000 {
            error!(
                "Invalid message code: {}, description: {}",
                reply_buffer.header.message_code, reply_buffer.header.description
            );
            return Ok(None);
        }

        match &reply_buffer.body.payload {
            Value::Object(payload_obj) => {
                let parse_result = serde_json::from_value::<TelecomMssUserMapping>(Value::Object(
                    payload_obj.clone(),
                ));
                match parse_result {
                    Result::Ok(mss_user_mapping) => Ok(Some(mss_user_mapping)),
                    Err(e) => {
                        error!("Failed to parse TelecomMssOrgMapping from response: {e:?}");
                        Ok(None)
                    }
                }
            }
            _ => {
                error!(
                    "Unexpected response payload format: {:?}",
                    reply_buffer.body.payload
                );
                Ok(None)
            }
        }
    }

    pub async fn mss_user_queryorder(&self, hr_code: &str) -> Result<Option<Vec<TelecomMssUser>>> {
        let payload: Vec<Value> = vec![
            json!(vec![json!(hr_code)]), // 嵌套数组
        ];

        let reply_buffer = self
            .invoke_gateway_service(
                "mss.user.queryorder",
                self.telecom_config.targets.basedata,
                payload,
            )
            .await?;

        if reply_buffer.header.message_code != 10000 {
            error!(
                "Invalid message code: {}, description: {}",
                reply_buffer.header.message_code, reply_buffer.header.description
            );
            return Ok(None);
        }

        match &reply_buffer.body.payload {
            Value::Array(arr) => {
                let parse_result =
                    serde_json::from_value::<Vec<TelecomMssUser>>(Value::Array(arr.clone()));
                match parse_result {
                    Result::Ok(vec_mss_user) => Ok(Some(vec_mss_user)),
                    Err(e) => {
                        error!("Failed to parse TelecomMssUser from response: {e:?}");
                        Ok(None)
                    }
                }
            }
            _ => {
                error!(
                    "Unexpected mss_user_queryorder response payload format: {:?}",
                    reply_buffer.body.payload
                );
                Ok(None)
            }
        }
    }
}
