use chrono::Local;
use serde_json::Value;
use sqlx::MySqlPool;
use tracing::{error, info};
use uuid::Uuid;

use crate::models::push_result::{MssPushResult, MssPushResultDetail, PushResultService};

const SUCCESS_CODE: &str = "200";

const REQUEST_KEYS: [(&str, i32, &str, &str); 4] = [
    ("classData", 1, "trainingId", "train_id"),
    ("lecturerData", 2, "course_id", "course_id"),
    ("psnTrainingData", 3, "userId", "user_id"),
    ("psnArchiveData", 4, "userId", "user_id"),
];

const ERROR_KEYS: [(&str, i32, &str); 4] = [
    ("classData", 1, "trainingId"),
    ("lecturerData", 2, "course_id"),
    ("psnTrainingData", 3, "userId"),
    ("psnArchiveData", 4, "userId"),
];

pub struct PushResultParser {
    push_result_service: PushResultService,
}

impl PushResultParser {
    pub fn new(mysql_pool: MySqlPool) -> Self {
        PushResultParser {
            push_result_service: PushResultService::new(mysql_pool),
        }
    }
    pub async fn parse(&self, data: &str, result: &str) -> Result<(), String> {
        info!("Parsing push result beginning");

        let mut push_result = MssPushResult {
            id: Uuid::new_v4().to_string(),
            push_time: Local::now().naive_local(),
            train_id: None,
            course_id: None,
            user_id: None,
            data_type: None,
            error_msg: None,
            error_code: None,
        };
        let mut result_details = Vec::new();

        // 1. 解析 'result' JSON
        let result_data = match self.parse_json(result) {
            Ok(val) => val,
            Err(e) => {
                push_result.error_code = Some("500".into());
                return self
                    .handle_parse_error(&mut push_result, &result_details, e)
                    .await;
            }
        };

        push_result.error_code = result_data
            .get("descCode")
            .and_then(Value::as_str)
            .map(ToString::to_string);

        // 2. 解析请求数据JSON
        let request_data = match self.parse_json(data) {
            Ok(val) => val,
            Err(e) => {
                return self
                    .handle_parse_error(&mut push_result, &result_details, e)
                    .await;
            }
        };

        // 3. 从请求数据中提取信息
        Self::extract_request_info(&request_data, &mut push_result, &mut result_details);

        // 4. 处理成功情况
        if push_result.error_code.as_deref() == Some(SUCCESS_CODE) {
            self.record_result(&push_result, &result_details).await;
            info!(
                "Parsing push result completed successfully. Result ID: {}",
                push_result.id
            );
            return Ok(());
        }

        // 5. 处理失败情况
        if let Err(e) = self
            .handle_failure(&result_data, &mut push_result, &mut result_details)
            .await
        {
            self.record_result(&push_result, &result_details).await;
            return Err(e);
        }

        // 6. 记录失败结果
        self.record_result(&push_result, &result_details).await;
        info!(
            "Parsing push result completed with error. Result ID: {}",
            push_result.id
        );

        // 7.返回错误信息
        Err(push_result.error_msg.clone().unwrap_or_else(|| {
            format!(
                "Push failed with code: {}",
                push_result.error_code.as_deref().unwrap_or("UNKNOWN")
            )
        }))
    }

    /// 从请求数据中提取信息
    fn extract_request_info(
        request_data: &Value,
        push_result: &mut MssPushResult,
        result_details: &mut Vec<MssPushResultDetail>,
    ) {
        for &(key, data_type_val, id_field, result_field) in &REQUEST_KEYS {
            if let Some(array) = request_data.get(key).and_then(Value::as_array) {
                if let Some(obj) = array.first().and_then(Value::as_object) {
                    if let Some(id_val) = obj.get(id_field).and_then(Value::as_str) {
                        push_result.data_type = Some(data_type_val);

                        match result_field {
                            "train_id" => push_result.train_id = Some(id_val.to_string()),
                            "course_id" => push_result.course_id = Some(id_val.to_string()),
                            "user_id" => push_result.user_id = Some(id_val.to_string()),
                            _ => (),
                        }

                        result_details.push(MssPushResultDetail {
                            data_id: push_result.id.clone(),
                            result_id: Some(id_val.to_string()),
                        });
                    }
                }
            }
        }
    }

    /// 解析JSON字符串
    fn parse_json(&self, input: &str) -> Result<Value, String> {
        serde_json::from_str(input)
            .map_err(|e| format!("Failed to parse JSON: {e:?}, Input: {input}"))
    }

    /// 处理结果解析错误
    async fn handle_parse_error(
        &self,
        push_result: &mut MssPushResult,
        result_details: &[MssPushResultDetail],
        error: String,
    ) -> Result<(), String> {
        error!("{}", error);
        push_result.error_msg = Some(error.clone());
        self.record_result(push_result, result_details).await;
        Err(error)
    }

    /// 处理失败响应
    async fn handle_failure(
        &self,
        result_data: &Value,
        push_result: &mut MssPushResult,
        result_details: &mut Vec<MssPushResultDetail>,
    ) -> Result<(), String> {
        let raw_data_str = result_data
            .get("data")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                let err = format!("Missing 'data' field in result JSON: {result_data:?}");
                error!("{err}");
                push_result.error_msg = Some(err.clone());
                err
            })?;

        let error_data = self.parse_json(raw_data_str).map_err(|e| {
            error!("{e}");
            push_result.error_msg = Some(e.clone());
            e
        })?;

        // 重置结果详情
        result_details.clear();
        push_result.train_id = None;
        push_result.course_id = None;
        push_result.user_id = None;
        push_result.data_type = None;

        // 从错误数据中提取信息
        if let Some(error_data_obj) = error_data.as_object() {
            for &(key, data_type_val, id_field) in &ERROR_KEYS {
                if let Some(array) = error_data_obj.get(key).and_then(Value::as_array) {
                    if let Some(obj) = array.first().and_then(Value::as_object) {
                        push_result.data_type = Some(data_type_val);

                        // 提取ID字段
                        if let Some(id_val) = obj.get(id_field).and_then(Value::as_str) {
                            result_details.push(MssPushResultDetail {
                                data_id: push_result.id.clone(),
                                result_id: Some(id_val.to_string()),
                            });
                        }

                        // 提取错误信息
                        if let Some(msg) = obj.get("errormsg").and_then(Value::as_str) {
                            push_result.error_msg = Some(msg.to_string());
                        }
                        if let Some(code) = obj.get("errorcode").and_then(Value::as_str) {
                            push_result.error_code = Some(code.to_string());
                        }

                        // 提取其他可能存在的ID
                        if let Some(train_id) = obj.get("trainingId").and_then(Value::as_str) {
                            push_result.train_id = Some(train_id.to_string());
                        }
                        if let Some(course_id) = obj.get("course_id").and_then(Value::as_str) {
                            push_result.course_id = Some(course_id.to_string());
                        }
                        if let Some(user_id) = obj.get("userId").and_then(Value::as_str) {
                            push_result.user_id = Some(user_id.to_string());
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// 记录结果到数据库
    async fn record_result(
        &self,
        push_result: &MssPushResult,
        result_details: &[MssPushResultDetail],
    ) {
        if let Err(e) = self
            .push_result_service
            .record(push_result, result_details)
            .await
        {
            error!("Failed to record push result: {e:?}");
        }
    }
}
