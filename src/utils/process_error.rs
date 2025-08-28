use anyhow::Error as AnyhowError;
use reqwest::Error as ReqwestError;

// 1. 自定义错误类型，用于区分可重试和不可重试的错误
#[derive(Debug, thiserror::Error)] // 使用 thiserror 库可以方便地实现 Error trait
pub enum ProcessError {
    #[error("网关请求超时，可以重试: {0}")]
    GatewayTimeout(String), // 专门用于 reqwest 的超时等网络错误

    #[error("永久性错误，不应重试: {0}")]
    Permanent(#[from] anyhow::Error), // 包含所有其他错误，如数据解析失败、逻辑错误等
}

pub trait MapToProcessError<T> {
    /// 将 anyhow::Error 映射为自定义的 ProcessError
    fn map_gateway_err(self) -> Result<T, ProcessError>;
}

// 2. 为所有 Result<T, anyhow::Error> 实现这个trait
impl<T> MapToProcessError<T> for Result<T, AnyhowError> {
    fn map_gateway_err(self) -> Result<T, ProcessError> {
        self.map_err(|e| {
            if let Some(reqwest_err) = e.root_cause().downcast_ref::<ReqwestError>() {
                if reqwest_err.is_timeout() || reqwest_err.is_connect() {
                    return ProcessError::GatewayTimeout(e.to_string());
                }
            }
            ProcessError::Permanent(e)
        })
    }
}
