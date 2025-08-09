use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct Destination {
    pub source: i32,
    pub target: i32,
    pub service: String,
    pub mode: i32,
    #[serde(rename = "sync")]
    pub is_sync: bool,
}

/// 对应 Java 中的 MessageHeader
#[derive(Debug, Serialize, Deserialize)]
pub struct MessageHeader {
    #[serde(rename = "messageId")]
    pub message_id: String,
    pub op_code: i32,
    pub timestamp: i64, // Java 是 long (毫秒级), 对应 i64
    pub destination: Destination,
}

/// 对应 Java 中的 ServiceMessage.body
#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceMessageBody {
    // payload 可能是 QueryDTO 转换为的 Map，或者其他 Object
    // 示例中为 [{}]，最灵活的方式是使用 Vec<serde_json::Value>
    pub payload: Vec<Value>,
}

/// 对应 Java 中的 ServiceMessage
#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceMessage {
    pub header: MessageHeader,
    pub body: ServiceMessageBody,
}

// --- 响应相关数据结构 ---
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")] // 根据需要决定是否驼峰命名
pub struct ServiceMessageReplyBuffer {
    pub header: MessageReplyHeader,
    pub body: ServiceMessageReplyBody,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageReplyHeader {
    #[serde(rename = "messageId")]
    pub message_id: String,
    pub op_code: i32,
    pub timestamp: i64,
    pub destination: Destination, // 目的地信息，与请求中相同

    pub message_code: i32,
    pub description: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceMessageReplyBody {
    // 如果 payload 可能是不确定的类型（比如有时是 false，有时是 { "data": "..." }），
    // 那么应该使用 pub payload: Value,
    pub payload: Value,
}
