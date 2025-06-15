use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow; // 从数据库读取

#[derive(Debug, FromRow, Serialize, Deserialize, Clone)]
pub struct ClassData {
    pub _id: String,
    pub id: String,
    pub operation: String,
    #[serde(rename = "trainingId")]
    pub training_id: String,
    #[serde(rename = "trainingName")]
    pub training_name: String,
    pub train_level: Option<String>,
    pub train_mode: Option<String>,
    pub train_category: Option<String>,
    pub train_content: Option<String>,
    pub train_purpose: Option<String>,
    pub train_object: Option<String>,
    pub train_claim: Option<String>,
    pub train_organizer: Option<String>,
    pub plan_id: Option<String>,
    pub train_type: Option<String>,
    pub train_assess_type: Option<String>,
    pub train_time: Option<Decimal>,
    pub train_people_number: Option<Decimal>,
    pub train_sponsor_number: Option<Decimal>,
    pub train_user_assess: Option<Decimal>,
    pub train_sponsor_assess: Option<Decimal>,
    pub train_explan: Option<String>,
    pub train_assist_organizer: Option<String>,
    pub train_responsible_user: Option<String>,
    pub train_responsible_user_name: Option<String>,
    pub train_address: Option<String>,
    pub train_address_info: Option<String>,
    pub train_responsible_user_mobile: Option<String>,
    pub train_beg_time: Option<String>,
    pub train_end_time: Option<String>,
    pub signup_beg_time: Option<String>,
    pub signup_end_time: Option<String>,
    pub train_fee: Option<Decimal>,
    pub training_status: Option<String>,
    #[serde(rename = "supDeptCode")]
    pub sup_dept_code: Option<String>,
    #[serde(rename = "supDeptName")]
    pub sup_dept_name: Option<String>,
    #[serde(rename = "supDeptType")]
    pub sup_dept_type: Option<String>,
    #[serde(rename = "creatPlanOrg")]
    pub creat_plan_org: Option<String>,
    #[serde(rename = "creatPlanOrgType")]
    pub creat_plan_org_type: Option<String>,
    pub org_id: Option<String>,
    pub org_name: Option<String>,
    pub org_class: Option<String>,
}

// 示例：讲师数据结构体
#[derive(Debug, FromRow, Serialize, Deserialize, Clone)]
pub struct LecturerData {
    pub lecturer_id: i32,
    pub lecturer_name: String,
    pub expertise: String,
    // ... 讲师相关的字段
}

// 示例：培训数据结构体
#[derive(Debug, FromRow, Serialize, Deserialize, Clone)]
pub struct PsnTrainingData {
    pub training_id: i32,
    pub course_name: String,
    pub duration_hours: i32,
    // ... 培训相关的字段
}

// 示例：归档数据结构体
#[derive(Debug, FromRow, Serialize, Deserialize, Clone)]
pub struct PsnArchiveData {
    pub archive_id: i32,
    pub file_name: String,
    pub file_size: i32,
    // ... 归档相关的字段
}

// 定义一个枚举来封装所有可能的数据类型
#[derive(Debug, Serialize, Clone)] // Enum 也需要 Serialize
#[serde(untagged)] // 使用 untagged 序列化，这样它不会在 JSON 中添加额外的标签
pub enum DynamicPsnData {
    Class(ClassData),
    Lecturer(LecturerData),
    PsnTraining(PsnTrainingData),
    PsnArchive(PsnArchiveData),
}

impl DynamicPsnData {
    // 辅助方法，根据枚举变体返回对应的动态键名
    pub fn get_key_name(&self) -> &'static str {
        match self {
            DynamicPsnData::Class(_) => "classData",
            DynamicPsnData::Lecturer(_) => "lecturerData",
            DynamicPsnData::PsnTraining(_) => "psnTrainingData",
            DynamicPsnData::PsnArchive(_) => "psnArchiveData",
        }
    }
}
