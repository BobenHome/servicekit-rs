use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow; // 从数据库读取

// 班级数据结构体
#[derive(Debug, FromRow, Serialize, Deserialize, Clone)]
pub struct ClassData {
    pub _id: String,
    pub id: String,
    pub operation: String,
    #[serde(rename = "trainingId")]
    pub training_id: String,
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

// 讲师数据结构体
#[derive(Debug, FromRow, Serialize, Deserialize, Clone, Default)]
#[serde(default)] // 自动处理缺失字段的默认值
pub struct LecturerData {
    pub _id: String,
    /// 业务主键
    pub id: String,
    /// 数据状态
    pub operation: String,
    /// 培训班id
    #[serde(rename = "trainingId")]
    pub training_id: String,
    /// hr传输数据人员编码
    #[serde(rename = "userId")]
    pub user_id: Option<String>,
    /// 真实姓名
    pub user_name: Option<String>,
    /// 讲师分类
    pub lecturer_type: Option<String>,
    /// 课程id
    pub course_id: Option<String>,
    /// 课程名称
    pub course_name: Option<String>,
    /// 课程时长
    pub course_time: Option<Decimal>,
    /// 授课质量评估分
    pub course_assess: Option<String>,
    /// 开始时间
    pub start_date: Option<String>,
    /// 结束时间
    pub end_date: Option<String>,
    /// 员工类别
    #[serde(rename = "jobCategory")]
    pub job_category: Option<String>,
    /// 课程状态 (默认值: "已开课")
    pub course_status: String,
}

// 人员清单数据结构体
#[derive(Debug, FromRow, Serialize, Deserialize, Clone, Default)]
#[serde(default)]
pub struct TrainingData {
    pub _id: String,
    /// 业务主键
    pub id: String,
    /// 培训班id
    #[serde(rename = "trainingId")]
    pub training_id: String,
    /// 学员mss的id
    #[serde(rename = "userId")]
    pub user_id: Option<String>,
    /// 学员班内角色
    pub is_sponsor: String,
    /// 结业状态
    #[serde(rename = "psnTrainingStatus")]
    pub psn_training_status: String,
    /// 员工类别
    #[serde(rename = "jobCategory")]
    pub job_category: Option<String>,
    /// 数据状态
    pub operation: String,
    /// 备注
    pub remark: Option<String>,
}

// 人员归档数据结构体
#[derive(Debug, FromRow, Serialize, Deserialize, Clone, Default)]
#[serde(default)]
pub struct ArchiveData {
    /// 业务主键
    pub _id: String,
    /// 业务主键
    pub id: String,
    /// 数据状态
    pub operation: String,
    /// MSS组织唯一编码
    pub user_org_id: Option<String>,
    /// MSS组织类型
    pub user_org_type: Option<String>,
    /// hr传输数据人员编码
    #[serde(rename = "userId")]
    pub user_id: Option<String>,
    /// 培训班名称
    pub training_name: String,
    /// 培训班id
    #[serde(rename = "trainingId")]
    pub training_id: String,
    /// 培训开始日期
    pub training_start_date: String,
    /// 培训结束日期
    pub training_end_date: String,
    /// 培训类别
    pub training_category: String,
    /// 培训方式
    pub train_mode: String,
    /// 学习层评估成绩
    pub study_assess_score: Option<String>,
    /// 行为层评估成绩
    pub action_assess_score: Option<String>,
    /// 主办单位部门MSS组织唯一编码
    pub sponsor_dept: Option<String>,
    /// 主办单位部门MSS组织类型
    pub sponsor_dept_type: Option<String>,
    /// 主办单位公司MSS组织唯一编码
    pub sponsor_org_id: Option<String>,
    /// 主办单位公司MSS组织类型
    pub sponsor_org_type: Option<String>,
    /// 培训学时
    pub training_time: Option<String>,
    /// 培训地点（境内/外）0:境内 1:境外
    pub training_place_type: String,
    /// 备注
    pub remark: Option<String>,
    /// 证书编号
    #[serde(rename = "certificateId")]
    pub certificate_id: Option<String>,
    /// 证书等级
    pub certificate_level: Option<String>,
    /// 证书名称
    pub certificate_name: Option<String>,
    /// 有效期
    pub grant_date: Option<String>,
    /// 发证单位名称
    pub dept: Option<String>,
    /// 发证单位组织编码
    #[serde(rename = "deptCode")]
    pub dept_code: Option<String>,
    /// 发证单位组织类型
    #[serde(rename = "dept_Type")]
    pub dept_type: Option<String>,
    /// 培训目的
    pub train_purpose: Option<String>,
    /// 培训内容
    pub train_content: Option<String>,
    /// 承办单位名称
    #[serde(rename = "undertakeAgentName")]
    pub undertake_agent_name: Option<String>,
    /// 空
    #[serde(rename = "assistAgentName")]
    pub assist_agent_name: Option<String>,
    /// 获证日期
    #[serde(rename = "getCertificateTime")]
    pub get_certificate_time: Option<String>,
    /// 员工类别
    #[serde(rename = "jobCategory")]
    pub job_category: Option<String>,
    /// 是否完成培训班（培训班状态已完毕后，数据有效）
    #[serde(rename = "psnArchiveStatus")]
    pub psn_archive_status: Option<String>,
}

// 定义一个枚举来封装所有可能的数据类型
#[derive(Debug, Serialize, Clone)] // Enum 也需要 Serialize
#[serde(untagged)] // 使用 untagged 序列化，这样它不会在 JSON 中添加额外的标签
pub enum DynamicPsnData {
    Class(ClassData),
    Lecturer(LecturerData),
    Training(TrainingData),
    Archive(ArchiveData),
}

impl DynamicPsnData {
    // 辅助方法，根据枚举变体返回对应的动态键名
    pub fn get_key_name(&self) -> &'static str {
        match self {
            DynamicPsnData::Class(_) => "classData",
            DynamicPsnData::Lecturer(_) => "lecturerData",
            DynamicPsnData::Training(_) => "trainingData",
            DynamicPsnData::Archive(_) => "archiveData",
        }
    }
}
