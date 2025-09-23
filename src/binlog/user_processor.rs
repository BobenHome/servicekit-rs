use crate::schedule::binlog_sync::{EntityMetaInfo, ModifyOperationLog, PermanentFailure};
use crate::utils::{mysql_client, MapToProcessError, ProcessError};
use crate::AppContext;
use anyhow::Result;
use chrono::{Local, NaiveDateTime};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Execute, MySql, QueryBuilder, Transaction};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::ops::DerefMut;
use std::sync::Arc;
use tracing::{error, info};

// 最大重试次数
const MAX_RETRIES: u32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelecomUser {
    pub id: String,
    #[serde(rename = "entityMetaInfo")]
    pub entity_meta_info: Option<EntityMetaInfo>,
    pub is_delete: Option<bool>,
    pub delete: Option<bool>,
    pub loginname: Option<String>,
    pub name: Option<String>,
    pub gender: Option<i32>,
    pub photo: Option<String>,
    pub no: Option<String>,
    pub certificate_type: Option<i32>,
    pub certificate_code: Option<String>,
    pub is_ehr_sync: Option<bool>,
    pub org: Option<String>,
    pub status: Option<i32>,
    pub contact_info: Option<ContactInfo>,
    pub effective_time_start: Option<i64>,
    pub effective_time_end: Option<i64>,
    pub archives_info: Option<ArchivesInfo>,
    pub is_outter: Option<bool>,
    pub user_group_ids: Option<Vec<String>>,
    pub account_type: Option<i32>,
    pub ext: Option<UserExt>,
    #[serde(rename = "encryptCertificate_code")]
    pub encrypt_certificate_code: Option<String>,

    pub hit_date: Option<String>, // yyyy-MM-dd 格式的日期字符串
    pub in_time: Option<NaiveDateTime>,
    pub year: Option<String>,
    pub month: Option<String>,
    pub hit_date1: Option<NaiveDateTime>,
}

impl TelecomUser {
    pub fn trim(&mut self) {
        if let Some(name) = &mut self.name {
            *name = name
                .replace("\n\r", "")
                .replace("/", "-")
                .trim()
                .to_string();
        }

        if let Some(org) = &mut self.org {
            *org = org.replace("\n\r", "").replace("/", "-").trim().to_string();
        }

        if let Some(user_ext) = &mut self.ext {
            user_ext.trim();
        }

        if let Some(contact_info) = &mut self.contact_info {
            contact_info.trim();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContactInfo {
    pub phone: Option<String>,
    pub mobile: Option<String>,
    pub email: Option<String>,
}

impl ContactInfo {
    pub fn trim(&mut self) {
        if let Some(phone) = &mut self.phone {
            *phone = phone.trim().replace("\n", "").to_string();
        }

        if let Some(mobile) = &mut self.mobile {
            *mobile = mobile.trim().replace("\n", "").to_string();
        }

        if let Some(email) = &mut self.email {
            *email = email.trim().replace("\n", "").to_string();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchivesInfo {
    pub birthday: Option<i64>,
    #[serde(rename = "isonlychild")]
    pub is_only_child: Option<bool>,
    pub is_union_members: Option<bool>,
    pub major: Option<String>,
    pub folk: Option<String>,
    pub join_union_date: Option<i64>,
    pub political: Option<String>,
    pub party_date: Option<i64>,
    pub academy: Option<String>,
}

impl ArchivesInfo {
    pub fn trim(&mut self) {
        if let Some(major) = &mut self.major {
            *major = major.trim().replace("\n", "").to_string();
        }

        if let Some(folk) = &mut self.folk {
            *folk = folk.trim().replace("\n", "").to_string();
        }

        if let Some(academy) = &mut self.academy {
            *academy = academy.trim().replace("\n", "").to_string();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserExt {
    pub base_station: Option<BaseStation>,
    pub it_info: Option<Vec<Value>>,
    pub pe_info: Option<Vec<Value>>,
    pub pro_info: Option<Vec<Value>>,
    pub job_info: Option<JobInfo>,
    pub name_card: Option<NameCard>,
    pub weight: Option<f32>,
    pub is_activated: Option<bool>,
    pub authorize_info: Option<AuthorizeInfo>,
    pub password_reset: Option<bool>,
    pub activated_time: Option<i64>,
}

impl UserExt {
    pub fn trim(&mut self) {
        if let Some(base_station) = &mut self.base_station {
            base_station.trim();
        }

        if let Some(name_card) = &mut self.name_card {
            name_card.trim();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NameCard {
    pub name: Option<String>,
    pub company: Option<String>,
    pub company_id: Option<String>,
    #[serde(rename = "companyphone")]
    pub company_phone: Option<String>,
    pub organization: Option<String>,
    pub station: Option<String>,
    pub email: Option<String>,
    pub mobile: Option<String>,
    pub gender: Option<String>,
    pub folk: Option<String>,
}

impl NameCard {
    pub fn trim(&mut self) {
        if let Some(email) = &mut self.email {
            *email = email.trim().replace("\n", "").to_string();
        }

        if let Some(name) = &mut self.name {
            *name = name
                .trim()
                .replace("\n", "")
                .replace("/", "-")
                .replace(" ", "") // 注意：这是非断空格（&nbsp;）
                .replace("|", "-")
                .trim()
                .to_string();
        }

        if let Some(company) = &mut self.company {
            *company = company
                .trim()
                .replace("\n\r", "")
                .replace("/", "-")
                .replace("|", "-")
                .trim()
                .to_string();
        }

        if let Some(organization) = &mut self.organization {
            *organization = organization
                .replace("\n", "")
                .replace("/", "-")
                .replace("|", "-")
                .trim()
                .to_string();
        }

        if let Some(station) = &mut self.station {
            *station = station
                .trim()
                .replace("\n", "")
                .replace("/", "-")
                .replace("|", "-")
                .trim()
                .to_string();
        }

        if let Some(mobile) = &mut self.mobile {
            *mobile = mobile
                .trim()
                .replace("\n", "")
                .replace("/", "-")
                .replace("|", "-")
                .trim()
                .to_string();
        }

        if let Some(company_phone) = &mut self.company_phone {
            *company_phone = company_phone
                .trim()
                .replace("\n", "")
                .replace("/", "-")
                .replace("|", "-")
                .trim()
                .to_string();
        }

        if let Some(folk) = &mut self.folk {
            *folk = folk
                .trim()
                .replace("\n", "")
                .replace("/", "-")
                .replace("|", "-")
                .trim()
                .to_string();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobInfo {
    pub post_name: Option<String>,
    #[serde(rename = "jobStatus")]
    pub job_status: Option<String>,
    #[serde(rename = "jobType")]
    pub job_type: Option<String>,
    #[serde(rename = "hrJobType")]
    pub hr_job_type: Option<String>,
    #[serde(rename = "jobCategory")]
    pub job_category: Option<String>,

    pub positive_date: Option<i32>,
    pub special_job_years: Option<i32>,
    pub work_date: Option<i64>,
    pub special_job: Option<String>,
    pub leave_date: Option<i32>,
    pub work_age: Option<i32>,
    pub is_core_staff: Option<String>,
    #[serde(rename = "enterunit_date")]
    pub enter_unit_date: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseStation {
    pub code: Option<String>,
    pub name: Option<String>,
    pub system: Option<String>,
    pub level: Option<String>,
    #[serde(rename = "gradeSystem")]
    pub grade_system: Option<String>,
    pub grade: Option<String>,
    pub sequence: Option<String>,
}

impl BaseStation {
    pub fn trim(&mut self) {
        if let Some(phone) = &mut self.name {
            *phone = phone.trim().replace("\n", "").to_string();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizeInfo {
    #[serde(rename = "expirationDate")]
    pub expiration_date: Option<i64>,
    #[serde(rename = "mobileVague")]
    pub mobile_vague: Option<String>,
    #[serde(rename = "identityCardDecryptAble")]
    pub identity_card_decrypt_able: Option<String>,
    #[serde(rename = "emailVague")]
    pub email_vague: Option<String>,
    #[serde(rename = "mobileDecryptAble")]
    pub mobile_decrypt_able: Option<String>,
    pub code: Option<String>,
    #[serde(rename = "identityCardEncrypt")]
    pub identity_card_encrypt: Option<String>,
    #[serde(rename = "jobNumber")]
    pub job_number: Option<String>,
    #[serde(rename = "emailEncrypt")]
    pub email_encrypt: Option<String>,
    #[serde(rename = "mobileEncrypt")]
    pub mobile_encrypt: Option<String>,
    pub identity: Option<String>,
    #[serde(rename = "hrCode")]
    pub hr_code: Option<String>,
    #[serde(rename = "emailDecryptAble")]
    pub email_decrypt_able: Option<String>,
    pub account: Option<String>,
    #[serde(rename = "identityCardVague")]
    pub identity_card_vague: Option<String>,
}

/// 一个平铺的结构体，专门用于批量插入 d_telecom_user 表
struct InsertTelecomUser {
    base_station_sequence: Option<String>,
    base_station_code: Option<String>,
    base_station_system: Option<String>,
    base_station_gradesystem: Option<String>,
    base_station_level: Option<String>,
    base_station_grade: Option<String>,
    base_station_name: Option<String>,
    password_reset: Option<String>,
    ext_job_info_jobstatus: Option<String>,
    ext_job_info_jobcategory: Option<String>,
    ext_job_info_hrjobtype: Option<String>,
    ext_job_info_jobtype: Option<String>,
    name_card_company_id: Option<String>,
    name_card_gender: Option<String>,
    name_card_companyphone: Option<String>,
    name_card_organization: Option<String>,
    name_card_name: Option<String>,
    name_card_station: Option<String>,
    name_card_mobile: Option<String>,
    name_card_folk: Option<String>,
    name_card_company: Option<String>,
    name_card_email: Option<String>,
    weight: Option<f32>,
    no: Option<String>,
    account_type: Option<i32>,
    datelastmodified: Option<i64>,
    certificate_code: Option<String>,
    gender: Option<i32>,
    loginname: Option<String>,
    org: Option<String>,
    job_info_positive_date: Option<i32>,
    job_info_special_job_years: Option<i32>,
    job_info_work_date: Option<i64>,
    job_info_is_special_job: Option<String>,
    job_info_leave_date: Option<i32>,
    job_info_work_age: Option<i32>,
    job_info_is_core_staff: Option<String>,
    job_info_enterunit_date: Option<i64>,
    is_ehr_sync: Option<String>,
    photo: Option<String>,
    effective_time_end: Option<i64>,
    contact_info_phone: Option<String>,
    contact_info_mobile: Option<String>,
    contact_info_email: Option<String>,
    user_group_ids: Option<String>,
    d_delete: Option<String>,
    is_delete: Option<String>,
    effective_time_start: Option<i64>,
    encryptcertificate_code: Option<String>,
    name: Option<String>,
    id: String,
    certificate_type: Option<i32>,
    status: Option<i32>,
    archives_info_birthday: Option<i64>,
    archives_info_isonlychild: Option<String>,
    archives_info_is_union_members: Option<String>,
    archives_info_major: Option<String>,
    archives_info_folk: Option<String>,
    archives_info_join_union_date: Option<i64>,
    archives_info_political: Option<String>,
    archives_info_party_date: Option<i64>,
    archives_info_academy: Option<String>,
    hit_date: Option<String>,
    in_time: Option<NaiveDateTime>,
    year: Option<String>,
    month: Option<String>,
    archived_batches: Option<String>,
    hit_date1: Option<NaiveDateTime>,
}

impl From<TelecomUser> for InsertTelecomUser {
    fn from(user: TelecomUser) -> Self {
        // 使用 Option 的 `?` 操作符（问号）可以极大简化链式调用
        // 我们将提取逻辑放在一个立即执行的闭包中，以便使用 `?`
        let base_station = (|| user.ext.as_ref()?.base_station.as_ref())();
        let job_info = (|| user.ext.as_ref()?.job_info.as_ref())();
        let name_card = (|| user.ext.as_ref()?.name_card.as_ref())();
        let archives_info = user.archives_info.as_ref();
        let contact_info = user.contact_info.as_ref();

        Self {
            // Base Station Fields
            base_station_sequence: base_station.and_then(|bs| bs.sequence.clone()),
            base_station_code: base_station.and_then(|bs| bs.code.clone()),
            base_station_system: base_station.and_then(|bs| bs.system.clone()),
            base_station_gradesystem: base_station.and_then(|bs| bs.grade_system.clone()),
            base_station_level: base_station.and_then(|bs| bs.level.clone()),
            base_station_grade: base_station.and_then(|bs| bs.grade.clone()),
            base_station_name: base_station.and_then(|bs| bs.name.clone()),

            // Job Info Fields
            ext_job_info_jobstatus: job_info.and_then(|ji| ji.job_status.clone()),
            ext_job_info_jobcategory: job_info.and_then(|ji| ji.job_category.clone()),
            ext_job_info_hrjobtype: job_info.and_then(|ji| ji.hr_job_type.clone()),
            ext_job_info_jobtype: job_info.and_then(|ji| ji.job_type.clone()),
            job_info_positive_date: job_info.and_then(|ji| ji.positive_date),
            job_info_special_job_years: job_info.and_then(|ji| ji.special_job_years),
            job_info_work_date: job_info.and_then(|ji| ji.work_date),
            job_info_is_special_job: job_info.and_then(|ji| ji.special_job.clone()),
            job_info_leave_date: job_info.and_then(|ji| ji.leave_date),
            job_info_work_age: job_info.and_then(|ji| ji.work_age),
            job_info_is_core_staff: job_info.and_then(|ji| ji.is_core_staff.clone()),
            job_info_enterunit_date: job_info.and_then(|ji| ji.enter_unit_date),

            // Name Card Fields
            name_card_company_id: name_card.and_then(|nc| nc.company_id.clone()),
            name_card_gender: name_card.and_then(|nc| nc.gender.clone()),
            name_card_companyphone: name_card.and_then(|nc| nc.company_phone.clone()),
            name_card_organization: name_card.and_then(|nc| nc.organization.clone()),
            name_card_name: name_card.and_then(|nc| nc.name.clone()),
            name_card_station: name_card.and_then(|nc| nc.station.clone()),
            name_card_mobile: name_card.and_then(|nc| nc.mobile.clone()),
            name_card_folk: name_card.and_then(|nc| nc.folk.clone()),
            name_card_company: name_card.and_then(|nc| nc.company.clone()),
            name_card_email: name_card.and_then(|nc| nc.email.clone()),

            // Contact Info Fields
            contact_info_phone: contact_info.and_then(|ci| ci.phone.clone()),
            contact_info_mobile: contact_info.and_then(|ci| ci.mobile.clone()),
            contact_info_email: contact_info.and_then(|ci| ci.email.clone()),

            // Archives Info Fields
            archives_info_birthday: archives_info.and_then(|ai| ai.birthday),
            archives_info_isonlychild: archives_info
                .and_then(|ai| ai.is_only_child.map(|b| b.to_string()).clone()),
            archives_info_is_union_members: archives_info
                .and_then(|ai| ai.is_union_members.map(|b| b.to_string()).clone()),
            archives_info_major: archives_info.and_then(|ai| ai.major.clone()),
            archives_info_folk: archives_info.and_then(|ai| ai.folk.clone()),
            archives_info_join_union_date: archives_info.and_then(|ai| ai.join_union_date),
            archives_info_political: archives_info.and_then(|ai| ai.political.clone()),
            archives_info_party_date: archives_info.and_then(|ai| ai.party_date),
            archives_info_academy: archives_info.and_then(|ai| ai.academy.clone()),

            // Root level fields and others
            password_reset: user
                .ext
                .as_ref()
                .and_then(|e| e.password_reset.map(|b| b.to_string())),
            weight: user.ext.as_ref().and_then(|e| e.weight),
            no: user.no,
            account_type: user.account_type,
            datelastmodified: user.entity_meta_info.and_then(|emi| emi.date_last_modified),
            certificate_code: user.certificate_code,
            gender: user.gender,
            loginname: user.loginname,
            org: user.org,
            is_ehr_sync: user.is_ehr_sync.map(|b| b.to_string()),
            photo: None,
            effective_time_end: user.effective_time_end,
            user_group_ids: user.user_group_ids.map(|ids| ids.join(",")),
            d_delete: user.delete.map(|b| b.to_string()),
            is_delete: user.is_delete.map(|b| b.to_string()),
            effective_time_start: user.effective_time_start,
            encryptcertificate_code: user.encrypt_certificate_code,
            name: user.name.map(|n| n.trim().replace('\u{200b}', "")),
            id: user.id,
            certificate_type: user.certificate_type,
            status: user.status,
            hit_date: user.hit_date,
            in_time: user.in_time,
            year: user.year,
            month: user.month,
            archived_batches: None,
            hit_date1: user.hit_date1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelecomMssUser {
    pub id: Option<String>,
    pub time: Option<i64>,
    pub identity: Option<String>,
    pub code: Option<String>,
    #[serde(rename = "hrId")]
    pub hr_id: Option<String>,
    #[serde(rename = "hrCode")]
    pub hr_code: Option<String>,
    pub account: Option<String>,
    pub name: Option<String>,
    #[serde(rename = "englishName")]
    pub english_name: Option<String>,
    pub email: Option<String>,
    #[serde(rename = "organizationCode")]
    pub organization_code: Option<String>,
    #[serde(rename = "companyCode")]
    pub company_code: Option<String>,
    pub sex: Option<i32>,
    #[serde(rename = "identityCard")]
    pub identity_card: Option<String>,
    pub birthday: Option<i64>,
    #[serde(rename = "firstMobile")]
    pub first_mobile: Option<String>,
    #[serde(rename = "userStatus")]
    pub user_status: Option<i32>,
    pub sort: Option<f32>,
    #[serde(rename = "jobNumber")]
    pub job_number: Option<String>,
    #[serde(rename = "baseStation")]
    pub base_station: Option<String>,
    pub station: Option<String>,
    #[serde(rename = "stationSystem")]
    pub station_system: Option<String>,
    #[serde(rename = "stationLevel")]
    pub station_level: Option<String>,
    #[serde(rename = "stationGradeSystem")]
    pub station_grade_system: Option<String>,
    #[serde(rename = "stationGrade")]
    pub station_grade: Option<String>,
    #[serde(rename = "stationSequence")]
    pub station_sequence: Option<String>,
    #[serde(rename = "jobStatus")]
    pub job_status: Option<String>,
    #[serde(rename = "jobType")]
    pub job_type: Option<String>,
    #[serde(rename = "hrJobType")]
    pub hr_job_type: Option<String>,
    #[serde(rename = "jobCategory")]
    pub job_category: Option<String>,
    pub telephone: Option<String>,
    #[serde(rename = "standByAccount")]
    pub stand_by_account: Option<String>,
}

impl PartialEq for TelecomMssUser {
    fn eq(&self, other: &Self) -> bool {
        // 比较 hr_code 或 hr_id
        match (&self.hr_code, &other.hr_code, &self.hr_id, &other.hr_id) {
            (Some(a), Some(b), _, _) if a == b => true,
            (_, _, Some(a), Some(b)) if a == b => true,
            _ => false,
        }
    }
}

impl Eq for TelecomMssUser {}

impl Hash for TelecomMssUser {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // 使用 hr_code 和 hr_id 的组合进行哈希
        // 直接对 Option 类型的字段进行哈希
        self.hr_id.hash(state);
        self.hr_code.hash(state);
    }
}

impl PartialOrd for TelecomMssUser {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TelecomMssUser {
    fn cmp(&self, other: &Self) -> Ordering {
        // 一个辅助函数，用于安全地解析 Option<String> 为 i32，解析失败时使用 0
        let parse_or_default = |opt_s: &Option<String>| {
            opt_s
                .as_ref()
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0)
        };

        let self_job_type = parse_or_default(&self.job_type);
        let other_job_type = parse_or_default(&other.job_type);

        let self_hr_job_type = parse_or_default(&self.hr_job_type);
        let other_hr_job_type = parse_or_default(&other.hr_job_type);

        let self_time = self.time.unwrap_or(0);
        let other_time = other.time.unwrap_or(0);

        // 使用 .cmp().then_with(...) 进行优雅的链式比较
        self.user_status
            .cmp(&other.user_status)
            .then_with(|| self_job_type.cmp(&other_job_type)) // user_status升序
            .then_with(|| self_hr_job_type.cmp(&other_hr_job_type)) // job_type升序
            .then_with(|| other_time.cmp(&self_time)) // 时间降序
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelecomMssUserMapping {
    pub uid: Option<String>,
    #[serde(rename = "mssUid")]
    pub hr_code: Option<String>, // 这里是 hrCode 不唯一， hrId才是唯一的
    pub name: Option<String>,
    #[serde(rename = "certificateCode")]
    pub certificate_code: Option<String>,
    pub organization: Option<String>,
    #[serde(rename = "standardStation")]
    pub standard_station: Option<String>,
}

impl PartialEq for TelecomMssUserMapping {
    fn eq(&self, other: &Self) -> bool {
        // 比较 uid 和 mss_uid 是否都相等
        self.uid == other.uid && self.hr_code == other.hr_code
    }
}

impl Eq for TelecomMssUserMapping {}

impl Hash for TelecomMssUserMapping {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // 使用 uid 和 mss_uid 的组合进行哈希
        // 模拟 Java 中的 Objects.hashCode(uid + mssUid)
        let combined = format!(
            "{}{}",
            self.uid.as_deref().unwrap_or(""),
            self.hr_code.as_deref().unwrap_or("")
        );
        combined.hash(state);
    }
}

// 用于在处理过程中聚合所有相关数据的结构体
#[derive(Default)]
pub struct ProcessedUserData {
    pub telecom_users: Vec<TelecomUser>,
    pub mss_user_mappings: Vec<TelecomMssUserMapping>,
    pub mss_users: Vec<TelecomMssUser>,

    pub user_ids_to_delete: Vec<String>, // 根据网大ID删除d_telecom_user表以及d_mss_user_mapping表数据
    pub job_numbers_to_delete: Vec<String>, // 根据job_number删除d_mss_user表数据
    pub hr_codes_to_delete: Vec<String>, // 根据hr_code删除d_mss_user表数据
}

impl ProcessedUserData {
    /// 将另一个 ProcessedUserData 合并到自身
    pub fn merge(&mut self, other: &mut ProcessedUserData) {
        self.telecom_users.append(&mut other.telecom_users);
        self.mss_user_mappings.append(&mut other.mss_user_mappings);
        self.mss_users.append(&mut other.mss_users);

        self.user_ids_to_delete
            .append(&mut other.user_ids_to_delete);
        self.job_numbers_to_delete
            .append(&mut other.job_numbers_to_delete);
    }
}

// 定义处理状态机，用于保存每个日志的处理进度
#[derive(Debug)]
enum ProcessingState {
    // 初始状态，只有原始日志
    Initial(ModifyOperationLog),
    // 成功获取 TelecomUser，保存下来
    GotTelecomUser(ModifyOperationLog, Box<TelecomUser>), // 将大的字段（如 TelecomUser）包装在 Box 里，让枚举变体本身变得非常小，从而让整个枚举都变得小巧
    // 成功获取 TelecomMssUserMapping，保存下来
    GotMssUserMapping(ModifyOperationLog, TelecomMssUserMapping, String),
}

// 表示状态转换的结果
enum Transition {
    // 状态成功向前推进，这是新的状态
    Advanced(Box<ProcessingState>),
    // 所有步骤已成功完成，并携带最后一步获取的数据
    Completed(Box<ModifyOperationLog>, Box<TelecomMssUser>),
}

pub struct UserDataProcessor {
    app_context: Arc<AppContext>,
}

impl UserDataProcessor {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
    /// 主入口函数，包含了重试逻辑
    pub async fn process_users(&self, logs: Vec<ModifyOperationLog>) -> Result<()> {
        // 将原始日志初始化为状态机的初始状态
        let mut states_to_process: Vec<ProcessingState> =
            logs.into_iter().map(ProcessingState::Initial).collect();

        let mut final_processed_data = ProcessedUserData::default();

        for i in 0..MAX_RETRIES {
            if states_to_process.is_empty() {
                info!("All user data has been successfully processed.");
                break;
            }
            info!(
                "Processing user data, {} retry attempts remaining. Pending count: {}",
                MAX_RETRIES - i,
                states_to_process.len()
            );

            let (mut processed_data_chunk, next_states, permanent_failures) =
                self.advance_states(states_to_process).await;

            // 合并当轮成功的数据
            final_processed_data.merge(&mut processed_data_chunk);

            // 记录永久失败的日志
            if !permanent_failures.is_empty() {
                for failure in permanent_failures {
                    error!(
                        "Processing user permanently failed, will not retry. Reason: {}. Log: {:?}",
                        failure.reason, failure.log
                    );
                }
            }
            // 更新待处理列表，用于下一轮重试
            states_to_process = next_states;
        }

        // 重试次数用尽后，如果仍有未处理的状态，则记录错误
        if !states_to_process.is_empty() {
            error!(
                "Maximum retries reached, {} logs still unprocessed.",
                states_to_process.len()
            );
        }

        // 所有轮次结束后，一次性保存所有成功的数据
        match self.save_processed_data(&final_processed_data).await {
            Ok(_) => info!("All batches of user data successfully saved to database."),
            Err(e) => error!("Failed to refresh mc_org_show table: {e:?}"),
        }

        // 在 d_* 表更新成功后，调用刷新 mc_user_ztk 的逻辑
        if let Err(e) = self.refresh_mc_user_ztk(&final_processed_data).await {
            error!("Failed to refresh mc_user_ztk table: {e:?}");
        }

        Ok(())
    }

    /// 保存处理好的数据到数据库
    async fn save_processed_data(&self, data: &ProcessedUserData) -> Result<()> {
        let mut tx = self.app_context.mysql_pool.begin().await?;
        // --- 1. 执行批量刪除 ---
        info!("Starting batch deletion user of old data...");
        mysql_client::batch_delete(&mut tx, "d_telecom_user", "id", &data.user_ids_to_delete)
            .await?;
        mysql_client::batch_delete(
            &mut tx,
            "d_mss_user_mapping",
            "USERID",
            &data.user_ids_to_delete,
        )
        .await?;
        mysql_client::batch_delete(&mut tx, "d_mss_user", "HRCODE", &data.hr_codes_to_delete)
            .await?;
        mysql_client::batch_delete(
            &mut tx,
            "d_mss_user",
            "JOBNUMBER",
            &data.job_numbers_to_delete,
        )
        .await?;
        // --- 2. 执行批量插入 ---
        info!("Starting batch insertion user of new data...");
        // 1. 插入 TelecomUser
        let users_to_insert = data
            .telecom_users
            .iter()
            .cloned()
            .unique_by(|o| o.id.clone())
            .collect::<Vec<_>>();
        if !users_to_insert.is_empty() {
            self.batch_insert_telecom_users(&mut tx, users_to_insert)
                .await?;
        }
        // 2. 插入 TelecomMssUserMapping
        let mss_user_mappings_to_insert = data
            .mss_user_mappings
            .iter()
            .cloned()
            .unique_by(|o| o.uid.clone())
            .collect::<Vec<_>>();
        if !mss_user_mappings_to_insert.is_empty() {
            self.batch_insert_telecom_mss_user_mappings(&mut tx, mss_user_mappings_to_insert)
                .await?;
        }
        // 3. 插入 TelecomMssUser
        let mss_users_to_insert = data
            .mss_users
            .iter()
            .cloned()
            .unique_by(|o| o.id.clone())
            .collect::<Vec<_>>();
        if !mss_users_to_insert.is_empty() {
            self.batch_insert_telecom_mss_users(&mut tx, mss_users_to_insert)
                .await?
        }
        tx.commit().await?;
        info!("End batch insertion user of new data...");
        Ok(())
    }

    /// 状态机处理函数，驱动每个日志的状态向前演进
    async fn advance_states(
        &self,
        states: Vec<ProcessingState>,
    ) -> (
        ProcessedUserData,     // 本轮成功完成处理的数据
        Vec<ProcessingState>,  // 需要重试的状态
        Vec<PermanentFailure>, // 永久失败的日志
    ) {
        let mut processed_data = ProcessedUserData::default();
        let mut states_for_retry = Vec::new();
        let mut permanent_failures = Vec::new();

        let now = Local::now().naive_local();
        let year = now.format("%Y").to_string();
        let month = now.format("%m").to_string();

        for state in states {
            let mut current_state = state;
            // 使用 loop 来驱动单个日志的状态流转，直到成功、需要重试或永久失败
            loop {
                // 注意：这里传递的是引用，避免不必要的 clone
                let next_transition_result = match &current_state {
                    ProcessingState::Initial(log) => self.handle_initial_state(log.clone()).await,
                    // 解构时，user 是 &Box<TelecomUser> 类型
                    ProcessingState::GotTelecomUser(log, _) => {
                        self.handle_got_telecom_user_state(log.clone()).await
                    }
                    ProcessingState::GotMssUserMapping(log, _, hr_code) => {
                        self.handle_got_mss_user_mapping_state(log.clone(), hr_code.clone())
                            .await
                    }
                };

                match next_transition_result {
                    // 状态成功推进
                    Ok(Transition::Advanced(next_state_box)) => {
                        // next_state_box 是 Box<ProcessingState>
                        // 核心逻辑：立即处理上一个状态的数据
                        match &*next_state_box {
                            // 使用 * 解引用 Box
                            ProcessingState::GotTelecomUser(log, user) => {
                                // 从 Initial -> GotTelecomUser，处理 user
                                let need_insert = log.type_ == 1 || log.type_ == 2;
                                // user 是 &Box<TelecomUser>，使用 .id 会自动解引用
                                processed_data.user_ids_to_delete.push(user.id.clone());
                                if let Some(job_number) = user
                                    .ext
                                    .as_ref()
                                    .and_then(|ext| ext.authorize_info.as_ref())
                                    .and_then(|auth_info| auth_info.job_number.as_ref())
                                {
                                    processed_data
                                        .job_numbers_to_delete
                                        .push(job_number.clone());
                                }
                                if need_insert {
                                    // (**user) 从 &Box<T> 得到 T
                                    let mut user_to_insert = (**user).clone();
                                    user_to_insert.year = Some(year.clone());
                                    user_to_insert.month = Some(month.clone());
                                    user_to_insert.in_time = Some(now);
                                    user_to_insert.hit_date1 = Some(now);
                                    user_to_insert.hit_date =
                                        Some(now.format("%Y-%m-%d").to_string());
                                    processed_data.telecom_users.push(user_to_insert);
                                }
                            }
                            ProcessingState::GotMssUserMapping(log, mapping, hr_code) => {
                                // 从 GotTelecomUser -> GotMssMapping，处理 mapping 和 hr_code
                                let need_insert = log.type_ == 1 || log.type_ == 2;
                                processed_data.hr_codes_to_delete.push(hr_code.clone());
                                if need_insert {
                                    processed_data.mss_user_mappings.push(mapping.clone());
                                }
                            }
                            _ => {}
                        }
                        // 更新状态，继续循环
                        // 更新状态，从 Box 中移出值
                        current_state = *next_state_box;
                    }
                    // 所有步骤都已成功完成
                    Ok(Transition::Completed(log, mss_user)) => {
                        // 处理最后一步 mss_orgs 的数据
                        let need_insert = log.type_ == 1 || log.type_ == 2;
                        if need_insert {
                            processed_data.mss_users.push(*mss_user);
                        }
                        break; // 此日志处理完成，跳出 loop
                    }
                    // 发生超时，将当前状态加入重试列表
                    Err(ProcessError::GatewayTimeout(_)) => {
                        states_for_retry.push(current_state);
                        break; // 跳出 loop，处理下一条日志
                    }
                    // 发生永久性错误，记录并放弃
                    Err(ProcessError::Permanent(e)) => {
                        let log = match current_state {
                            ProcessingState::Initial(log) => log,
                            ProcessingState::GotTelecomUser(log, ..) => log,
                            ProcessingState::GotMssUserMapping(log, ..) => log,
                        };
                        permanent_failures.push(PermanentFailure {
                            log,
                            reason: e.to_string(),
                        });
                        break; // 跳出 loop，处理下一条日志
                    }
                }
            }
        }
        info!(
            "states_for_retry: {states_for_retry:?} len: {}",
            states_for_retry.len()
        );
        (processed_data, states_for_retry, permanent_failures)
    }

    // --- 为每个状态创建一个独立的辅助处理函数，使逻辑更清晰 ---
    async fn handle_initial_state(
        &self,
        log: ModifyOperationLog,
    ) -> Result<Transition, ProcessError> {
        match self.transform_to_telecom_user(&log).await? {
            // 成功获取，返回 Advanced 状态
            Some(user) => Ok(Transition::Advanced(Box::new(
                ProcessingState::GotTelecomUser(log, Box::new(user)),
            ))),
            None => Err(ProcessError::Permanent(anyhow::anyhow!(
                "Unable to find corresponding TelecomUser"
            ))),
        }
    }

    async fn handle_got_telecom_user_state(
        &self,
        log: ModifyOperationLog,
    ) -> Result<Transition, ProcessError> {
        let (mapping, hr_code) = self.transform_to_mss_user_mapping(&log).await?;
        // 成功获取，返回 Advanced 状态
        Ok(Transition::Advanced(Box::new(
            ProcessingState::GotMssUserMapping(log, mapping, hr_code),
        )))
    }

    async fn handle_got_mss_user_mapping_state(
        &self,
        log: ModifyOperationLog,
        hr_code: String,
    ) -> Result<Transition, ProcessError> {
        // 1. 获取 mss_users 列表
        let mss_users = self
            .transform_to_mss_users(&hr_code)
            .await?
            .ok_or_else(|| {
                ProcessError::Permanent(anyhow::anyhow!("Unable to find TelecomMssUser"))
            })?;

        // mss_users 接口返回的只有一个值，所以这里取最小没有意义了，但还是保留吧
        // 2. 使用 .iter().min() 找到优先级最高（最小）的用户
        let best_mss_user = mss_users.into_iter().min().ok_or_else(|| {
            // 3. 如果列表为空，说明没有找到任何有效用户，这是一个永久性错误
            ProcessError::Permanent(anyhow::anyhow!(
                "Found an empty TelecomMssUser list for hr_code: {}",
                hr_code
            ))
        })?;

        // 4. 成功后返回 Completed 状态，并携带单个最优用户的数据
        Ok(Transition::Completed(
            Box::new(log),
            Box::new(best_mss_user),
        ))
    }

    async fn transform_to_telecom_user(
        &self,
        log: &ModifyOperationLog,
    ) -> Result<Option<TelecomUser>, ProcessError> {
        let cid = log
            .cid
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("CID is missing for log {}", log.id))?;

        self.app_context
            .gateway_client
            .user_loadbyid(cid)
            .await
            .map_gateway_err()
    }

    async fn transform_to_mss_user_mapping(
        &self,
        log: &ModifyOperationLog,
    ) -> Result<(TelecomMssUserMapping, String), ProcessError> {
        // 1. 处理逻辑错误：如果 CID 缺失，这是一个永久性错误
        let cid = log.cid.as_deref().ok_or_else(|| {
            ProcessError::Permanent(anyhow::anyhow!("CID is missing for log {}", log.id))
        })?;

        // 2. 处理网络调用：在这里区分超时错误和其它错误
        let mapping_option = self
            .app_context
            .gateway_client
            .mss_user_translate(cid)
            .await
            .map_gateway_err()?;

        // 3. 处理逻辑错误：如果 API 调用成功但没有返回数据，这是一个永久性错误
        let mapping = mapping_option.ok_or_else(|| {
            ProcessError::Permanent(anyhow::anyhow!("MSS user not found for CID: {cid}"))
        })?;

        // 4. 处理逻辑错误：如果返回的数据缺少必要的 mss_code 字段，这也是一个永久性错误
        let hr_code = mapping.hr_code.clone().ok_or_else(|| {
            ProcessError::Permanent(anyhow::anyhow!("MSS hr_code is missing for mapping"))
        })?;

        Ok((mapping, hr_code))
    }

    async fn transform_to_mss_users(
        &self,
        hr_code: &str,
    ) -> Result<Option<Vec<TelecomMssUser>>, ProcessError> {
        self.app_context
            .gateway_client
            .mss_user_queryorder(hr_code)
            .await
            .map_gateway_err()
    }

    async fn batch_insert_telecom_users(
        &self,
        tx: &mut Transaction<'_, MySql>,
        users: Vec<TelecomUser>,
    ) -> Result<()> {
        if users.is_empty() {
            return Ok(());
        }
        // 预转换：O(n) 开销，但逻辑分离
        let insert_users: Vec<InsertTelecomUser> = users.into_iter().map(Into::into).collect();

        // 使用 QueryBuilder 安全地构建批量插入语句
        let mut query_builder = QueryBuilder::new(
            "INSERT INTO d_telecom_user (
            base_station_sequence,
            base_station_code,
            base_station_system,
            base_station_gradesystem,
            base_station_level,
            base_station_grade,
            base_station_name,
            password_reset,
            ext_job_info_jobstatus,
            ext_job_info_jobcategory,
            ext_job_info_hrjobtype,
            ext_job_info_jobtype,
            name_card_company_id,
            name_card_gender,
            name_card_companyphone,
            name_card_organization,
            name_card_name,
            name_card_station,
            name_card_mobile,
            name_card_folk,
            name_card_company,
            name_card_email,
            weight,
            no,
            account_type,
            datelastmodified,
            certificate_code,
            gender,
            loginname,
            org,
            job_info_positive_date,
            job_info_special_job_years,
            job_info_work_date,
            job_info_is_special_job,
            job_info_leave_date,
            job_info_work_age,
            job_info_is_core_staff,
            job_info_enterunit_date,
            is_ehr_sync,
            photo,
            effective_time_end,
            contact_info_phone,
            contact_info_mobile,
            contact_info_email,
            user_group_ids,
            d_delete,
            is_delete,
            effective_time_start,
            encryptcertificate_code,
            name,
            id,
            certificate_type,
            status,
            archives_info_birthday,
            archives_info_isonlychild,
            archives_info_is_union_members,
            archives_info_major,
            archives_info_folk,
            archives_info_join_union_date,
            archives_info_political,
            archives_info_party_date,
            archives_info_academy,
            hitdate,
            intime,
            year,
            month,
            archived_batches,
            hitdate1
        ) ",
        );
        query_builder.push_values(&insert_users, |mut b, user| {
            // 按照SQL语句中列的顺序绑定值
            b.push_bind(&user.base_station_sequence)
                .push_bind(&user.base_station_code)
                .push_bind(&user.base_station_system)
                .push_bind(&user.base_station_gradesystem)
                .push_bind(&user.base_station_level)
                .push_bind(&user.base_station_grade)
                .push_bind(&user.base_station_name)
                .push_bind(&user.password_reset)
                .push_bind(&user.ext_job_info_jobstatus)
                .push_bind(&user.ext_job_info_jobcategory)
                .push_bind(&user.ext_job_info_hrjobtype)
                .push_bind(&user.ext_job_info_jobtype)
                .push_bind(&user.name_card_company_id)
                .push_bind(&user.name_card_gender)
                .push_bind(&user.name_card_companyphone)
                .push_bind(&user.name_card_organization)
                .push_bind(&user.name_card_name)
                .push_bind(&user.name_card_station)
                .push_bind(&user.name_card_mobile)
                .push_bind(&user.name_card_folk)
                .push_bind(&user.name_card_company)
                .push_bind(&user.name_card_email)
                .push_bind(user.weight)
                .push_bind(&user.no)
                .push_bind(user.account_type)
                .push_bind(user.datelastmodified) // 假设 entity_meta_info 中有 datelastmodified
                .push_bind(&user.certificate_code)
                .push_bind(user.gender)
                .push_bind(&user.loginname)
                .push_bind(&user.org)
                .push_bind(user.job_info_positive_date)
                .push_bind(user.job_info_special_job_years)
                .push_bind(user.job_info_work_date)
                .push_bind(&user.job_info_is_special_job)
                .push_bind(user.job_info_leave_date)
                .push_bind(user.job_info_work_age)
                .push_bind(&user.job_info_is_core_staff)
                .push_bind(user.job_info_enterunit_date)
                .push_bind(&user.is_ehr_sync)
                .push_bind(&user.photo)
                .push_bind(user.effective_time_end)
                .push_bind(&user.contact_info_phone)
                .push_bind(&user.contact_info_mobile)
                .push_bind(&user.contact_info_email)
                .push_bind(&user.user_group_ids) // 将Vec<String>转换为字符串
                .push_bind(&user.d_delete)
                .push_bind(&user.is_delete)
                .push_bind(user.effective_time_start)
                .push_bind(&user.encryptcertificate_code)
                .push_bind(&user.name)
                .push_bind(&user.id)
                .push_bind(user.certificate_type)
                .push_bind(user.status)
                .push_bind(user.archives_info_birthday)
                .push_bind(&user.archives_info_isonlychild)
                .push_bind(&user.archives_info_is_union_members)
                .push_bind(&user.archives_info_major)
                .push_bind(&user.archives_info_folk)
                .push_bind(user.archives_info_join_union_date)
                .push_bind(&user.archives_info_political)
                .push_bind(user.archives_info_party_date)
                .push_bind(&user.archives_info_academy)
                .push_bind(&user.hit_date)
                .push_bind(user.in_time)
                .push_bind(&user.year)
                .push_bind(&user.month)
                .push_bind(&user.archived_batches)
                .push_bind(user.hit_date1);
        });
        let query = query_builder.build();
        query.execute(tx.deref_mut()).await?;
        Ok(())
    }

    async fn batch_insert_telecom_mss_user_mappings(
        &self,
        tx: &mut Transaction<'_, MySql>,
        mss_user_mappings: Vec<TelecomMssUserMapping>,
    ) -> Result<()> {
        if mss_user_mappings.is_empty() {
            return Ok(());
        }
        let mut query_builder = QueryBuilder::new(
            "INSERT INTO d_mss_user_mapping (
            standardstation,
            userid,
            certificatecode,
            organization,
            name,
            mssuid
        ) ",
        );
        query_builder.push_values(mss_user_mappings, |mut b, mss_org_mapping| {
            b.push_bind(mss_org_mapping.standard_station)
                .push_bind(mss_org_mapping.uid)
                .push_bind(mss_org_mapping.certificate_code)
                .push_bind(mss_org_mapping.organization)
                .push_bind(mss_org_mapping.name)
                .push_bind(mss_org_mapping.hr_code);
        });
        let query = query_builder.build();
        query.execute(&mut **tx).await?;
        Ok(())
    }

    async fn batch_insert_telecom_mss_users(
        &self,
        tx: &mut Transaction<'_, MySql>,
        mss_users: Vec<TelecomMssUser>,
    ) -> Result<()> {
        if mss_users.is_empty() {
            return Ok(());
        }
        let mut query_builder = QueryBuilder::new(
            "INSERT INTO d_mss_user (
            BIRTHDAY,
            ENGLISHNAME,
            JOBSTATUS,
            JOBCATEGORY,
            CODE,
            USERSTATUS,
            STATIONGRADE,
            HRCODE,
            FIRSTMOBILE,
            `IDENTITY`,
            STATION,
            STATIONSEQUENCE,
            ID,
            JOBTYPE,
            EMAIL,
            COMPANYCODE,
            SEX,
            TELEPHONE,
            IDENTITYCARD,
            SORT,
            STATIONSYSTEM,
            STANDBYACCOUNT,
            HRID,
            ORGANIZATIONCODE,
            STATIONLEVEL,
            NAME,
            STATIONGRADESYSTEM,
            BASESTATION,
            HRJOBTYPE,
            `TIME`,
            ACCOUNT,
            JOBNUMBER,
            MAPID
        ) ",
        );
        query_builder.push_values(mss_users, |mut b, mss_user| {
            b.push_bind(mss_user.birthday)
                .push_bind(mss_user.english_name)
                .push_bind(mss_user.job_status)
                .push_bind(mss_user.job_category)
                .push_bind(mss_user.code)
                .push_bind(mss_user.user_status)
                .push_bind(mss_user.station_grade)
                .push_bind(mss_user.hr_code.clone())
                .push_bind(mss_user.first_mobile)
                .push_bind(mss_user.identity)
                .push_bind(mss_user.station)
                .push_bind(mss_user.station_sequence)
                .push_bind(mss_user.hr_id.clone())
                .push_bind(mss_user.job_type)
                .push_bind(mss_user.email)
                .push_bind(mss_user.company_code)
                .push_bind(mss_user.sex)
                .push_bind(mss_user.telephone)
                .push_bind(mss_user.identity_card)
                .push_bind(mss_user.sort)
                .push_bind(mss_user.station_system)
                .push_bind(mss_user.stand_by_account)
                .push_bind(mss_user.hr_id)
                .push_bind(mss_user.organization_code)
                .push_bind(mss_user.station_level)
                .push_bind(mss_user.name)
                .push_bind(mss_user.station_grade_system)
                .push_bind(mss_user.base_station)
                .push_bind(mss_user.hr_job_type)
                .push_bind(mss_user.time)
                .push_bind(mss_user.account)
                .push_bind(mss_user.job_number)
                .push_bind(mss_user.hr_code);
        });
        let query = query_builder.build();
        query.execute(&mut **tx).await?;
        Ok(())
    }

    /// 根据受影响的组织ID，增量刷新 mc_org_show 表
    async fn refresh_mc_user_ztk(&self, data: &ProcessedUserData) -> Result<()> {
        // 1. 收集本次批次所有受影响的、唯一的组织ID
        let mut affected_ids = data
            .user_ids_to_delete
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        for user in &data.telecom_users {
            affected_ids.insert(user.id.clone());
        }
        let unique_affected_ids: Vec<String> = affected_ids.into_iter().collect();

        if unique_affected_ids.is_empty() {
            info!("No organization data changes, no need to refresh mc_org_show.");
            return Ok(());
        }
        info!(
            "Starting refresh of mc_org_show table, affected organization ID count: {}",
            unique_affected_ids.len()
        );
        // 2. 开启一个新的事务来处理刷新逻辑
        let mut tx = self.app_context.mysql_pool.begin().await?;

        // 3. (Delete) 先从 mc_user_ztk 中删除所有受影响的记录
        mysql_client::batch_delete(&mut tx, "mc_user_ztk", "ID", &unique_affected_ids).await?;

        // 4. (Insert) 重新计算并插入需要存在的数据
        //    只为那些需要新增或更新的组织（即存在于 telecom_users 列表中的）执行插入
        let ids_to_insert: Vec<String> = data.telecom_users.iter().map(|o| o.id.clone()).collect();

        if !ids_to_insert.is_empty() {
            // 4.1. 从 .sql 文件加载原始SQL
            let raw_sql_query = sqlx::query_file!("queries/refresh_mc_user_ztk.sql");

            // 4.2. 使用 QueryBuilder 附加动态的 WHERE IN 子句
            let mut query_builder = QueryBuilder::new(raw_sql_query.sql());
            query_builder.push(" WHERE TU.ID IN (");
            let mut separated = query_builder.separated(", ");
            for id in &ids_to_insert {
                separated.push_bind(id);
            }
            separated.push_unseparated(")");

            // 4.3. 构建并执行最终的查询
            let final_query = query_builder.build();
            let result = final_query.execute(tx.deref_mut()).await?;

            info!(
                "Inserted {} new records into mc_user_ztk",
                result.rows_affected()
            );
        }
        // 5. 提交事务
        tx.commit().await?;
        info!("mc_user_ztk table refresh complete.");

        Ok(())
    }
}
