use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct PsnClass {
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
