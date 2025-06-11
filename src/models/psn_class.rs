use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct PsnClass {
    pub _id: String,
    pub id: String,
    pub trainingId: String,
    pub training_name: String,
    pub org_class: Option<String>,
}
