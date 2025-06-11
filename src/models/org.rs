use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct McOrgShow {
    pub id: String,
    pub name: Option<String>,
    pub parent: Option<String>,
    pub globle: Option<String>,
    pub log_sdate: String,
}
