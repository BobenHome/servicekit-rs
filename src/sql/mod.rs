use std::fs;
use std::path::Path;

pub fn load_sql(name: &str) -> String {
    let sql_path = Path::new("src/sql").join(format!("{}.sql", name));
    fs::read_to_string(sql_path).unwrap_or_else(|_| panic!("Failed to load SQL file: {}", name))
}
