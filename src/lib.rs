use anyhow::Result;
use std::pin::Pin;
use std::{any::type_name, future::Future};
// 定义一个 trait，用于所有可以被调度器执行的任务
// 它们必须是 Send + Sync + 'static (线程安全，可在线程间移动，且生命周期静态)
// 并且提供一个返回 Result<()> 的异步执行方法
pub trait TaskExecutor: Send + Sync + 'static {
    // 获取任务名称
    fn name(&self) -> &str {
        let full_name = type_name::<Self>();
        full_name.rsplit("::").next().unwrap_or("Unknown")
    }
    fn execute(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
}

pub mod config;
pub mod context;
pub mod db;
pub mod logging;
pub mod mappers;
pub mod models;
pub mod parsers;
pub mod schedule;
pub mod utils;
pub mod web;

pub use models::train::{ClassData, DynamicPsnData, LecturerData, PsnDataKind};
pub use web::WebServer;

pub use config::{AppConfig, ClickhouseConfig, MssInfoConfig};
pub use mappers::archiving_mss_mapper::{ArchivingMssMapper, RecordMssReply};
pub use parsers::push_result_parser::PushResultParser;

pub use context::AppContext;
pub use utils::mss_client::psn_dos_push;
