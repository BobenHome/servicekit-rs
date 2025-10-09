use anyhow::{Context, Result};
use chrono::Local;
use logroller::{Compression, LogRollerBuilder, Rotation, RotationAge, TimeZone};
use std::fs::{self};
use std::path::PathBuf;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::{self, filter::EnvFilter, fmt, prelude::*, util::SubscriberInitExt};

// 自定义本地时间格式，保持不变
pub struct LocalTimer;

impl FormatTime for LocalTimer {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", Local::now().format("%Y-%m-%d %H:%M:%S%.3f"))
    }
}

// =====================================================================
// Log Initialization Function
// =====================================================================

/// 初始化应用程序的 tracing 日志系统。
///
/// 配置包括：
/// - 控制台输出层，使用本地时间、线程ID/名称、文件名/行号和日志级别。
/// - 文件输出层，使用 tracing-appender 按天轮转（文件名如 app.YYYY-MM-DD.log），并在初始化时压缩旧日志文件。
/// - 注意：压缩使用 Gz 格式，仅在初始化时执行（不实时）。
pub fn init_logging() -> Result<WorkerGuard> {
    let log_dir = PathBuf::from("logs");
    fs::create_dir_all(&log_dir).context(format!("Failed to create log directory: {log_dir:?}"))?;

    // 使用 logroller 创建按本地时区每天轮转的文件 appender
    let appender = LogRollerBuilder::new("logs", "app") // 目录和基础文件名（会生成 app.YYYY-MM-DD.log）
        .rotation(Rotation::AgeBased(RotationAge::Daily)) // 每天轮转
        .suffix("log".to_string())
        .time_zone(TimeZone::Local) // 使用本地时区（东八区）
        .compression(Compression::Gzip) // 自动压缩旧文件为 .gz
        .max_keep_files(30) // 可选：保留最近 30 个文件，防止无限增长
        .build()
        .context("Failed to build logroller appender")?;

    // 创建非阻塞 writer（异步写入）
    let (non_blocking, guard) = tracing_appender::non_blocking(appender);

    // 创建一个 fmt 层用于文件输出
    let file_layer = fmt::layer()
        .with_ansi(false) // 文件输出通常不需要 ANSI 颜色
        .with_writer(non_blocking) // 使用 tracing-appender 的 writer
        .with_target(true)
        .with_timer(LocalTimer) // 使用定义的本地时间格式
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_line_number(true)
        .with_file(true)
        .with_level(true)
        .with_filter(EnvFilter::new("info")); // 文件日志通常使用 info 级别

    // 创建一个 fmt 层用于控制台输出
    let stdout_layer = fmt::layer()
        .with_ansi(true) // 控制台输出可以有颜色
        .with_timer(LocalTimer)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_line_number(true)
        .with_file(true)
        .with_level(true)
        .with_filter(EnvFilter::new("debug")); // 控制台日志通常使用 debug 级别

    // 将两个层组合起来并初始化全局订阅者
    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(file_layer)
        .init();

    Ok(guard)
}
