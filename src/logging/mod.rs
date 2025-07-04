use anyhow::{Context, Result};
use chrono::Local;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::{self, filter::EnvFilter, fmt, prelude::*, util::SubscriberInitExt};

struct LocalTimer;

impl FormatTime for LocalTimer {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", Local::now().format("%Y-%m-%d %H:%M:%S%.3f"))
    }
}

pub fn init_logging() -> Result<()> {
    // 1. 初始化 tracing 日志系统
    // 创建一个按天轮转、文件大小超过 400MB 再切分的滚动文件 appender
    let file_appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY) // 按天轮转
        .max_log_files(30) // 保留最近 30 天的日志文件
        .filename_prefix("app") // 日志文件名的前缀，例如 app.log.2025-07-01
        .filename_suffix("log")
        .build("logs") // 日志文件存储的目录
        .context("Failed to create rolling file appender")?;

    // 创建一个 fmt 层用于文件输出，指定日期时间格式和日志内容格式
    let file_layer = fmt::layer()
        .with_ansi(false) // 文件输出通常不需要 ANSI 颜色
        .with_writer(file_appender) // 将日志写入滚动文件
        .with_target(true) // 显示 target (通常是模块路径)
        .with_timer(LocalTimer) // 使用定义的本地时间格式
        .with_thread_ids(true) // 显示线程 ID
        .with_thread_names(true) // 显示线程名称
        .with_line_number(true) // 显示行号
        .with_file(true) // 显示文件名
        .with_level(true) // 显示日志级别
        .with_filter(EnvFilter::new("info")); // 文件日志级别，可以通过 RUST_LOG 环境变量覆盖

    // 创建一个 fmt 层用于控制台输出
    let stdout_layer = fmt::layer()
        .with_ansi(true) // 控制台输出可以有颜色
        .with_timer(LocalTimer) // 使用定义的本地时间格式
        .with_thread_ids(true) // 显示线程 ID
        .with_thread_names(true) // 显示线程名称
        .with_line_number(true) // 显示行号
        .with_file(true) // 显示文件名
        .with_level(true) // 显示日志级别
        .with_filter(EnvFilter::new("info")); // 控制台日志级别

    // 将两个层组合起来并初始化全局订阅者
    tracing_subscriber::registry()
        .with(stdout_layer) // 控制台输出
        .with(file_layer) // 文件输出
        .init();

    Ok(())
}
