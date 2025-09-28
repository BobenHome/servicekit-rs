use anyhow::{Context, Result};
use chrono::{Local, NaiveDate};
use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::{self, File};
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
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

// 新增：压缩旧日志文件的函数
fn compress_old_logs(log_dir: &Path) -> Result<()> {
    let today = Local::now().date_naive();
    let entries = fs::read_dir(log_dir).context("Failed to read log directory")?;

    for entry in entries {
        let entry = entry.context("Failed to read directory entry")?;
        let path = entry.path();
        if path.is_file() && path.extension().map_or(false, |ext| ext == "log") {
            if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                if let Some(date_str) = file_name
                    .strip_prefix("app.")
                    .and_then(|s| s.strip_suffix(".log"))
                {
                    if let Ok(file_date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                        if file_date < today {
                            // 压缩文件为 .gz
                            let gz_path = path.with_extension("log.gz");
                            let input = File::open(&path)
                                .context(format!("Failed to open {path:?} for compression"))?;
                            let output = File::create(&gz_path)
                                .context(format!("Failed to create {gz_path:?}"))?;
                            let mut encoder = GzEncoder::new(output, Compression::default());
                            let mut reader = BufReader::new(input);
                            io::copy(&mut reader, &mut encoder)
                                .context("Failed to copy data during compression")?;
                            encoder.finish().context("Failed to finish GzEncoder")?;

                            // 删除原文件
                            fs::remove_file(&path)
                                .context(format!("Failed to remove original file {path:?}"))?;

                            println!("INFO: Compressed old log file {path:?} to {gz_path:?}");
                        }
                    }
                }
            }
        }
    }
    Ok(())
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

    // 新增：初始化时压缩旧日志文件
    if let Err(e) = compress_old_logs(&log_dir) {
        eprintln!("Warning: Failed to compress old logs: {e:?}");
        // 不中断初始化，继续
    }

    // 使用 tracing-appender 创建按天轮转的文件 appender
    let appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY) // 按本地日期每天轮转
        .filename_prefix("app") // 前缀：app
        .filename_suffix("log") // 后缀：log（结果：app.YYYY-MM-DD.log）
        .build(&log_dir) // 日志目录
        .context("Failed to build rolling file appender")?;

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
