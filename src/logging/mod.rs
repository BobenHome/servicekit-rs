use anyhow::{Context, Result};
use chrono::{Datelike, Local};
use std::{
    fs::{self, File, OpenOptions},
    io::{self},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::{self, filter::EnvFilter, fmt, prelude::*, util::SubscriberInitExt};

// 自定义本地时间格式，保持不变
struct LocalTimer;

impl FormatTime for LocalTimer {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", Local::now().format("%Y-%m-%d %H:%M:%S%.3f"))
    }
}

// =====================================================================
// Custom Local-Time Rolling Writer Implementation
// = ===================================================================

/// 一个封装了 `Arc<Mutex<File>>` 的写入器，实现了 `io::Write`。
struct LogFileWriter(Arc<Mutex<File>>);

impl io::Write for LogFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

/// 实现 `MakeWriter` 特性，用于自定义文件写入和轮转逻辑。
struct LocalTimeRollingWriter {
    active_file: Arc<Mutex<File>>,
    current_day: Mutex<u32>, // 存储当前是哪一天 (月份中的日期)
    log_dir: PathBuf,
}

impl LocalTimeRollingWriter {
    pub fn new(log_dir: &str) -> Result<Self> {
        let log_path = PathBuf::from(log_dir);
        fs::create_dir_all(&log_path)
            .context(format!("Failed to create log directory: {:?}", log_path))?;

        let now = Local::now();
        let current_day = now.day(); // 依赖 Datelike

        let file_path = Self::get_daily_log_path(&log_path, &now);

        let file = OpenOptions::new()
            .create(true)
            .append(true) // 追加模式
            .open(&file_path)
            .context(format!("Failed to open initial log file: {:?}", file_path))?;

        Ok(LocalTimeRollingWriter {
            active_file: Arc::new(Mutex::new(file)),
            current_day: Mutex::new(current_day),
            log_dir: log_path,
        })
    }

    // 根据本地日期生成日志文件路径
    fn get_daily_log_path(base_dir: &Path, now: &chrono::DateTime<Local>) -> PathBuf {
        let file_name = format!("app.{}.log", now.format("%Y-%m-%d"));
        base_dir.join(file_name)
    }

    // Handles the rotation logic internally.
    // If opening the new file fails, it logs the error but *does not* replace
    // the current active_file. This means logs will continue to go to the old file.
    fn ensure_current_file_is_correct(&self) -> Result<()> {
        // Returns anyhow::Result
        let now = Local::now();
        let today = now.day();

        let mut current_day_guard = self.current_day.lock().unwrap();

        // 每日轮转逻辑
        if *current_day_guard != today {
            println!(
                "INFO: Daily log rotation triggered. Old day: {}, New day: {}",
                *current_day_guard, today
            );
            *current_day_guard = today; // 更新记录的日期

            let new_file_path = Self::get_daily_log_path(&self.log_dir, &now);

            match OpenOptions::new()
                .create(true)
                .append(true)
                .open(&new_file_path)
            {
                Ok(new_file) => {
                    // 仅在此处锁定 active_file_guard 以替换 File
                    let mut active_file_guard = self.active_file.lock().unwrap();
                    *active_file_guard = new_file; // 替换 Mutex 内部的 File
                    println!("INFO: New log file opened: {:?}", new_file_path);
                }
                Err(e) => {
                    eprintln!(
                        "ERROR: Failed to open new daily log file {:#?}: {:?}",
                        new_file_path, e
                    );
                    // 如果打开新文件失败，不替换旧文件。
                    // 日志将继续写入到上一天的文件。
                }
            }
        }
        Ok(()) // 始终返回 Ok，因为错误已在内部处理
    }
}

// Implement MakeWriter for LocalTimeRollingWriter
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LocalTimeRollingWriter {
    type Writer = LogFileWriter;

    fn make_writer(&self) -> Self::Writer {
        // 确保正确的文件已打开并处于活动状态。
        // 此函数现在在内部处理自己的错误，并通过打印来确保 `active_file` 始终持有有效的 `File`
        //（要么是旧文件，要么是新打开的文件）。
        let _ = self.ensure_current_file_is_correct(); // 调用并忽略 Result，因为错误已在内部处理

        // 始终返回一个指向当前活动文件的 LogFileWriter。
        // 如果 ensure_current_file_is_correct 未能打开新文件，
        // 这仍将返回一个指向旧文件的写入器。
        LogFileWriter(Arc::clone(&self.active_file))
    }
}

// =====================================================================
// Log Initialization Function
// =====================================================================

/// 初始化应用程序的 tracing 日志系统。
///
/// 配置包括：
/// - 控制台输出层，使用本地时间、线程ID/名称、文件名/行号和日志级别。
/// - 文件输出层，按天轮转，使用本地时间命名日志文件，并包含详细日志信息。
/// - 注意：此自定义实现目前只支持按天轮转，不包含按文件大小切分。
pub fn init_logging() -> Result<()> {
    // 创建自定义的本地时间滚动文件写入器
    let custom_file_writer = LocalTimeRollingWriter::new("logs")?;

    // 创建一个 fmt 层用于文件输出
    let file_layer = fmt::layer()
        .with_ansi(false) // 文件输出通常不需要 ANSI 颜色
        .with_writer(custom_file_writer) // 使用自定义写入器
        .with_target(true)
        .with_timer(LocalTimer) // 使用定义的本地时间格式
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_line_number(true)
        .with_file(true)
        .with_level(true)
        .with_filter(EnvFilter::new("info"));

    // 创建一个 fmt 层用于控制台输出
    let stdout_layer = fmt::layer()
        .with_ansi(true) // 控制台输出可以有颜色
        .with_timer(LocalTimer)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_line_number(true)
        .with_file(true)
        .with_level(true)
        .with_filter(EnvFilter::new("info"));

    // 将两个层组合起来并初始化全局订阅者
    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(file_layer)
        .init();

    Ok(())
}
