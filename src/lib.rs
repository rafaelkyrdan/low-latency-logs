//! Low latency logs
//!

use crossbeam_channel::{Receiver, Sender, TryRecvError, bounded};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

static SEQUENCE_COUNTER: AtomicU64 = AtomicU64::new(0);
static GLOBAL_LOGGER: OnceLock<LowLatencyLogger> = OnceLock::new();
static MESSAGE_FORMATS: OnceLock<Arc<RwLock<HashMap<u32, String>>>> = OnceLock::new();

pub trait FastSerialize {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>);

    fn size_hint(&self) -> usize {
        8
    }
}

impl FastSerialize for i8 {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.push(*self as u8);
    }
    fn size_hint(&self) -> usize {
        1
    }
}

impl FastSerialize for i16 {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_le_bytes());
    }
    fn size_hint(&self) -> usize {
        2
    }
}

impl FastSerialize for i32 {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_le_bytes());
    }
    fn size_hint(&self) -> usize {
        4
    }
}

impl FastSerialize for i64 {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_le_bytes());
    }
    fn size_hint(&self) -> usize {
        8
    }
}

impl FastSerialize for u8 {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.push(*self);
    }
    fn size_hint(&self) -> usize {
        1
    }
}

impl FastSerialize for u16 {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_le_bytes());
    }
    fn size_hint(&self) -> usize {
        2
    }
}

impl FastSerialize for u32 {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_le_bytes());
    }
    fn size_hint(&self) -> usize {
        4
    }
}

impl FastSerialize for u64 {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_le_bytes());
    }
    fn size_hint(&self) -> usize {
        8
    }
}

impl FastSerialize for f32 {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_le_bytes());
    }
    fn size_hint(&self) -> usize {
        4
    }
}

impl FastSerialize for f64 {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_le_bytes());
    }
    fn size_hint(&self) -> usize {
        8
    }
}

impl FastSerialize for bool {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.push(*self as u8);
    }
    fn size_hint(&self) -> usize {
        1
    }
}

impl FastSerialize for &str {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        let len = self.len() as u32;
        buffer.extend_from_slice(&len.to_le_bytes());
        buffer.extend_from_slice(self.as_bytes());
    }
    fn size_hint(&self) -> usize {
        4 + self.len()
    }
}

impl FastSerialize for &&str {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        let len = self.len() as u32;
        buffer.extend_from_slice(&len.to_le_bytes());
        buffer.extend_from_slice(self.as_bytes());
    }
    fn size_hint(&self) -> usize {
        4 + self.len()
    }
}

impl FastSerialize for String {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        self.as_str().write_to_buffer(buffer);
    }
    fn size_hint(&self) -> usize {
        4 + self.len()
    }
}

impl FastSerialize for &String {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        self.as_str().write_to_buffer(buffer);
    }
    fn size_hint(&self) -> usize {
        4 + self.len()
    }
}

impl FastSerialize for &[u8] {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        let len = self.len() as u32;
        buffer.extend_from_slice(&len.to_le_bytes());
        buffer.extend_from_slice(self);
    }
    fn size_hint(&self) -> usize {
        4 + self.len()
    }
}

#[derive(Debug)]
pub struct LogEntry {
    pub sequence: u64,
    pub timestamp_nanos: u64,
    pub level: LogLevel,
    pub message_id: u32,
    pub data: Vec<u8>,
}

impl LogEntry {
    pub fn new(level: LogLevel, message_id: u32, estimated_size: usize) -> Self {
        let sequence = SEQUENCE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let timestamp_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        Self {
            sequence,
            timestamp_nanos,
            level,
            message_id,
            data: Vec::with_capacity(estimated_size + 32), // Extra space for metadata
        }
    }
}

#[derive(Debug, Clone)]
pub enum LogDestination {
    Stdout,
    Stderr,
    File(PathBuf),
    Multiple(Vec<LogDestination>),
}

#[derive(Debug, Clone)]
pub struct LoggerConfig {
    pub channel_capacity: usize,

    pub destinations: Vec<LogDestination>,
    pub file_buffer_size: usize,
    pub enable_cpu_affinity: bool,
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 10000,
            destinations: vec![LogDestination::Stdout],
            file_buffer_size: 8192,
            enable_cpu_affinity: true,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogLevel {
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warn => write!(f, "WARN"),
            LogLevel::Error => write!(f, "ERROR"),
        }
    }
}

pub struct OutputWriter {
    writers: Vec<Box<dyn Write + Send + 'static>>,
}

impl OutputWriter {
    pub fn new(destinations: &[LogDestination], buffer_size: usize) -> io::Result<Self> {
        let mut writers = Vec::new();
        Self::collect_writers(destinations, buffer_size, &mut writers)?;
        Ok(Self { writers })
    }

    fn collect_writers(
        destinations: &[LogDestination],
        buffer_size: usize,
        writers: &mut Vec<Box<dyn Write + Send + 'static>>,
    ) -> io::Result<()> {
        for dest in destinations {
            match dest {
                LogDestination::Stdout => {
                    writers.push(
                        Box::new(BufWriter::with_capacity(buffer_size, io::stdout()))
                            as Box<dyn Write + Send>,
                    );
                }
                LogDestination::Stderr => {
                    writers.push(
                        Box::new(BufWriter::with_capacity(buffer_size, io::stderr()))
                            as Box<dyn Write + Send>,
                    );
                }
                LogDestination::File(path) => {
                    let file = OpenOptions::new().create(true).append(true).open(path)?;
                    writers.push(Box::new(BufWriter::with_capacity(buffer_size, file))
                        as Box<dyn Write + Send>);
                }
                LogDestination::Multiple(dests) => {
                    // Recursively handle nested multiple destinations
                    Self::collect_writers(dests, buffer_size, writers)?;
                }
            }
        }
        Ok(())
    }

    pub fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        for writer in &mut self.writers {
            writer.write_all(data)?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        for writer in &mut self.writers {
            writer.flush()?;
        }
        Ok(())
    }
}

impl Drop for OutputWriter {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

pub struct LowLatencyLogger {
    sender: Sender<LogEntry>,
    _handle: std::thread::JoinHandle<()>,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl LowLatencyLogger {
    pub fn new() -> Self {
        Self::with_config(LoggerConfig::default())
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut config = LoggerConfig::default();
        config.channel_capacity = capacity;
        Self::with_config(config)
    }

    pub fn with_config(config: LoggerConfig) -> Self {
        let (sender, receiver) = bounded(config.channel_capacity);
        let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let config_clone = config.clone();
        let shutdown_clone = shutdown.clone();
        let handle = std::thread::spawn(move || {
            Self::logging_thread(receiver, config_clone, shutdown_clone);
        });

        Self {
            sender,
            _handle: handle,
            shutdown,
        }
    }

    fn logging_thread(
        receiver: Receiver<LogEntry>,
        config: LoggerConfig,
        shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) {
        #[cfg(feature = "cpu-affinity")]
        if config.enable_cpu_affinity {
            if let Some(core_ids) = core_affinity::get_core_ids() {
                if let Some(&last_core) = core_ids.last() {
                    let _ = core_affinity::set_for_current(last_core);
                }
            }
        }

        let mut output_writer =
            match OutputWriter::new(&config.destinations, config.file_buffer_size) {
                Ok(writer) => writer,
                Err(e) => {
                    eprintln!("Failed to initialize output writer: {}", e);
                    return;
                }
            };

        loop {
            match receiver.try_recv() {
                Ok(entry) => {
                    Self::process_entry(entry, &mut output_writer);
                }
                Err(TryRecvError::Empty) => {
                    if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                        let mut remaining = Vec::new();
                        while let Ok(entry) = receiver.try_recv() {
                            remaining.push(entry);
                        }

                        for entry in remaining {
                            Self::process_entry(entry, &mut output_writer);
                        }

                        let _ = output_writer.flush();
                        break;
                    }

                    std::thread::yield_now();
                }
                Err(TryRecvError::Disconnected) => {
                    let _ = output_writer.flush();
                    break;
                }
            }
        }
    }

    fn process_entry(entry: LogEntry, output_writer: &mut OutputWriter) {
        let secs = entry.timestamp_nanos / 1_000_000_000;
        let nanos = entry.timestamp_nanos % 1_000_000_000;

        let format_registry = MESSAGE_FORMATS.get().map(|r| r.read().ok()).flatten();

        let log_line = if let Some(registry) = format_registry {
            if let Some(format) = registry.get(&entry.message_id) {
                match Self::decode_and_format_data(&entry.data, format) {
                    Ok(formatted_data) => {
                        format!(
                            "[{}] seq:{} ts:{}.{:09} msg_id:{} {}\n",
                            entry.level,
                            entry.sequence,
                            secs,
                            nanos,
                            entry.message_id,
                            formatted_data
                        )
                    }
                    Err(_) => {
                        let hex_data = Self::hex_dump(&entry.data);
                        format!(
                            "[{}] seq:{} ts:{}.{:09} msg_id:{} fmt:'{}' data:{}\n",
                            entry.level,
                            entry.sequence,
                            secs,
                            nanos,
                            entry.message_id,
                            format,
                            hex_data
                        )
                    }
                }
            } else {
                let hex_data = Self::hex_dump(&entry.data);
                format!(
                    "[{}] seq:{} ts:{}.{:09} msg_id:{} data:{}\n",
                    entry.level, entry.sequence, secs, nanos, entry.message_id, hex_data
                )
            }
        } else {
            let hex_data = Self::hex_dump(&entry.data);
            format!(
                "[{}] seq:{} ts:{}.{:09} msg_id:{} data:{}\n",
                entry.level, entry.sequence, secs, nanos, entry.message_id, hex_data
            )
        };

        // Write to all configured destinations
        let _ = output_writer.write_all(log_line.as_bytes());
    }

    fn decode_and_format_data(data: &[u8], format: &str) -> Result<String, &'static str> {
        let mut data_offset = 0;

        let mut output = String::new();
        let mut i = 0;

        while i < format.len() {
            if i + 1 < format.len() && &format[i..i + 2] == "{}" {
                match Self::decode_next_arg(data, &mut data_offset) {
                    Ok(decoded_value) => {
                        output.push_str(&decoded_value);
                    }
                    Err(_) => {
                        return Err("Failed to decode binary data");
                    }
                }
                i += 2;
            } else {
                output.push(format.as_bytes()[i] as char);
                i += 1;
            }
        }

        Ok(output)
    }

    fn decode_next_arg(data: &[u8], offset: &mut usize) -> Result<String, &'static str> {
        if *offset >= data.len() {
            return Err("Not enough data");
        }

        let remaining = data.len() - *offset;

        if remaining >= 8 {
            // Could be f64, i64, u64
            let value = u64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
            *offset += 8;

            // Try to detect if this is a float by checking if it's a reasonable float value
            let as_f64 = f64::from_bits(value);
            if as_f64.is_finite() && as_f64.fract() != 0.0 {
                return Ok(format!("{:.6}", as_f64));
            } else {
                return Ok(value.to_string());
            }
        } else if remaining >= 4 {
            // Could be f32, i32, u32
            let value = u32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
            *offset += 4;

            let as_f32 = f32::from_bits(value);
            if as_f32.is_finite() && as_f32.fract() != 0.0 {
                return Ok(format!("{:.3}", as_f32));
            } else {
                return Ok(value.to_string());
            }
        } else if remaining >= 1 {
            // Could be bool, i8, u8
            let value = data[*offset];
            *offset += 1;

            if value == 0 {
                return Ok("false".to_string());
            } else if value == 1 {
                return Ok("true".to_string());
            } else {
                return Ok(value.to_string());
            }
        }

        Err("Unknown data type")
    }

    fn hex_dump(data: &[u8]) -> String {
        if data.is_empty() {
            return "[]".to_string();
        }

        let mut result = "[".to_string();
        for (i, &byte) in data.iter().enumerate() {
            if i > 0 {
                result.push(' ');
            }
            result.push_str(&format!("{:02x}", byte));
        }
        result.push(']');
        result
    }

    pub fn log(&self, entry: LogEntry) -> Result<(), LogEntry> {
        match self.sender.try_send(entry) {
            Ok(()) => Ok(()),
            Err(crossbeam_channel::TrySendError::Full(entry)) => Err(entry),
            Err(crossbeam_channel::TrySendError::Disconnected(entry)) => Err(entry),
        }
    }

    pub fn shutdown(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn wait_for_shutdown(self) {
        let _ = self._handle.join();
    }
}

/// Initializers: default, with_capacity, with_config
pub fn init_low_latency_log() -> Result<(), &'static str> {
    let logger = LowLatencyLogger::new();
    match GLOBAL_LOGGER.set(logger) {
        Ok(()) => {
            let _ = MESSAGE_FORMATS.set(Arc::new(RwLock::new(HashMap::new())));
            Ok(())
        }
        Err(_) => Err("Low latency logger already initialized"),
    }
}

pub fn init_low_latency_log_with_capacity(capacity: usize) -> Result<(), &'static str> {
    let logger = LowLatencyLogger::with_capacity(capacity);
    match GLOBAL_LOGGER.set(logger) {
        Ok(()) => {
            let _ = MESSAGE_FORMATS.set(Arc::new(RwLock::new(HashMap::new())));
            Ok(())
        }
        Err(_) => Err("Low latency logger already initialized"),
    }
}

pub fn init_low_latency_log_with_config(config: LoggerConfig) -> Result<(), &'static str> {
    let logger = LowLatencyLogger::with_config(config);
    match GLOBAL_LOGGER.set(logger) {
        Ok(()) => {
            let _ = MESSAGE_FORMATS.set(Arc::new(RwLock::new(HashMap::new())));
            Ok(())
        }
        Err(_) => Err("Low latency logger already initialized"),
    }
}

pub fn register_message_format(message_id: u32, format: String) -> Result<(), &'static str> {
    if let Some(formats) = MESSAGE_FORMATS.get() {
        if let Ok(mut registry) = formats.write() {
            registry.insert(message_id, format);
            Ok(())
        } else {
            Err("Failed to acquire write lock on message formats")
        }
    } else {
        Err("Low latency logger not initialized")
    }
}

#[doc(hidden)]
pub fn __send_log_entry(entry: LogEntry) {
    if let Some(logger) = GLOBAL_LOGGER.get() {
        let _ = logger.log(entry);
    }
}

pub fn get_global_logger() -> Option<&'static LowLatencyLogger> {
    GLOBAL_LOGGER.get()
}

pub fn shutdown_low_latency_log() {
    if let Some(logger) = GLOBAL_LOGGER.get() {
        logger.shutdown();
    }
}

pub fn wait_for_shutdown() {
    if let Some(logger) = GLOBAL_LOGGER.get() {
        logger.shutdown();
    }
}

#[macro_export]
macro_rules! __size_hint {
    () => { 0 };
    ($arg:expr) => { $arg.size_hint() };
    ($arg:expr, $($rest:expr),+) => { $arg.size_hint() + __size_hint!($($rest),+) };
}

#[macro_export]
macro_rules! low_latency_log {
    ($level:expr, $msg_id:expr $(, $arg:expr)*) => {{
        let estimated_size = 0 $(+ $arg.size_hint())*;
        let mut entry = $crate::LogEntry::new($level, $msg_id, estimated_size);

        $(
            $crate::FastSerialize::write_to_buffer(&$arg, &mut entry.data);
        )*

        $crate::__send_log_entry(entry);
    }};
}

/// Logging by levels: debug, info, warn, error
#[macro_export]
macro_rules! low_latency_debug {
    ($msg_id:expr $(, $arg:expr)*) => {
        $crate::low_latency_log!($crate::LogLevel::Debug, $msg_id $(, $arg)*)
    };
}

#[macro_export]
macro_rules! low_latency_info {
    ($msg_id:expr $(, $arg:expr)*) => {
        $crate::low_latency_log!($crate::LogLevel::Info, $msg_id $(, $arg)*)
    };
}

#[macro_export]
macro_rules! low_latency_warn {
    ($msg_id:expr $(, $arg:expr)*) => {
        $crate::low_latency_log!($crate::LogLevel::Warn, $msg_id $(, $arg)*)
    };
}

#[macro_export]
macro_rules! low_latency_error {
    ($msg_id:expr $(, $arg:expr)*) => {
        $crate::low_latency_log!($crate::LogLevel::Error, $msg_id $(, $arg)*)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_logging() {
        // Only initialize if not already done
        let _ = init_low_latency_log();

        let volume = 100.02f64;
        let price = 20000.0f64;
        let flag = true;
        let symbol = "AAPL";

        low_latency_info!(1001, volume, price, flag, symbol);
        low_latency_debug!(1002, price, symbol);
        low_latency_warn!(1003, flag);
        low_latency_error!(1004, 42i32);

        // Give the logger time to process
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    #[test]
    fn test_message_formats() {
        // Only initialize if not already done
        let _ = init_low_latency_log();
        let _ = register_message_format(2001, "Test message: value={}".to_string());

        low_latency_info!(2001, 123.45f64);

        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}
