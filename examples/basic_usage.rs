use low_latency_logs::*;
use low_latency_logs::{
    init_low_latency_log, low_latency_debug, low_latency_info, low_latency_warn,
};
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Low Latency Log - Basic Usage Example");
    println!("=====================================");

    init_low_latency_log()?;

    register_message_format(
        1001,
        "Trade executed: volume={}, price={}, symbol={}".to_string(),
    )?;
    register_message_format(
        1002,
        "Order book update: symbol={}, bid={}, ask={}".to_string(),
    )?;
    register_message_format(
        1003,
        "System status: component={}, status={}, load={:.2}".to_string(),
    )?;

    println!("Logger initialized with message formats.");
    println!("Starting to log messages...\n");

    let mut trade_count = 0;
    let mut price = 150.0;
    let symbols = ["AAPL", "GOOGL", "MSFT", "TSLA"];

    for i in 0..10 {
        // Simulate trade execution
        trade_count += 1;
        price += (rand::random::<f64>() - 0.5) * 2.0; // Random price movement
        let volume = 100.0 + rand::random::<f64>() * 900.0; // 100-1000 shares
        let symbol = symbols[i % symbols.len()];

        low_latency_info!(1001, volume, price, &symbol);

        // Simulate order book update
        let bid = price - 0.01;
        let ask = price + 0.01;
        low_latency_info!(1002, &symbol, bid, ask);

        // Simulate system status
        let cpu_load = 45.0 + rand::random::<f64>() * 30.0;
        low_latency_info!(1003, "trading_engine", "healthy", cpu_load);

        // Add some debug and warning messages
        if i % 3 == 0 {
            low_latency_debug!(2001, "Processing batch", i as i32);
        }

        if cpu_load > 70.0 {
            low_latency_warn!(2002, "High CPU load detected", cpu_load);
        }

        // Small delay to simulate real-time processing
        thread::sleep(Duration::from_millis(100));
    }

    println!("\nExample completed! Check the output above to see the logged messages.");
    println!(
        "Notice how the binary data is automatically decoded and formatted based on the registered message formats."
    );

    // Give the logger time to process all messages
    thread::sleep(Duration::from_millis(100));

    Ok(())
}
