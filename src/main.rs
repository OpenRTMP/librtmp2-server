mod config;
mod db;
mod http;
mod keygen;
mod logger;
mod rtmp_bridge;
mod server;

use clap::Parser;
use config::{config_apply_env, config_load, ServerConfig};
use server::ServerApp;

#[derive(Parser, Debug)]
#[command(name = "librtmp2-server", disable_help_flag = true)]
struct Cli {
    /// Config file path
    #[arg(short = 'c', default_value = "config.env")]
    config: String,

    /// RTMP port (overrides config)
    #[arg(short = 'p')]
    rtmp_port: Option<u16>,

    /// HTTP port (overrides config)
    #[arg(short = 'w')]
    http_port: Option<u16>,

    /// Verbose (debug logging)
    #[arg(short = 'v')]
    verbose: bool,

    /// Show this help
    #[arg(short = 'h', action = clap::ArgAction::Help)]
    help: Option<bool>,
}

fn run() -> Result<(), String> {
    let cli = Cli::parse();

    let mut config = if std::path::Path::new(&cli.config).is_file() {
        config_load(&cli.config)?
    } else {
        eprintln!("No config file at {}, using defaults", cli.config);
        ServerConfig {
            config_file: cli.config.clone(),
            ..Default::default()
        }
    };

    // Environment variables override config file values.
    config_apply_env(&mut config);

    // CLI flags take highest priority.
    if let Some(port) = cli.rtmp_port {
        config.rtmp_bind = format!("0.0.0.0:{port}");
    }
    if let Some(port) = cli.http_port {
        config.http_bind = format!("0.0.0.0:{port}");
    }

    if cli.verbose {
        config.log_level = 3;
    }

    logger::init(config.log_level, &config.log_file);

    let result = (|| -> Result<(), String> {
        // ServerApp::create opens the database and resolves the API token.
        let app = ServerApp::create(config)?;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("Failed to start async runtime: {e}"))?;
        rt.block_on(app.run())
    })();

    logger::close();
    result
}

fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
