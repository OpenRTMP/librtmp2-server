mod config;
mod db;
mod http;
mod keygen;
mod logger;
mod rtmp_bridge;
mod server;

use clap::Parser;
use config::{config_apply_env, config_load, config_write_token, ServerConfig};
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
        let mut c = ServerConfig::default();
        c.config_file = cli.config.clone();
        c
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

    // Auto-generate the API token if none is stored yet. The generated token
    // is written back to the config file so it survives restarts.
    if config.api_token.is_empty() {
        let token = keygen::keygen_secret("tk_")?;
        let cfg_path = &config.config_file;
        config_write_token(cfg_path, &token)
            .map_err(|e| format!("Could not persist API token: {e}"))?;
        eprintln!(
            "============================================================\n\
             Generated API token (saved to {cfg_path}):\n\
             {token}\n\
             ============================================================"
        );
        config.api_token = token;
    }

    logger::init(config.log_level, &config.log_file);

    let result = (|| -> Result<(), String> {
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
