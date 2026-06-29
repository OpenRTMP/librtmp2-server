mod config;
mod db;
mod http;
mod keygen;
mod logger;
mod rtmp_bridge;
mod server;

use clap::Parser;
use config::{config_api_token_usable, config_apply_env, config_load, ServerConfig};
use server::ServerApp;

#[derive(Parser, Debug)]
#[command(name = "librtmp2-server", disable_help_flag = true)]
struct Cli {
    /// Config file path
    #[arg(short = 'c', default_value = "config.json")]
    config: String,

    /// RTMP port (overrides config)
    #[arg(short = 'p')]
    rtmp_port: Option<u16>,

    /// HTTP port (overrides config)
    #[arg(short = 'w')]
    http_port: Option<u16>,

    /// API token (overrides config)
    #[arg(short = 't')]
    api_token: Option<String>,

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
        ServerConfig::default()
    };

    // Environment variables override config file values.
    config_apply_env(&mut config);

    // CLI flags take highest priority: applied after config file load and
    // env application.
    if let Some(port) = cli.rtmp_port {
        config.rtmp_bind = format!("0.0.0.0:{port}");
    }
    if let Some(port) = cli.http_port {
        config.http_bind = format!("0.0.0.0:{port}");
    }
    if let Some(token) = cli.api_token {
        config.api_token = token;
    }

    if cli.verbose {
        config.log_level = 3;
    }

    // Refuse to start with missing, placeholder, or otherwise weak API
    // tokens. An empty token would bypass Bearer auth; the shipped config
    // placeholder is public knowledge and must not be accepted as real.
    if !config_api_token_usable(&config.api_token) {
        return Err(format!(
            "FATAL: auth.api_token is missing or uses a known weak placeholder. \
             Set a strong random token in {}, via -t, or LRTMP2_API_TOKEN.",
            cli.config
        ));
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
