mod config;
mod http;
mod standalone;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use serde_json::json;

#[derive(Parser)]
#[command(
    name = "kafkaopsdb",
    version,
    about = "Index of Kafka message metadata, queryable with SQL"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run indexer, compaction, vacuum and the query API in one process.
    Standalone {
        #[arg(short, long, default_value = "kafkaopsdb.toml")]
        config: PathBuf,
    },
    /// Run a SQL query against a running server.
    Sql {
        #[arg(
            long,
            env = "KAFKAOPSDB_SERVER",
            default_value = "http://localhost:8080"
        )]
        server: String,
        /// table, json or ndjson
        #[arg(short, long, default_value = "table")]
        format: String,
        query: String,
    },
    /// Show table and ingest status of a running server.
    Status {
        #[arg(
            long,
            env = "KAFKAOPSDB_SERVER",
            default_value = "http://localhost:8080"
        )]
        server: String,
    },
}

fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .new_agent()
}

fn sql(server: &str, format: &str, query: &str) -> Result<(), String> {
    let mut resp = agent()
        .post(&format!("{}/v1/query", server.trim_end_matches('/')))
        .send_json(json!({ "sql": query, "format": format }))
        .map_err(|e| e.to_string())?;
    let ok = resp.status().is_success();
    let body = resp
        .body_mut()
        .read_to_string()
        .map_err(|e| e.to_string())?;
    if !ok {
        let msg = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v["error"].as_str().map(str::to_owned))
            .unwrap_or(body);
        return Err(msg);
    }
    if format == "json" {
        let v: serde_json::Value = serde_json::from_str(&body).map_err(|e| e.to_string())?;
        println!("{}", serde_json::to_string_pretty(&v).unwrap());
    } else {
        print!("{body}");
    }
    Ok(())
}

fn status(server: &str) -> Result<(), String> {
    let mut resp = agent()
        .get(&format!("{}/v1/status", server.trim_end_matches('/')))
        .call()
        .map_err(|e| e.to_string())?;
    let body = resp
        .body_mut()
        .read_to_string()
        .map_err(|e| e.to_string())?;
    let v: serde_json::Value = serde_json::from_str(&body).map_err(|e| e.to_string())?;
    println!("{}", serde_json::to_string_pretty(&v).unwrap());
    Ok(())
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Standalone { config } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "info,datafusion=warn".into()),
                )
                .init();
            match config::Config::load(&config) {
                Ok(cfg) => tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| e.to_string())
                    .and_then(|rt| rt.block_on(standalone::run(cfg))),
                Err(e) => Err(e),
            }
        }
        Command::Sql {
            server,
            format,
            query,
        } => sql(&server, &format, &query),
        Command::Status { server } => status(&server),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
