pub mod cli;
pub mod filters;
pub mod gather;
pub mod mcp_server;
pub mod scanners;

use clap::Parser;

pub async fn run_cli() -> anyhow::Result<()> {
    let cli = cli::Cli::parse();
    cli.init();

    cli.command.run().await?;

    tracing::info!("Done");
    Ok(())
}
