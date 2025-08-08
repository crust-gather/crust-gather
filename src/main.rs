mod cli;
mod filters;
mod gather;
mod scanners;

use clap::{self, Parser};

use crate::cli::Cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cli.init();

    cli.command.run().await?;

    tracing::info!("Done");
    Ok(())
}
