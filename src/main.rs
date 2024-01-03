mod cli;
mod filters;
mod gather;
mod scanners;

#[cfg(test)]
mod tests;

use anyhow;
use clap::{self, Parser};
use log;

use crate::cli::Cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cli.init();

    let mut config = cli.command.load().await?;

    log::info!("Collecting resources");
    config.collect().await?;

    log::info!("Done");
    Ok(())
}
