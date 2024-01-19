#![warn(
    clippy::all,
    // clippy::restriction,
    clippy::pedantic,
    // clippy::nursery,
    // clippy::cargo,
)]

mod cli;
mod filters;
mod gather;
mod scanners;

#[cfg(test)]
mod tests;

use clap::{self, Parser};

use crate::cli::Cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cli.init();

    cli.command.load().await?.collect().await?;

    log::info!("Done");
    Ok(())
}
