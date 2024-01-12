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

    let config = cli.command.load().await?;

    config.collect().await?;

    log::info!("Done");
    Ok(())
}
