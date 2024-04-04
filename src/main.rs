mod cli;
mod filters;
mod gather;
mod scanners;

#[cfg(test)]
mod tests;

use clap::{self, Parser};

use crate::cli::{Cli, GatherCommands};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cli.init();

    match cli.command {
        cli::Commands::Collect { config } => {
            Into::<GatherCommands>::into(config)
                .load()
                .await?
                .collect()
                .await?
        }
        cli::Commands::CollectFromConfig { source, overrides } => {
            source
                .gather(overrides.client().await?)
                .await?
                .merge(overrides)
                .load()
                .await?
                .collect()
                .await?
        }
        cli::Commands::Serve { serve } => serve.get_api()?.serve().await?,
    };

    log::info!("Done");
    Ok(())
}
