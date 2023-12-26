mod filter;
mod resource;
mod scanners;

#[cfg(test)]
mod tests;

use anyhow;
use clap;
use filter::NamespaceInclude;
use kube::Client;
use resource::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cmd = clap::Command::new("crust-gather")
        .bin_name("crust-gather")
        .subcommand_required(true)
        .subcommand(
            clap::command!("collect")
                .arg(
                    clap::arg!(-i --namespaces <NAMESPACES> "list of namespaces for the objects to collect. Empty value includes all namespaces")
                        .value_parser(clap::value_parser!(NamespaceInclude)),
                )
        );

    let matches = cmd.get_matches();
    let matches = match matches.subcommand() {
        Some(("collect", matches)) => matches,
        _ => unreachable!("unknown subcommand"),
    };

    let config = Config {
        client: Client::try_default().await?,
        filter: matches
            .get_one("namespaces")
            .unwrap_or(&NamespaceInclude::default())
            .clone(),
    };
    config.collect().await?;
    Ok(())
}
