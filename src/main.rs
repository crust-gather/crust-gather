mod resource;

use anyhow;
use clap;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cmd = clap::Command::new("rust-gather")
        .bin_name("rust-gather")
        .subcommand_required(true)
        .subcommand(
            clap::command!("collect")
                .arg(
                    clap::arg!(-n --namespace <NAMESPACE> "namespace for the objects to collect")
                        .value_parser(clap::value_parser!(String)),
                )
                .arg_required_else_help(true),
        );
    let matches = cmd.get_matches();
    let matches = match matches.subcommand() {
        Some(("collect", matches)) => matches,
        _ => unreachable!("unknown subcommand"),
    };

    resource::collect(matches.get_one::<String>("namespace").unwrap()).await?;

    Ok(())
}
