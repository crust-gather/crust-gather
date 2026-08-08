extern crate scopeguard;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Box::pin(crust_gather::run_cli()).await
}
