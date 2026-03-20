#[tokio::main]
async fn main() -> anyhow::Result<()> {
    crust_gather::run_cli().await
}
