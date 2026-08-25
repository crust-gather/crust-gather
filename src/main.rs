extern crate scopeguard;

#[tokio::main]
#[snafu::report]
async fn main() -> Result<(), snafu::Whatever> {
    Box::pin(crust_gather::run_cli()).await
}
