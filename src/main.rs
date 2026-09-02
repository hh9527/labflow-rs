use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    labflow::cli::run().await
}
