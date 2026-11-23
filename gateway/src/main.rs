#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pybox_gateway::cli::run("pybox-gateway", None).await
}
