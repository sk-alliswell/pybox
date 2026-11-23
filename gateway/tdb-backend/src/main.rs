use std::path::Path;
use std::sync::Arc;

use pybox_gateway::obs::store::Backend;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pybox_gateway::cli::run(
        "pybox-gateway-tdb",
        Some(("tdb", |cfg| {
            let db =
                pybox_gateway_tdb::Store::open(Path::new(&cfg.dir)).map_err(anyhow::Error::msg)?;
            Ok(Arc::new(db) as Arc<dyn Backend>)
        })),
    )
    .await
}
