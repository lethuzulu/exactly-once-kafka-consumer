use exactly_once_kafka_consumer::{config::ConsumerConfig, consumer::KafkaConsumer, db::Db};
use tokio::sync::watch;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(fmt::layer().json())
        .init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let config       = ConsumerConfig::from_env()?;

    let db       = Db::new(&database_url).await?;
    db.migrate().await?;

    let consumer = KafkaConsumer::new(config, db)?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let handle = tokio::spawn(async move {
        if let Err(e) = consumer.run(shutdown_rx).await {
            tracing::error!(error = %e, "consumer exited with error");
        }
    });

    tokio::signal::ctrl_c().await?;
    tracing::info!("SIGINT received — shutting down");

    shutdown_tx.send(true)?;
    handle.await?;

    tracing::info!("shutdown complete");
    Ok(())
}