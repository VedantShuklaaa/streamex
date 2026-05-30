use crate::{
    core::types::{Exchange, TradeCallback},
    models::normalized::NormalizedResponse,
};
use std::sync::Arc;
use tokio::sync::broadcast;

pub struct SubscriptionManager {
    pub tx: Arc<broadcast::Sender<NormalizedResponse>>,
}

impl SubscriptionManager {
    pub fn new(tx: Arc<broadcast::Sender<NormalizedResponse>>) -> Self {
        Self { tx }
    }

    pub fn subscribe(
        &self,
        symbol: String,
        exchange: Exchange,
        callback: TradeCallback,
    ) -> tokio::task::JoinHandle<()> {
        let mut rx = self.tx.subscribe();

        let expected = match exchange {
            Exchange::Coinbase => symbol.replace("USDT", "/USD"),
            Exchange::Okx => symbol.replace("USDT", "/USDT"),

            _ => symbol.clone(),
        };

        tokio::spawn(async move {
            while let Ok(data) = rx.recv().await {
                if data.symbol != expected {
                    continue;
                }

                callback(data);
            }
        })
    }
}
