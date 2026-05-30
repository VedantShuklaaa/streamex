use crate::{core::types::TradeCallback, models::normalized::NormalizedResponse};
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
		callback: TradeCallback,
	) -> tokio::task::JoinHandle<()> {
		let mut rx = self.tx.subscribe();

		tokio::spawn(async move {
			while let Ok(data) = rx.recv().await {
				if data.symbol != symbol {
					continue;
				}

				callback(data);
			}
		})
	}
}
