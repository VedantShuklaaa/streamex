use crate::{
    core::{
        engine::start_engine,
        subscription_manager::SubscriptionManager,
        types::{EngineCommand, EngineHandle, Exchange, TradeCallback},
    },
    exchanges::binance::adapter::BinanceAdapter,
    models::normalized::NormalizedResponse,
};
use std::{collections::HashMap, sync::Arc};
use tokio::{runtime::Runtime, sync::{broadcast, mpsc}};

pub struct Streamex {
    engines: HashMap<Exchange, EngineHandle>,
    pub manager: SubscriptionManager,
    subscriptions: Vec<tokio::task::JoinHandle<()>>,
}

impl Streamex {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel::<NormalizedResponse>(100);
        let tx = Arc::new(tx);
        let manager = SubscriptionManager::new(tx);

        Self {
            engines: HashMap::new(),
            manager,
            subscriptions: Vec::new(),
        }
    }

    pub async fn trades<F>(
        &mut self, 
        runtime: Arc<Runtime>,
        exchange: Exchange, 
        symbol: &str, 
        callback: F)
    where
        F: Fn(NormalizedResponse) + Send + Sync + 'static,
    {
        let callback: TradeCallback = Arc::new(callback);
        if let Some(engine) = self.engines.get(&exchange) {
            let _ = engine.tx.send(EngineCommand::Subscribe(symbol.to_string())).await;
            let handle = self.manager.subscribe(symbol.to_string(), callback);

            self.subscriptions.push(handle);
            return;
        }

        let (tx, rx) = mpsc::channel(100);
        self.engines
            .insert(exchange.clone(), EngineHandle { tx: tx.clone() });

        match exchange {
            Exchange::Binance => {
                runtime.spawn(start_engine(BinanceAdapter, self.manager.tx.clone(), rx));
            }
        }

        let _ = tx.send(EngineCommand::Subscribe(symbol.to_string())).await;
    }

    pub async fn disconnect(&mut self, exchange: Exchange) {
        if let Some(engine) = self.engines.remove(&exchange) {
            let _ = engine.tx.send(EngineCommand::Shutdown).await;
        }
    }
}
