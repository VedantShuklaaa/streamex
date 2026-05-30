use crate::{
    core::{
        engine::start_engine,
        subscription_manager::SubscriptionManager,
        types::{EngineCommand, EngineHandle, Exchange, TradeCallback},
    },
    exchanges::{
        binance::adapter::BinanceAdapter, bitfinex::adapter::BitfinexAdapter,
        bitget::adapter::BitgetAdapter, bitstamp::adapter::BitstampAdapter,
        bybit::adapter::BybitAdapter, coinbase::adapter::CoinbaseAdapter,
        crypto_com::adapter::CryptoComAdapter, htx::adapter::HtxAdapter,
        kraken::adapter::KrakenAdapter, okx::adapter::OkxAdapter,
    },
    models::normalized::NormalizedResponse,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{broadcast, mpsc};

pub struct Streamex {
    engines: HashMap<Exchange, EngineHandle>,
    pub manager: SubscriptionManager,
    subscriptions: HashMap<(Exchange, String), tokio::task::JoinHandle<()>>,
}

impl Streamex {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel::<NormalizedResponse>(100);
        let tx = Arc::new(tx);
        let manager = SubscriptionManager::new(tx);

        Self {
            engines: HashMap::new(),
            manager,
            subscriptions: HashMap::new(),
        }
    }

    fn spawn_engine<A>(&self, adapter: A, rx: mpsc::Receiver<EngineCommand>)
    where
        A: crate::core::adapter::ExchangeAdapter + Send + Sync + 'static,
    {
        let engine_tx = self.manager.tx.clone();
        tokio::spawn(start_engine(adapter, engine_tx, rx));
    }

    pub async fn trades<F>(&mut self, exchange: Exchange, symbol: &str, callback: F)
    where
        F: Fn(NormalizedResponse) + Send + Sync + 'static,
    {
        let key = (exchange.clone(), symbol.to_string());
        let callback: TradeCallback = Arc::new(callback);

        if let Some(engine) = self.engines.get(&exchange) {
            let _ = engine
                .tx
                .send(EngineCommand::Subscribe(symbol.to_string()))
                .await;

            if self.subscriptions.contains_key(&key) {
                return;
            }

            let handle = self
                .manager
                .subscribe(symbol.to_string(), exchange.clone(), callback);

            self.subscriptions.insert(key, handle);

            return;
        }

        let (tx, rx) = mpsc::channel(100);
        self.engines
            .insert(exchange.clone(), EngineHandle { tx: tx.clone() });

        match exchange {
            Exchange::Binance => {
                self.spawn_engine(BinanceAdapter, rx);
            }

            Exchange::Coinbase => {
                self.spawn_engine(CoinbaseAdapter, rx);
            }

            Exchange::Okx => {
                self.spawn_engine(OkxAdapter, rx);
            }

            Exchange::Bybit => {
                self.spawn_engine(BybitAdapter, rx);
            }

            Exchange::Kraken => {
                self.spawn_engine(KrakenAdapter, rx);
            }

            Exchange::Bitget => {
                self.spawn_engine(BitgetAdapter, rx);
            }

            Exchange::Bitfinex => {
                self.spawn_engine(BitfinexAdapter::new(), rx);
            }

            Exchange::CryptoCom => {
                self.spawn_engine(CryptoComAdapter, rx);
            }

            Exchange::Htx => {
                self.spawn_engine(HtxAdapter, rx);
            }

            Exchange::Bitstamp => {
                self.spawn_engine(BitstampAdapter, rx);
            }
        }

        let _ = tx.send(EngineCommand::Subscribe(symbol.to_string())).await;
        let handle = self
            .manager
            .subscribe(symbol.to_string(), exchange.clone(), callback);

        self.subscriptions.insert(key, handle);
    }

    pub async fn disconnect(&mut self, exchange: Exchange) {
        if let Some(engine) = self.engines.remove(&exchange) {
            let _ = engine.tx.send(EngineCommand::Shutdown).await;
        }

        self.subscriptions.retain(|(ex, _), handle| {
            if *ex == exchange {
                handle.abort();
                false
            } else {
                true
            }
        });
    }
}
