use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use crate::models::normalized::NormalizedResponse;

#[derive(Debug)]
pub enum EngineCommand {
    Subscribe(String),
    Unsubscribe(String),
    Shutdown,
}

pub struct EngineHandle {
    pub tx: Sender<EngineCommand>,
}

pub type TradeCallback = Arc<dyn Fn(NormalizedResponse) + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Exchange {
	Binance,
    Coinbase,
    Okx,
    Bybit,
    Kraken,
    Bitget,
    Bitfinex,
    CryptoCom,
    Htx,
    Bitstamp,
}