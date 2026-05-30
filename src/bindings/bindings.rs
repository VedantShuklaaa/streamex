use napi::{bindgen_prelude::Function, threadsafe_function::ThreadsafeFunctionCallMode};
use napi_derive::napi;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{
    core::{stream::Streamex, types::Exchange},
    models::normalized::NormalizedResponse,
};

#[napi]
pub struct JsStreamex {
    inner: Arc<Mutex<Streamex>>,
}

fn parse_exchange(exchange: &str) -> napi::Result<Exchange> {
    match exchange {
        "binance" => Ok(Exchange::Binance),
        "coinbase" => Ok(Exchange::Coinbase),
        "okx" => Ok(Exchange::Okx),
        "bybit" => Ok(Exchange::Bybit),
        "kraken" => Ok(Exchange::Kraken),
        "bitget" => Ok(Exchange::Bitget),
        "bitfinex" => Ok(Exchange::Bitfinex),
        "crypto_com" => Ok(Exchange::CryptoCom),
        "htx" => Ok(Exchange::Htx),
        "bitstamp" => Ok(Exchange::Bitstamp),

        _ => Err(napi::Error::from_reason("unsupported exchange")),
    }
}

#[napi]
impl JsStreamex {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Streamex::new())),
        }
    }

    #[napi]
    pub fn trades(
        &self,
        exchange: String,
        symbol: String,
        callback: Function<'_, String, ()>,
    ) -> napi::Result<()> {
        let exchange = parse_exchange(exchange.as_str())?;

        let tsfn = callback.build_threadsafe_function().build()?;
        let inner = self.inner.clone();

        napi::bindgen_prelude::spawn(async move {
            let mut locked = inner.lock().await;
            locked
                .trades(exchange, &symbol, move |trade: NormalizedResponse| {
                    let payload = serde_json::to_string(&trade).unwrap();
                    tsfn.call(payload, ThreadsafeFunctionCallMode::NonBlocking);
                })
                .await;
        });

        Ok(())
    }

    #[napi]
    pub fn disconnect(&self, exchange: String) -> napi::Result<()> {
        let exchange = parse_exchange(exchange.as_str())?;

        let inner = self.inner.clone();
        napi::bindgen_prelude::spawn(async move {
            inner.lock().await.disconnect(exchange).await;
        });
        Ok(())
    }
}
