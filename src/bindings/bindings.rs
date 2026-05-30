use napi::{bindgen_prelude::Function, threadsafe_function::ThreadsafeFunctionCallMode};
use napi_derive::napi;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

use crate::{
    core::{stream::Streamex, types::Exchange},
    models::normalized::NormalizedResponse,
};

#[napi]
pub struct JsStreamex {
    inner: Arc<Mutex<Streamex>>,
    runtime: Arc<Runtime>,
    tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

#[napi]
impl JsStreamex {
    #[napi(constructor)]
    pub fn new() -> Self {
        let runtime = Runtime::new().unwrap();
        Self {
            inner: Arc::new(Mutex::new(Streamex::new())),
            runtime: Arc::new(runtime),
            tasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[napi]
    pub fn trades(
        &self,
        exchange: String,
        symbol: String,
        callback: Function<'_, String, ()>,
    ) -> napi::Result<()> {
        let exchange = match exchange.as_str() {
            "binance" => Exchange::Binance,
            _ => {
                return Err(napi::Error::from_reason("unsupported exchange"));
            }
        };

        let tsfn = callback.build_threadsafe_function().build()?;

        let inner = self.inner.clone();

        let runtime = self.runtime.clone();
        let runtime_clone = runtime.clone();
        let handle = runtime.spawn(async move {
            let mut locked = inner.lock().await;

            locked
                .trades(
                    runtime_clone,
                    exchange,
                    &symbol,
                    move |trade: NormalizedResponse| {
                        let payload = serde_json::to_string(&trade).unwrap();

                        tsfn.call(payload, ThreadsafeFunctionCallMode::NonBlocking);
                    },
                )
                .await;
        });

        self.tasks.blocking_lock().push(handle);
        Ok(())
    }

    #[napi]
    pub fn disconnect(&self, exchange: String) -> napi::Result<()> {
        let exchange = match exchange.as_str() {
            "binance" => Exchange::Binance,
            _ => {
                return Err(napi::Error::from_reason("unsupported exchange"));
            }
        };

        let inner = self.inner.clone();

        self.runtime.spawn(async move {
            inner.lock().await.disconnect(exchange).await;
        });
        Ok(())
    }
}
