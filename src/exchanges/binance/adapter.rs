use anyhow::Result;

use crate::{
    core::adapter::ExchangeAdapter,
    exchanges::binance::{
        model::{BinanceRawResponse, SubMessageBinance},
        normalize::normalize_binance_response,
    },
    models::normalized::NormalizedResponse,
};

pub struct BinanceAdapter;

impl ExchangeAdapter for BinanceAdapter {
	fn websocket_url(&self) -> &'static str {
		"wss://data-stream.binance.vision:443/ws"
	}

	fn normalize_symbol(&self, symbol: &str) -> String {
		symbol.to_lowercase()
	}

	fn subscribe_message(&self, symbol: &str) -> Result<String> {
		let payload = SubMessageBinance {
			method: "SUBSCRIBE".to_string(),
			params: vec![format!("{}@trade", symbol)],
			id: 1
		};

		Ok(serde_json::to_string(&payload)?)
	}

	fn unsubscribe_message(&self, symbol: &str) -> Result<String> {
		let payload = SubMessageBinance {
			method: "UNSUBSCRIBE".to_string(),
			params: vec![format!("{}@trade", symbol)],
			id: 1
		};

		Ok(serde_json::to_string(&payload)?)
	}

	fn parse_message(&self, text: &str) -> Vec<NormalizedResponse> {
		if !text.contains("\"e\"") {
            return vec![];
        }

		let parsed = serde_json::from_str::<BinanceRawResponse>(text);
		match parsed {
			Ok(payload) => {
				let parsed = normalize_binance_response(payload);
				vec![parsed]
			}
			Err(e) => {
				eprintln!("error: {}", e);
				vec![]
			}
		}
	}
}
