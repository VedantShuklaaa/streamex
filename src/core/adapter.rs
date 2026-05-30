use anyhow::Result;

use crate::models::normalized::NormalizedResponse;

pub trait ExchangeAdapter {
	fn websocket_url(&self) -> &'static str;
	fn normalize_symbol(&self, symbol: &str) -> String;
	fn subscribe_message(&self, symbol: &str) -> Result<String>;
	fn unsubscribe_message(&self, symbol: &str) -> Result<String>;
	fn parse_message(&self, text: &str) -> Vec<NormalizedResponse>;
}