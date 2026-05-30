use futures_util::{SinkExt, StreamExt};
use std::{collections::HashSet, sync::Arc, io::Read};
use tokio::sync::{broadcast, mpsc::Receiver};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use flate2::read::GzDecoder;

use crate::{
    core::{adapter::ExchangeAdapter, types::EngineCommand},
    models::normalized::NormalizedResponse,
};

pub async fn start_engine<A>(
    adapter: A,
    tx: Arc<broadcast::Sender<NormalizedResponse>>,
    mut rx: Receiver<EngineCommand>,
) where
    A: ExchangeAdapter + Send + Sync + 'static,
{
    let mut active_symbols = HashSet::<String>::new();

    loop {
        let url = adapter.websocket_url();

        println!("connecting to {}", url);

        let connection = connect_async(url).await;

        let (ws_stream, _) = match connection {
            Ok(success) => {
                println!("connected successfully");
                success
            }

            Err(err) => {
                eprintln!("connection failed: {}", err);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        let (mut write, mut read) = ws_stream.split();

        for symbol in &active_symbols {
            let normalized = adapter.normalize_symbol(&symbol);
            let payload = adapter.subscribe_message(&normalized).unwrap();

            let _ = write.send(Message::Text(payload.into())).await;
        }

        println!("subscriptions restored");

        loop {
            tokio::select! {
                Some(message) = read.next() =>{
                    match message {
                        Ok(Message::Text(text)) => {
                            let parsed = adapter.parse_message(&text);

                            if parsed.is_empty() {
                                continue;
                            }
                            for trade in parsed {
                                let _ = tx.send(trade);
                            }
                        }

                        Ok(Message::Binary(bin)) => {
                            let mut decoder = GzDecoder::new(&bin[..]);
                            let mut text = String::new();

                            if let Err(err) = decoder.read_to_string(&mut text) {
                                eprintln!("gzip decode failed: {}", err);
                                continue;
                            }

                            if text.contains("\"ping\"") {
                                let pong = text.replace("ping", "pong");
                                let _ = write.send(Message::Text(pong.into())).await;

                                continue;
                            }

                            let parsed = adapter.parse_message(&text);

                            if parsed.is_empty() {
                                continue;
                            }

                            for trade in parsed {
                                if let Err(err) = tx.send(trade.clone()) {
                                    eprintln!("broadcast failed: {}", err);
                                }
                            }
                        }

                        Ok(Message::Close(_)) => {
                            println!("websocket closed");
                            break;
                        }

                        Err(err) => {
                            eprintln!("websocket error: {}", err);
                            break;
                        }

                        _ => {}
                    }
                }

                Some(command) = rx.recv() => {
                    match command {
                        EngineCommand::Subscribe(symbol) => {
                            active_symbols.insert(symbol.clone());

                            let normalized = adapter.normalize_symbol(&symbol);
                            let payload = adapter.subscribe_message(&normalized).unwrap();

                            let _ = write
                                .send(
                                    Message::Text(
                                        payload.into()
                                    )
                                )
                                .await;

                            println!("subscribed: {}",symbol);
                        }

                        EngineCommand::Unsubscribe(symbol) =>{
                            active_symbols.remove(&symbol);

                            let normalized = adapter.normalize_symbol(&symbol);
                            let payload = adapter.unsubscribe_message(&normalized).unwrap();

                            let _ = write
                                        .send(Message::Text(
                                            payload.into()
                                        )).await;
                            println!("unsubscribed: {}",symbol);
                        }

                        EngineCommand::Shutdown =>{
                            println!("engine shutdown");
                            return;
                        }
                    }
                }
            }
        }

        println!("reconnecting in 5 seconds");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}
