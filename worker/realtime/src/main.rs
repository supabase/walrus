use clap::Parser;
use futures::stream::SplitSink;
use futures_util::SinkExt;
use futures_util::{future, pin_mut, StreamExt};
use serde::Serialize;
use serde_json;
use std::str;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, WebSocketStream};
use url;

/// reads JSON from stdin and forwards it to supabase realtime
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, default_value = "wss://sendwal.fly.dev/socket")]
    url: String,

    #[clap(long)]
    apikey: String,

    #[clap(long, default_value = "room:test")]
    topic: String,
}

#[derive(Serialize)]
enum PhoenixMessageEvent {
    #[serde(rename(serialize = "phx_join"))]
    Join,
    #[serde(rename(serialize = "new_msg"))]
    Message,
    #[serde(rename(serialize = "heartbeat"))]
    Heartbeat,
}

#[derive(Serialize)]
struct PhoenixMessage {
    event: PhoenixMessageEvent,
    payload: serde_json::Value,
    #[serde(rename(serialize = "ref"))]
    reference: Option<String>,
    topic: String,
}

#[tokio::main]
async fn main() {
    // url
    let args = Args::parse();
    let addr = build_url(&args.url, &args.apikey);
    let url = url::Url::parse(&addr).unwrap();

    // websocket
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    println!("WebSocket handshake successful");
    let (mut write, read) = ws_stream.split();

    write = join_topic(write, args.topic.to_string()).await;

    // Futures channel
    let (tx, rx) = futures_channel::mpsc::unbounded();
    let heartbeat_tx = tx.clone();

    tokio::spawn(read_stdin(tx, args.topic.to_string()));
    tokio::spawn(heartbeat(heartbeat_tx, args.topic.to_string()));

    // Map
    let tx_to_ws = rx.map(Ok).forward(write);
    let ws_to_stdout = {
        read.for_each(|message| async {
            let data = message.unwrap().into_data();
            tokio::io::stdout().write_all(&data).await.unwrap();
        })
    };

    pin_mut!(tx_to_ws, ws_to_stdout);
    future::select(tx_to_ws, ws_to_stdout).await;
}

pub fn build_url(url: &str, apikey: &str) -> String {
    let addr = format!("{}/websocket?vsn={}&apikey={}", url, "1.0.0", apikey);
    addr
}

async fn join_topic(
    mut writer: SplitSink<
        WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        Message,
    >,
    topic_name: String,
) -> SplitSink<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, Message> {
    // join channel
    let join_message = PhoenixMessage {
        event: PhoenixMessageEvent::Join,
        payload: serde_json::json!({}),
        reference: None,
        topic: topic_name,
    };

    let join_message = serde_json::to_string(&join_message).unwrap();
    let msg = Message::Text(join_message);
    writer.send(msg).await.unwrap();
    writer
}

// Our helper method which will read data from stdin and send it along the
// sender provided.
async fn read_stdin(tx: futures_channel::mpsc::UnboundedSender<Message>, topic: String) {
    let mut stdin = tokio::io::stdin();
    loop {
        let mut buf = vec![0; 61024];

        // Read stdin
        let n = match stdin.read(&mut buf).await {
            Err(_) | Ok(0) => break,
            Ok(n) => n,
        };
        buf.truncate(n);

        let mut lines = buf.lines();

        while let Some(line) = lines.next_line().await.expect("invalid line") {
            // Read stdin as string

            // Parse stdin string as json
            let msg_json =
                serde_json::from_str(&line).expect(&format!("failed to parse message '{}'", line));

            // Repack json contents into a phoenix message
            let phoenix_msg = PhoenixMessage {
                event: PhoenixMessageEvent::Message,
                payload: msg_json,
                reference: None,
                topic: topic.to_string(),
            };

            // Wrap phoenix message in a websocket message
            let msg = Message::Text(serde_json::to_string(&phoenix_msg).unwrap());

            // push to futures stream
            tx.unbounded_send(msg).unwrap();
        }
    }
}

async fn heartbeat(tx: futures_channel::mpsc::UnboundedSender<Message>, topic: String) {
    loop {
        sleep(Duration::from_secs(3)).await;
        let phoenix_msg = PhoenixMessage {
            event: PhoenixMessageEvent::Heartbeat,
            payload: serde_json::json!({"msg": "ping"}),
            reference: None,
            topic: topic.to_string(),
        };
        // Wrap phoenix message in a websocket message
        let msg = Message::Text(serde_json::to_string(&phoenix_msg).unwrap());
        // push to futures stream
        tx.unbounded_send(msg).unwrap();
    }
}
