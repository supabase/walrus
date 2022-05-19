use clap::Parser;
use env_logger;
use futures::stream::SplitSink;
use futures_util::SinkExt;
use futures_util::{future, pin_mut, StreamExt};
use log::{error, info, warn};
use serde::Serialize;
use serde_json;
use std::str;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, WebSocketStream};
use url;

/// reads JSON from stdin and forwards it to supabase realtime
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, default_value = "wss://sendwal.fly.dev/socket")]
    url: String,

    #[clap(
        long,
        value_name = "HEADER>=<VALUE",
        parse(try_from_str = parse_header),
        number_of_values = 1,
    )]
    header: Vec<(String, String)>,

    #[clap(long, default_value = "room:test")]
    topic: String,
}

fn parse_header(user_input: &str) -> Result<(String, String), String> {
    let mut splitter = user_input.splitn(2, "=");

    let key = splitter.next();
    let val = splitter.next();

    match (key, val) {
        (Some(k), Some(v)) => Ok((k.to_string(), v.to_string())),
        _ => Err(format!(
            "Could not parse header key-value pair: {}",
            user_input
        )),
    }
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
    let addr = build_url(&args.url, &args.header);
    let url = url::Url::parse(&addr).expect("invalid URL");

    println!("{:?}", args);

    // enable logger
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    loop {
        // websocket
        info!("Connecting to websocket");
        let ws_connection = connect_async(&url).await;

        match ws_connection {
            Err(msg) => {
                let n_seconds = 3;
                error!("Failed to connect to websocket. Error: {}", msg);
                info!("Attempting websocket reconnect in {} seconds", n_seconds);
                sleep(Duration::from_secs(n_seconds)).await;
            }
            Ok((ws_stream, _)) => {
                info!("WebSocket handshake successful");
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
        }
    }
}

pub fn build_url(url: &str, params: &Vec<(String, String)>) -> String {
    let mut params_uri: String = "".to_owned();
    for (k, v) in params {
        params_uri.push_str(&format!("&{}={}", k, v));
    }
    let addr = format!("{}/websocket?vsn={}{}", url, "1.0.0", params_uri);
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
    let stdin = tokio::io::stdin();
    let buf = BufReader::new(stdin);
    let mut lines = buf.lines();

    loop {
        let line_res_opt: Result<Option<String>, std::io::Error> = lines.next_line().await;

        match line_res_opt {
            Ok(line_opt) => {
                match line_opt {
                    Some(line) => {
                        // Parse stdin string as json
                        match serde_json::from_str(&line) {
                            Ok(msg_json) => {
                                // Repack json contents into a phoenix message
                                let phoenix_msg = PhoenixMessage {
                                    event: PhoenixMessageEvent::Message,
                                    payload: msg_json,
                                    reference: None,
                                    topic: topic.to_string(),
                                };
                                // Wrap phoenix message in a websocket message
                                let msg =
                                    Message::Text(serde_json::to_string(&phoenix_msg).unwrap());
                                // push to output stream
                                tx.unbounded_send(msg).unwrap();
                            }
                            Err(err) => {
                                warn!(
                                    "Error parsing stdin line to json: error={}, line={}",
                                    err, line
                                )
                            }
                        }
                    }
                    None => {
                        warn!("Received empty line from stdin");
                    }
                }
            }
            Err(err) => {
                error!("Error reading line from stdin: {}", err);
            }
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
