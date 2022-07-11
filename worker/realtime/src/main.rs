use clap::Parser;
use env_logger;
use futures::{Sink, SinkExt, Stream};
use futures_util::{future, pin_mut, StreamExt};
use log::{error, info, warn};
use serde::Serialize;
use serde_json;
use std::str;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, WebSocketStream};

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
    #[serde(rename(serialize = "changes"))]
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

    let config = PhoenixWsConfig {
        addr: build_url(&args.url, &args.header),
        topic_name: args.topic.to_string(),
    };

    let topic = config.topic_name.to_string();

    info!("{:?}", args);

    // enable logger
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();

    let ws_stream: ReconnectWs = ReconnectWs::connect(config).await.unwrap();
    info!("WebSocket handshake successful");

    let (write, read) = ws_stream.split();

    // Futures channel
    let (tx, rx) = futures_channel::mpsc::unbounded();
    let heartbeat_tx = tx.clone();
    tokio::spawn(read_stdin(tx, topic.to_string()));

    tokio::spawn(heartbeat(heartbeat_tx));

    // Map
    let tx_to_ws = rx.map(Ok).forward(write);

    let ws_to_stdout = {
        read.for_each(|message| async {
            match message {
                Ok(msg) => match msg.into_text() {
                    Ok(msg_text) => info!("{}", msg_text),
                    Err(err) => {
                        error!(
                            "Failed to parse message from realtime service: Error: {}",
                            err
                        );
                        // Attempt to rejoin
                        //write = join_topic(write, topic.to_string()).await;
                    }
                },
                Err(err) => {
                    error!(
                        "Failed to read message from realtime service: Error: {}",
                        err
                    );
                    // Attempt to rejoin
                    //write = join_topic(write, topic.to_string()).await;
                }
            };
        })
    };

    pin_mut!(tx_to_ws, ws_to_stdout);
    future::select(tx_to_ws, ws_to_stdout).await;
}

pub fn build_url(url: &str, params: &Vec<(String, String)>) -> String {
    let mut params_uri: String = "".to_owned();
    for (k, v) in params {
        params_uri.push_str(&format!("&{}={}", k, v));
    }
    let addr = format!("{}/websocket?vsn={}{}", url, "1.0.0", params_uri);
    addr
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
                                match tx.unbounded_send(msg) {
                                    Ok(()) => (),
                                    Err(err) => {
                                        error!("Error sending message: {}", err);
                                    }
                                };
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

async fn heartbeat(tx: futures_channel::mpsc::UnboundedSender<Message>) {
    loop {
        sleep(Duration::from_secs(20)).await;
        let phoenix_msg = PhoenixMessage {
            event: PhoenixMessageEvent::Heartbeat,
            payload: serde_json::json!({"msg": "ping"}),
            reference: None,
            topic: "phoenix".to_string(),
        };
        // Wrap phoenix message in a websocket message
        let msg = Message::Text(serde_json::to_string(&phoenix_msg).unwrap());
        // push to futures stream
        match tx.unbounded_send(msg) {
            Ok(()) => (),
            Err(err) => {
                error!("Error sending heatbeat: {}", err);
            }
        };
    }
}

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use stream_reconnect::{ReconnectStream, UnderlyingStream};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::error::Error as WsError;
use tokio_tungstenite::MaybeTlsStream;

// A websocket to communicate with a Phoenix server
// It reconnects and re-subscribes to a topic if disconnected
struct PhoenixWs(WebSocketStream<MaybeTlsStream<TcpStream>>, PhoenixWsConfig);

#[derive(Clone)]
struct PhoenixWsConfig {
    addr: String,
    topic_name: String,
}

impl Stream for PhoenixWs {
    type Item = Result<Message, WsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

impl Sink<Message> for PhoenixWs {
    type Error = WsError;

    fn poll_ready(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0).poll_ready(_cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        Pin::new(&mut self.0).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0).poll_close(cx)
    }
}

// implement Stream & Sink for MyWs

impl UnderlyingStream<PhoenixWsConfig, Result<Message, WsError>, WsError> for PhoenixWs {
    // Establishes connection.
    // Additionally, this will be used when reconnect tries are attempted.
    fn establish(
        config: PhoenixWsConfig,
    ) -> Pin<Box<dyn Future<Output = Result<Self, WsError>> + Send>> {
        Box::pin(async move {
            // In this case, we are trying to connect to the WebSocket endpoint
            info!("Connecting to Realtime");

            let mut ws_connection = connect_async(config.addr.clone()).await?.0;

            // (re)Join Topic
            let join_message = PhoenixMessage {
                event: PhoenixMessageEvent::Join,
                payload: serde_json::json!({}),
                reference: None,
                topic: config.topic_name.to_string(),
            };
            let join_message = serde_json::to_string(&join_message).unwrap();
            let msg = Message::Text(join_message);
            ws_connection.send(msg).await?;
            info!("Joining topic");
            Ok(PhoenixWs(ws_connection, config.clone()))
        })
    }

    // The following errors are considered disconnect errors.
    fn is_write_disconnect_error(&self, err: &WsError) -> bool {
        matches!(
            err,
            WsError::ConnectionClosed
                | WsError::AlreadyClosed
                | WsError::Io(_)
                | WsError::Tls(_)
                | WsError::Protocol(_)
        )
    }

    // If an `Err` is read, then there might be an disconnection.
    fn is_read_disconnect_error(&self, item: &Result<Message, WsError>) -> bool {
        if let Err(e) = item {
            self.is_write_disconnect_error(e)
        } else {
            false
        }
    }

    // Return "Exhausted" if all retry attempts are failed.
    fn exhaust_err() -> WsError {
        WsError::Io(io::Error::new(io::ErrorKind::Other, "Exhausted"))
    }
}

type ReconnectWs = ReconnectStream<PhoenixWs, PhoenixWsConfig, Result<Message, WsError>, WsError>;
