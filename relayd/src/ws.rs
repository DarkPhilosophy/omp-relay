use axum::extract::ws::{CloseCode, CloseFrame, Message as AxumMessage, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message as TungsteniteMessage};

pub async fn bridge<S>(client: WebSocket, upstream: WebSocketStream<S>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut client_sink, mut client_stream) = client.split();
    let (mut upstream_sink, mut upstream_stream) = upstream.split();

    loop {
        tokio::select! {
            client_message = client_stream.next() => {
                let Some(Ok(message)) = client_message else { break };
                let closes = matches!(message, AxumMessage::Close(_));
                let Ok(message) = axum_to_tungstenite(message) else { break };
                if upstream_sink.send(message).await.is_err() { break; }
                if closes { break; }
            }
            upstream_message = upstream_stream.next() => {
                let Some(Ok(message)) = upstream_message else { break };
                let closes = matches!(message, TungsteniteMessage::Close(_));
                let Some(message) = tungstenite_to_axum(message) else { continue };
                if client_sink.send(message).await.is_err() { break; }
                if closes { break; }
            }
        }
    }

    let _ = upstream_sink.close().await;
    let _ = client_sink.close().await;
}

fn axum_to_tungstenite(message: AxumMessage) -> Result<TungsteniteMessage, ()> {
    Ok(match message {
        AxumMessage::Text(text) => TungsteniteMessage::Text(text.to_string().into()),
        AxumMessage::Binary(bytes) => TungsteniteMessage::Binary(bytes),
        AxumMessage::Ping(bytes) => TungsteniteMessage::Ping(bytes),
        AxumMessage::Pong(bytes) => TungsteniteMessage::Pong(bytes),
        AxumMessage::Close(frame) => TungsteniteMessage::Close(frame.map(|frame| {
            tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: frame.code.into(),
                reason: frame.reason.to_string().into(),
            }
        })),
    })
}

fn tungstenite_to_axum(message: TungsteniteMessage) -> Option<AxumMessage> {
    match message {
        TungsteniteMessage::Text(text) => Some(AxumMessage::Text(text.to_string().into())),
        TungsteniteMessage::Binary(bytes) => Some(AxumMessage::Binary(bytes)),
        TungsteniteMessage::Ping(bytes) => Some(AxumMessage::Ping(bytes)),
        TungsteniteMessage::Pong(bytes) => Some(AxumMessage::Pong(bytes)),
        TungsteniteMessage::Close(frame) => {
            Some(AxumMessage::Close(frame.map(|frame| CloseFrame {
                code: CloseCode::from(frame.code),
                reason: frame.reason.to_string().into(),
            })))
        }
        TungsteniteMessage::Frame(_) => None,
    }
}
