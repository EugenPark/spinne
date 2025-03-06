use crate::{CONNECTION_PAUSE, CONNECTION_RETRIES, ID};
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    spawn,
    task::JoinHandle,
    time::sleep,
};

pub(crate) async fn spawn_registration_listener(
    listen_address: SocketAddr,
    expected_connections: usize,
) -> JoinHandle<ID> {
    spawn(async move {
        let mut read_buf = [0u8; 8];
        let listener = TcpListener::bind(listen_address)
            .await
            .expect("Failed to listen to listen address");

        let mut id = None;
        for _ in 1..=expected_connections {
            id = Some(read_id(&listener, &mut read_buf).await);
        }
        id.expect("Failed to get an ID")
    })
}

async fn read_id(listener: &TcpListener, read_buf: &mut [u8; 8]) -> ID {
    let (mut stream, _) = listener
        .accept()
        .await
        .expect("Failed to accept connection");
    stream
        .read_exact(read_buf)
        .await
        .expect("Failed to read ID to buffer");
    u64::from_be_bytes(*read_buf)
}

pub(crate) async fn spawn_registration_sender(id: ID, address: SocketAddr) -> JoinHandle<()> {
    spawn(async move {
        for _ in 1..=CONNECTION_RETRIES {
            match TcpStream::connect(address).await {
                Ok(mut stream) => {
                    let bytes = id.to_be_bytes();
                    stream
                        .write_all(&bytes)
                        .await
                        .expect("Failed to write id to node");
                }
                Err(_err) => sleep(CONNECTION_PAUSE).await,
            };
        }
    })
}
