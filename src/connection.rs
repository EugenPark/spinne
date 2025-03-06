use crate::ID;
use crate::{CONNECTION_PAUSE, CONNECTION_RETRIES};
use std::{collections::HashMap, net::SocketAddr};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    spawn,
    task::JoinHandle,
    time::sleep,
};

pub(crate) async fn spawn_connection_listener(
    listen_address: SocketAddr,
    mut expected_connections: Vec<ID>,
) -> JoinHandle<(HashMap<ID, OwnedReadHalf>, HashMap<ID, OwnedWriteHalf>)> {
    spawn(async move {
        let mut read_buf = [0u8; 8];
        let listener = TcpListener::bind(listen_address)
            .await
            .expect("Failed to listen to listen address");

        let mut incoming_connections = HashMap::new();
        let mut outgoing_connections = HashMap::new();
        while !expected_connections.is_empty() {
            let (from, (incoming, outgoing)) = get_connection(&listener, &mut read_buf).await;
            incoming_connections.insert(from, incoming);
            outgoing_connections.insert(from, outgoing);
            expected_connections = expected_connections
                .into_iter()
                .filter(|&id| id != from)
                .collect();
        }

        (incoming_connections, outgoing_connections)
    })
}

pub(crate) async fn spawn_connector(
    own_id: ID,
    address: SocketAddr,
) -> Result<(OwnedReadHalf, OwnedWriteHalf), String> {
    for _ in 1..=CONNECTION_RETRIES {
        match TcpStream::connect(address).await {
            Err(_err) => sleep(CONNECTION_PAUSE).await,
            Ok(mut stream) => {
                let bytes = own_id.to_be_bytes();
                stream.set_nodelay(true).expect("Failed to set nodelay");
                stream
                    .write_all(&bytes)
                    .await
                    .expect("Failed to write id to node");

                return Ok(stream.into_split());
            }
        }
    }

    Err("Failed to connect".to_string())
}

async fn get_connection(
    listener: &TcpListener,
    read_buf: &mut [u8; 8],
) -> (ID, (OwnedReadHalf, OwnedWriteHalf)) {
    let (mut stream, _) = listener
        .accept()
        .await
        .expect("Failed to accept connection");
    stream
        .read_exact(read_buf)
        .await
        .expect("Failed to read ID to buffer");
    let from = u64::from_be_bytes(*read_buf);

    stream.set_nodelay(true).expect("Failed to set nodelay");

    (from, stream.into_split())
}
