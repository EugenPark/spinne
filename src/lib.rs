use crate::{
    connection::{spawn_connection_listener, spawn_connector},
    registration::{spawn_registration_listener, spawn_registration_sender},
};
use bincode;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::ErrorKind, net::SocketAddr};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    spawn,
    sync::mpsc,
    task::JoinHandle,
    time::Duration,
};

mod connection;
mod registration;

const CONNECTION_RETRIES: i32 = 10;
const CONNECTION_PAUSE: Duration = Duration::from_secs(1);

pub type ID = u64;
pub type AddressMap = HashMap<ID, SocketAddr>;

pub struct Spinne {
    id: ID,
    node_ids: Vec<ID>,
    sender_task: Option<JoinHandle<()>>,
    listener_tasks: Option<Vec<JoinHandle<()>>>,
    incoming_connections: Option<HashMap<ID, OwnedReadHalf>>,
    outgoing_connections: Option<HashMap<ID, OwnedWriteHalf>>,
}

impl Spinne {
    pub fn id(&self) -> ID {
        self.id
    }

    pub fn peer_ids(&self) -> Vec<ID> {
        self.node_ids
            .clone()
            .into_iter()
            .filter(|&id| id != self.id())
            .collect()
    }

    pub fn node_ids(&self) -> Vec<ID> {
        self.node_ids.clone()
    }
}

impl Spinne {
    fn new(
        id: ID,
        node_ids: Vec<ID>,
        incoming_connections: HashMap<ID, OwnedReadHalf>,
        outgoing_connections: HashMap<ID, OwnedWriteHalf>,
    ) -> Self {
        Spinne {
            id,
            node_ids,
            sender_task: None,
            listener_tasks: None,
            incoming_connections: Some(incoming_connections),
            outgoing_connections: Some(outgoing_connections),
        }
    }
    pub fn start<T>(&mut self) -> (mpsc::Sender<(ID, T)>, mpsc::Receiver<(ID, T)>)
    where
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    {
        let (tx_incoming, rx_incoming) = mpsc::channel::<(ID, T)>(1_000);
        let (tx_outgoing, rx_outgoing) = mpsc::channel::<(ID, T)>(1_000);

        let incoming_connections = match self.incoming_connections.take() {
            Some(incoming_connections) => incoming_connections,
            None => panic!("Failed to get incoming connections"),
        };

        let outgoing_connections = match self.outgoing_connections.take() {
            Some(outgoing_connections) => outgoing_connections,
            None => panic!("Failed to get incoming connections"),
        };

        self.listener_tasks = Some(spawn_listen(incoming_connections, tx_incoming));
        self.sender_task = Some(spawn_sender(outgoing_connections, rx_outgoing));

        (tx_outgoing, rx_incoming)
    }

    pub async fn init(listen_address: SocketAddr, addresses: AddressMap) -> Self {
        let node_ids: Vec<ID> = addresses.iter().map(|(&k, _)| k).collect();
        let id = register(listen_address, addresses.clone()).await;
        let (incoming_connections, outgoing_connections) =
            connect(id, listen_address, addresses).await;
        Spinne::new(id, node_ids, incoming_connections, outgoing_connections)
    }

    pub async fn shutdown(&mut self) {
        self.sender_task
            .take()
            .expect("Called shutdown on unitialized sender task")
            .abort();
        for listener_task in self
            .listener_tasks
            .take()
            .expect("Calling shutdown on unitialized listener task")
            .iter()
        {
            listener_task.abort();
        }
    }
}

async fn register(listen_address: SocketAddr, addresses: AddressMap) -> ID {
    let listen_handle = spawn_registration_listener(listen_address, addresses.len()).await;

    let mut handles = vec![];
    for (id, address) in addresses {
        let handle = spawn_registration_sender(id, address).await;
        handles.push(handle);
    }
    join_all(handles).await;

    listen_handle
        .await
        .expect("Listener failed to exit gracefully")
}

async fn connect(
    own_id: ID,
    listen_address: SocketAddr,
    addresses: AddressMap,
) -> (HashMap<ID, OwnedReadHalf>, HashMap<ID, OwnedWriteHalf>) {
    let higher_addresses: HashMap<ID, SocketAddr> = addresses
        .clone()
        .into_iter()
        .filter(|&(id, _)| id > own_id)
        .collect();

    let expected_connections = addresses
        .into_iter()
        .map(|(id, _)| id)
        .filter(|&id| own_id != id && higher_addresses.get(&id).is_none())
        .collect();

    let connection_listener_handle =
        spawn_connection_listener(listen_address, expected_connections).await;

    let mut sender_incoming_connections = HashMap::new();
    let mut sender_outgoing_connections = HashMap::new();
    for (id, address) in higher_addresses {
        let (incoming, outgoing) = spawn_connector(own_id, address)
            .await
            .expect("Failed to connect to Node");
        sender_incoming_connections.insert(id, incoming);
        sender_outgoing_connections.insert(id, outgoing);
    }

    let (mut incoming_connections, mut outgoing_connections) = connection_listener_handle
        .await
        .expect("Failed to get listener connections");

    incoming_connections.extend(sender_incoming_connections);
    outgoing_connections.extend(sender_outgoing_connections);

    (incoming_connections, outgoing_connections)
}

fn spawn_listen<T: for<'de> Deserialize<'de> + Send + 'static>(
    incoming_connections: HashMap<ID, OwnedReadHalf>,
    tx: mpsc::Sender<(ID, T)>,
) -> Vec<JoinHandle<()>> {
    let mut handles = vec![];
    for (id, mut reader) in incoming_connections {
        let handle = spawn({
            let tx = tx.clone();
            async move {
                let mut length_buf = [0u8; 4];
                loop {
                    match reader.read_exact(&mut length_buf).await {
                        Ok(_) => (),
                        Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                        Err(e) if e.kind() == ErrorKind::ConnectionReset => break,
                        Err(e) => panic!("Encountered unexpected error: {e}"),
                    }
                    let length = u32::from_le_bytes(length_buf) as usize;

                    let mut data_buf = vec![0u8; length];
                    match reader.read_exact(&mut data_buf).await {
                        Ok(_) => (),
                        Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                        Err(e) if e.kind() == ErrorKind::ConnectionReset => break,
                        Err(e) => panic!("Encountered unexpected error: {e}"),
                    };

                    let data: T =
                        bincode::deserialize(&data_buf).expect("Failed to deserialize the data");

                    tx.send((id, data)).await.expect("Failed to send the data");
                }
            }
        });
        handles.push(handle);
    }
    handles
}

fn spawn_sender<T: Send + 'static + Serialize>(
    mut outgoing_connections: HashMap<ID, OwnedWriteHalf>,
    mut rx: mpsc::Receiver<(ID, T)>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some((id, data)) = rx.recv().await {
            let connection = outgoing_connections.get_mut(&id).expect("No such node");
            let serialized_data = bincode::serialize(&data).expect("Failed to serialize the data");

            let length_bytes = (serialized_data.len() as u32).to_le_bytes();

            match connection.write_all(&length_bytes).await {
                Ok(_) => (),
                Err(e) if e.kind() == ErrorKind::BrokenPipe => continue,
                Err(e) => panic!("{e}"),
            }
            match connection.write_all(&serialized_data).await {
                Ok(_) => (),
                Err(e) if e.kind() == ErrorKind::BrokenPipe => continue,
                Err(e) => panic!("{e}"),
            };
        }
    })
}
