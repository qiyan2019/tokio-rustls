#![cfg(feature = "early-data")]

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rustls::pki_types::ServerName;
use rustls::{self, ClientConfig, ServerConnection, Stream};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

async fn send<S: AsyncRead + AsyncWrite + Unpin>(
    config: Arc<ClientConfig>,
    addr: SocketAddr,
    wrapper: impl Fn(TcpStream) -> S,
    data: &[u8],
    vectored: bool,
) -> io::Result<(TlsStream<S>, Vec<u8>)> {
    let connector = TlsConnector::from(config).early_data(true);
    let stream = wrapper(TcpStream::connect(&addr).await?);
    let domain = ServerName::try_from("foobar.com").unwrap();

    let mut stream = connector.connect(domain, stream).await?;
    utils::write(&mut stream, data, vectored).await?;
    stream.flush().await?;
    stream.shutdown().await?;

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;

    Ok((stream, buf))
}

#[tokio::test]
async fn test_0rtt() -> io::Result<()> {
    test_0rtt_impl(|s| s, false).await
}

#[tokio::test]
async fn test_0rtt_vectored() -> io::Result<()> {
    test_0rtt_impl(|s| s, true).await
}

#[tokio::test]
async fn test_0rtt_vectored_flush_pending() -> io::Result<()> {
    test_0rtt_impl(utils::FlushWrapper::new, false).await
}

async fn test_0rtt_impl<S: AsyncRead + AsyncWrite + Unpin>(
    wrapper: impl Fn(TcpStream) -> S,
    vectored: bool,
) -> io::Result<()> {
    let (mut server, mut client) = utils::make_configs();
    server.max_early_data_size = 8192;
    let server = Arc::new(server);

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let server_port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        loop {
            let (mut sock, _addr) = listener.accept().unwrap();

            let server = Arc::clone(&server);
            thread::spawn(move || {
                let mut conn = ServerConnection::new(server).unwrap();
                conn.complete_io(&mut sock).unwrap();

                if let Some(mut early_data) = conn.early_data() {
                    let mut buf = Vec::new();
                    early_data.read_to_end(&mut buf).unwrap();
                    let mut stream = Stream::new(&mut conn, &mut sock);
                    stream.write_all(b"EARLY:").unwrap();
                    stream.write_all(&buf).unwrap();
                }

                let mut stream = Stream::new(&mut conn, &mut sock);
                stream.write_all(b"LATE:").unwrap();
                loop {
                    let mut buf = [0; 1024];
                    let n = stream.read(&mut buf).unwrap();
                    if n == 0 {
                        conn.send_close_notify();
                        conn.complete_io(&mut sock).unwrap();
                        break;
                    }
                    stream.write_all(&buf[..n]).unwrap();
                }
            });
        }
    });

    client.enable_early_data = true;
    let client = Arc::new(client);
    let addr = SocketAddr::from(([127, 0, 0, 1], server_port));

    let (io, buf) = send(client.clone(), addr, &wrapper, b"hello", vectored).await?;
    assert!(!io.get_ref().1.is_early_data_accepted());
    assert_eq!("LATE:hello", String::from_utf8_lossy(&buf));

    let (io, buf) = send(client, addr, wrapper, b"world!", vectored).await?;
    assert!(io.get_ref().1.is_early_data_accepted());
    assert_eq!("EARLY:world!LATE:", String::from_utf8_lossy(&buf));

    Ok(())
}

/// 启动测试服务端：第 `drop_from` 个及之后的连接（从 0 计数）握手前直接关闭，
/// 其余连接完成握手后写出 `greeting` 并关闭。
fn spawn_greeting_server(
    server: Arc<rustls::ServerConfig>,
    greeting: &'static [u8],
    drop_from: usize,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        for index in 0.. {
            let (mut sock, _addr) = listener.accept().unwrap();
            if index >= drop_from {
                // 先读走 ClientHello 再关闭，保证对端收到的是 FIN 而不是 RST
                let _ = sock.read(&mut [0u8; 4096]);
                drop(sock);
                continue;
            }

            let server = Arc::clone(&server);
            thread::spawn(move || {
                let mut conn = ServerConnection::new(server).unwrap();
                conn.complete_io(&mut sock).unwrap();
                Stream::new(&mut conn, &mut sock)
                    .write_all(greeting)
                    .unwrap();
                conn.send_close_notify();
                conn.complete_io(&mut sock).unwrap();
            });
        }
    });
    addr
}

/// 第一次连接只为拿到会话票据，读到 EOF 即可。
async fn warm_up(config: Arc<ClientConfig>, addr: SocketAddr) -> io::Result<()> {
    let connector = TlsConnector::from(config);
    let domain = ServerName::try_from("foobar.com").unwrap();
    let mut stream = connector
        .connect(domain, TcpStream::connect(&addr).await?)
        .await?;
    stream.read_to_end(&mut Vec::new()).await?;
    Ok(())
}

/// 处于 0-RTT 状态且尚未写入任何数据时直接读：必须由读操作自己推进握手，
/// 而不是挂起等待另一半写入唤醒。
#[tokio::test]
async fn test_0rtt_read_before_write_completes_handshake() -> io::Result<()> {
    let (mut server, mut client) = utils::make_configs();
    server.max_early_data_size = 8192;
    client.enable_early_data = true;
    let client = Arc::new(client);
    let addr = spawn_greeting_server(Arc::new(server), b"GREETING", usize::MAX);

    warm_up(client.clone(), addr).await?;

    let connector = TlsConnector::from(client).early_data(true);
    let domain = ServerName::try_from("foobar.com").unwrap();
    let mut stream = connector
        .connect(domain, TcpStream::connect(&addr).await?)
        .await?;

    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf))
        .await
        .expect("读操作未能推进 0-RTT 握手")?;
    assert_eq!(b"GREETING", buf.as_slice());
    Ok(())
}

/// 0-RTT 状态下直接读且对端在握手期间关闭连接：必须返回 `UnexpectedEof`，不能一直挂起。
#[tokio::test]
async fn test_0rtt_read_before_write_peer_closed() -> io::Result<()> {
    let (mut server, mut client) = utils::make_configs();
    server.max_early_data_size = 8192;
    client.enable_early_data = true;
    let client = Arc::new(client);
    // 只有第 0 个连接（取票据）正常握手，之后的连接立即被关闭
    let addr = spawn_greeting_server(Arc::new(server), b"GREETING", 1);

    warm_up(client.clone(), addr).await?;

    let connector = TlsConnector::from(client).early_data(true);
    let domain = ServerName::try_from("foobar.com").unwrap();
    let mut stream = connector
        .connect(domain, TcpStream::connect(&addr).await?)
        .await?;

    let mut buf = [0u8; 16];
    let err = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("对端关闭后读操作仍然挂起")
        .expect_err("握手期间对端关闭应当返回错误");
    assert_eq!(io::ErrorKind::UnexpectedEof, err.kind());
    Ok(())
}

// Include `utils` module
include!("utils.rs");
