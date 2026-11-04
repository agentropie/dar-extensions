//! Local wire-level HTTP adapter used by behavioral tests.
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
    task::JoinHandle,
};

#[derive(Debug)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub headers: String,
    pub body: Vec<u8>,
}

pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
}
impl Response {
    pub fn json(value: serde_json::Value) -> Self {
        Self {
            status: 200,
            body: serde_json::to_vec(&value).unwrap(),
            headers: vec![("Content-Type".into(), "application/json".into())],
        }
    }
    pub fn bytes(body: Vec<u8>) -> Self {
        Self {
            status: 200,
            body,
            headers: Vec::new(),
        }
    }
    pub fn status(status: u16) -> Self {
        Self {
            status,
            body: b"{}".to_vec(),
            headers: Vec::new(),
        }
    }
}

pub struct Server {
    pub base: String,
    requests: mpsc::Receiver<Request>,
    task: JoinHandle<()>,
}
impl Server {
    pub async fn start(responses: Vec<Response>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (tx, requests) = mpsc::channel(100);
        let task = tokio::spawn(async move {
            for response in responses {
                let (mut socket, _) =
                    tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                        .await
                        .expect("expected HTTP request")
                        .unwrap();
                let mut bytes = Vec::new();
                let end = loop {
                    let mut buffer = [0; 8192];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0, "incomplete request");
                    bytes.extend_from_slice(&buffer[..count]);
                    if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
                let size = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                            .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                while bytes.len() - end < size {
                    let mut buffer = [0; 8192];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0, "incomplete request body");
                    bytes.extend_from_slice(&buffer[..count]);
                }
                let mut first_line = headers.lines().next().unwrap().split_whitespace();
                let request = Request {
                    method: first_line.next().unwrap().into(),
                    path: first_line.next().unwrap().into(),
                    headers,
                    body: bytes[end..end + size].to_vec(),
                };
                tx.send(request).await.unwrap();
                let mut header =
                    format!("HTTP/1.1 {} test\r\nConnection: close\r\n", response.status);
                if !response.headers.iter().any(|(key, _)| {
                    key.eq_ignore_ascii_case("content-length")
                        || key.eq_ignore_ascii_case("transfer-encoding")
                }) {
                    header.push_str(&format!("Content-Length: {}\r\n", response.body.len()));
                }
                for (key, value) in response.headers {
                    header.push_str(&format!("{key}: {value}\r\n"));
                }
                header.push_str("\r\n");
                socket.write_all(header.as_bytes()).await.unwrap();
                let _ = socket.write_all(&response.body).await;
            }
        });
        Self {
            base,
            requests,
            task,
        }
    }
    pub async fn next(&mut self) -> Request {
        tokio::time::timeout(std::time::Duration::from_secs(5), self.requests.recv())
            .await
            .expect("request deadline")
            .expect("expected request")
    }
    pub async fn finish(self) {
        self.task.await.unwrap();
    }
}
