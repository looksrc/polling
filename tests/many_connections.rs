//! Tests to ensure more than 32 connections can be polled at once.
//! 测试轮询器可以同时管理超过32个连接。

// Doesn't work on OpenBSD.
#![cfg(not(target_os = "openbsd"))]

use std::io::{self, prelude::*};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use polling::Events;

#[test]
fn many_connections() {
    // Create 100 connections.
    // 创建100对TCP连接，reader为客户端，writer为服务端。
    let mut connections = Vec::new();
    for i in 0..100 {
        let (reader, writer) = tcp_pair().unwrap();
        connections.push((i, reader, writer));
    }

    // Create a poller and add all the connections.
    // 创建一个轮询器，监视所有客户端的读事件。
    let poller = polling::Poller::new().unwrap();

    for (i, reader, _) in connections.iter() {
        unsafe {
            poller.add(reader, polling::Event::readable(*i)).unwrap();
        }
    }

    let mut events = Events::new();
    while !connections.is_empty() {
        // Choose a random connection to write to.
        // 随机取出一个连接。(列表中就少一个)
        let i = fastrand::usize(..connections.len());
        let (id, mut reader, mut writer) = connections.remove(i);

        // Write a byte to the connection.
        // 向客户端写入内容
        writer.write_all(&[1]).unwrap();

        // Wait for the connection to become readable.
        // 轮询器等待事件投递。
        poller
            .wait(&mut events, Some(Duration::from_secs(10)))
            .unwrap();

        // Check that the connection is readable.
        // 判定：
        // 1.接收到1个投递事件。
        // 2.投递的事件恰好与所选泽的连接的客户端预期的事件相等。
        let current_events = events.iter().collect::<Vec<_>>();
        assert_eq!(current_events.len(), 1, "events: {:?}", current_events);
        assert_eq!(
            current_events[0].with_no_extra(),
            polling::Event::readable(id)
        );

        // Read the byte from the connection.
        // 判定：
        // 1.客户端读取到的内容为服务端发送的内容
        // 2.即，事件框架接收到的客户端读就绪事件恰与服务端的写入操作相对应。
        let mut buf = [0];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, [1]);

        // 从IO框架中移除被测试过的连接。
        poller.delete(&reader).unwrap();
        events.clear();
    }
}

/// 在本机创建一对TCP连接，返回客户端和服务端。
fn tcp_pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let a = TcpStream::connect(listener.local_addr()?)?;
    let (b, _) = listener.accept()?;
    Ok((a, b))
}
