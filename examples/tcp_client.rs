use std::{io, net};

use polling::Event;
use socket2::Type;

fn main() -> io::Result<()> {
    // 客户端所用的套接字
    let socket = socket2::Socket::new(socket2::Domain::IPV4, Type::STREAM, None)?;

    // 创建轮询器，加入被监控的资源
    let poller = polling::Poller::new()?;
    unsafe {
        poller.add(&socket, Event::new(0, true, true))?;
    }

    // 设置非阻塞模式，发起向服务端发起连接
    let addr = net::SocketAddr::new(net::Ipv4Addr::LOCALHOST.into(), 8080);
    socket.set_nonblocking(true)?;
    let _ = socket.connect(&addr.into());

    // 等待被监控的事件
    let mut events = polling::Events::new();
    events.clear();
    poller.wait(&mut events, None)?;

    // 处理已发生的事件
    let event = events.iter().next();
    let event = match event {
        Some(event) => event,
        None => {
            println!("no event");
            return Ok(());
        }
    };

    // 处理事件失败
    println!("event: {:?}", event);
    if event.is_err().unwrap_or(false) {
        println!("connect failed");
    }

    Ok(())
}
