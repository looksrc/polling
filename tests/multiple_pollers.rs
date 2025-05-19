//! Test registering one source into multiple pollers.
//! 
//! 测试边缘触发、水平触发、单次触发对多轮询器的影响：
//! 1.水平触发：至少一个轮询器能监测到事件，其它轮询器可能监测到。
//! 2.单次触发：只有一个轮询器能监测到事件。
//! 3.边缘触发：所有轮询器都会监测到事件。

use polling::{Event, Events, PollMode, Poller};

use std::io::{self, prelude::*};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

#[test]
fn level_triggered() {
    let poller1 = Poller::new().unwrap();
    let poller2 = Poller::new().unwrap();
    let mut events = Events::new();

    if !poller1.supports_level() || !poller2.supports_level() {
        return;
    }

    // Register the source into both pollers.
    // 将单个资源同时注册到两个轮询器中，预期事件：写就绪，触发方式：水平触发。
    let (mut reader, mut writer) = tcp_pair().unwrap();
    unsafe {
        poller1
            .add_with_mode(&reader, Event::readable(1), PollMode::Level)
            .unwrap();
        poller2
            .add_with_mode(&reader, Event::readable(2), PollMode::Level)
            .unwrap();
    }

    // Neither poller should have any events.
    // 判定：两个轮询器都未收到投递事件。
    assert_eq!(
        poller1
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        0
    );
    assert!(events.is_empty());
    assert_eq!(
        poller2
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        0
    );
    assert!(events.is_empty());

    // Write to the source.
    // 写入。
    writer.write_all(&[1]).unwrap();

    // At least one poller should have an event.
    // 判定：
    // 1.首个开始监视的轮询器必收到投递事件。
    // 2.后续其它轮询器可能收到投递事件，也可能收不到。
    // 原因：
    // 水平触发时，只要状态满足条件每次等待事件时都会被投递。
    assert_eq!(
        poller1
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        1
    );
    assert_eq!(events.len(), 1);
    assert_eq!(
        events.iter().next().unwrap().with_no_extra(),
        Event::readable(1)
    );

    events.clear();
    // poller2 should have zero or one events.
    match poller2.wait(&mut events, Some(Duration::from_secs(1))) {
        Ok(1) => {
            assert_eq!(events.len(), 1);
            assert_eq!(
                events.iter().next().unwrap().with_no_extra(),
                Event::readable(2)
            );
        }
        Ok(0) => assert!(events.is_empty()),
        _ => panic!("unexpected error"),
    }

    // Writing more data should cause the same event.
    // 再次写入。
    writer.write_all(&[1]).unwrap();
    events.clear();
    assert_eq!(
        poller1
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        1
    );
    assert_eq!(events.len(), 1);
    assert_eq!(
        events.iter().next().unwrap().with_no_extra(),
        Event::readable(1)
    );

    // poller2 should have zero or one events.
    events.clear();
    match poller2.wait(&mut events, Some(Duration::from_secs(1))) {
        Ok(1) => {
            assert_eq!(events.len(), 1);
            assert_eq!(
                events.iter().next().unwrap().with_no_extra(),
                Event::readable(2)
            );
        }
        Ok(0) => assert!(events.is_empty()),
        _ => panic!("unexpected error"),
    }

    // Read from the source.
    // 通过读取清空读缓冲区。
    // 判定：所有轮询器都无法再收到事件投递。
    reader.read_exact(&mut [0; 2]).unwrap();

    // Both pollers should not have any events.
    events.clear();
    assert_eq!(
        poller1
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        0
    );
    assert!(events.is_empty());
    assert_eq!(
        poller2
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        0
    );
    assert!(events.is_empty());

    // Dereference the pollers.
    poller1.delete(&reader).unwrap();
    poller2.delete(&reader).unwrap();
}

#[test]
fn edge_triggered() {
    let poller1 = Poller::new().unwrap();
    let poller2 = Poller::new().unwrap();
    let mut events = Events::new();

    if !poller1.supports_edge() || !poller2.supports_edge() {
        return;
    }

    // Register the source into both pollers.
    let (mut reader, mut writer) = tcp_pair().unwrap();
    unsafe {
        poller1
            .add_with_mode(&reader, Event::readable(1), PollMode::Edge)
            .unwrap();
        poller2
            .add_with_mode(&reader, Event::readable(2), PollMode::Edge)
            .unwrap();
    }

    // Neither poller should have any events.
    assert_eq!(
        poller1
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        0
    );
    assert!(events.is_empty());
    assert_eq!(
        poller2
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        0
    );
    assert!(events.is_empty());

    // Write to the source.
    writer.write_all(&[1]).unwrap();

    // Both pollers should have an event.
    assert_eq!(
        poller1
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        1
    );
    assert_eq!(events.len(), 1);
    assert_eq!(
        events.iter().next().unwrap().with_no_extra(),
        Event::readable(1)
    );

    events.clear();
    assert_eq!(
        poller2
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        1
    );
    assert_eq!(events.len(), 1);
    assert_eq!(
        events.iter().next().unwrap().with_no_extra(),
        Event::readable(2)
    );

    // Writing to the poller again should cause an event.
    writer.write_all(&[1]).unwrap();

    // Both pollers should have one event.
    events.clear();
    assert_eq!(
        poller1
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        1
    );
    assert_eq!(events.len(), 1);
    assert_eq!(
        events.iter().next().unwrap().with_no_extra(),
        Event::readable(1)
    );

    events.clear();
    assert_eq!(
        poller2
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        1
    );
    assert_eq!(events.len(), 1);
    assert_eq!(
        events.iter().next().unwrap().with_no_extra(),
        Event::readable(2)
    );

    // Read from the source.
    reader.read_exact(&mut [0; 2]).unwrap();

    // Both pollers should not have any events.
    events.clear();
    assert_eq!(
        poller1
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        0
    );
    assert!(events.is_empty());
    assert_eq!(
        poller2
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        0
    );
    assert!(events.is_empty());

    // Dereference the pollers.
    poller1.delete(&reader).unwrap();
    poller2.delete(&reader).unwrap();
}

#[test]
fn oneshot_triggered() {
    let poller1 = Poller::new().unwrap();
    let poller2 = Poller::new().unwrap();
    let mut events = Events::new();

    // Register the source into both pollers.
    let (mut reader, mut writer) = tcp_pair().unwrap();
    unsafe {
        poller1
            .add_with_mode(&reader, Event::readable(1), PollMode::Oneshot)
            .unwrap();
        poller2
            .add_with_mode(&reader, Event::readable(2), PollMode::Oneshot)
            .unwrap();
    }

    // Neither poller should have any events.
    assert_eq!(
        poller1
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        0
    );
    assert!(events.is_empty());
    assert_eq!(
        poller2
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        0
    );
    assert!(events.is_empty());

    // Write to the source.
    writer.write_all(&[1]).unwrap();

    // Sources should have either one or no events.
    match poller1.wait(&mut events, Some(Duration::from_secs(1))) {
        Ok(1) => {
            assert_eq!(events.len(), 1);
            assert_eq!(
                events.iter().next().unwrap().with_no_extra(),
                Event::readable(1)
            );
        }
        Ok(0) => assert!(events.is_empty()),
        _ => panic!("unexpected error"),
    }
    events.clear();

    match poller2.wait(&mut events, Some(Duration::from_secs(1))) {
        Ok(1) => {
            assert_eq!(events.len(), 1);
            assert_eq!(
                events.iter().next().unwrap().with_no_extra(),
                Event::readable(2)
            );
        }
        Ok(0) => assert!(events.is_empty()),
        _ => panic!("unexpected error"),
    }
    events.clear();

    // Writing more data should not cause an event.
    writer.write_all(&[1]).unwrap();

    // Sources should have no events.
    assert_eq!(
        poller1
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        0
    );
    assert!(events.is_empty());
    assert_eq!(
        poller2
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        0
    );
    assert!(events.is_empty());

    // Read from the source.
    reader.read_exact(&mut [0; 2]).unwrap();

    // Sources should have no events.
    assert_eq!(
        poller1
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        0
    );
    assert!(events.is_empty());
    assert_eq!(
        poller2
            .wait(&mut events, Some(Duration::from_secs(1)))
            .unwrap(),
        0
    );
    assert!(events.is_empty());
}

fn tcp_pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let a = TcpStream::connect(listener.local_addr()?)?;
    let (b, _) = listener.accept()?;
    Ok((a, b))
}
