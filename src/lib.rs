//! Portable interface to epoll, kqueue, event ports, and IOCP.
//!
//! Supported platforms:
//! - [epoll](https://en.wikipedia.org/wiki/Epoll): Linux, Android, RedoxOS
//! - [kqueue](https://en.wikipedia.org/wiki/Kqueue): macOS, iOS, tvOS, watchOS, visionOS, FreeBSD, NetBSD, OpenBSD,
//!   DragonFly BSD
//! - [event ports](https://illumos.org/man/port_create): illumos, Solaris
//! - [poll](https://en.wikipedia.org/wiki/Poll_(Unix)): VxWorks, Fuchsia, HermitOS, other Unix systems
//! - [IOCP](https://learn.microsoft.com/en-us/windows/win32/fileio/i-o-completion-ports): Windows, Wine (version 7.13+)
//!
//! By default, polling is done in oneshot mode, which means interest in I/O events needs to
//! be re-enabled after an event is delivered if we're interested in the next event of the same
//! kind. However, level and edge triggered modes are also available for certain operating
//! systems. See the documentation of the [`PollMode`] type for more information.
//!
//! Only one thread can be waiting for I/O events at a time.
//!
//! # Examples
//!
//! ```no_run
//! use polling::{Event, Events, Poller};
//! use std::net::TcpListener;
//!
//! // Create a TCP listener.
//! let socket = TcpListener::bind("127.0.0.1:8000")?;
//! socket.set_nonblocking(true)?;
//! let key = 7; // Arbitrary key identifying the socket.
//!
//! // Create a poller and register interest in readability on the socket.
//! let poller = Poller::new()?;
//! unsafe {
//!     poller.add(&socket, Event::readable(key))?;
//! }
//!
//! // The event loop.
//! let mut events = Events::new();
//! loop {
//!     // Wait for at least one I/O event.
//!     events.clear();
//!     poller.wait(&mut events, None)?;
//!
//!     for ev in events.iter() {
//!         if ev.key == key {
//!             // Perform a non-blocking accept operation.
//!             socket.accept()?;
//!             // Set interest in the next readability event.
//!             poller.modify(&socket, Event::readable(key))?;
//!         }
//!     }
//! }
//!
//! poller.delete(&socket)?;
//! # std::io::Result::Ok(())
//! ```

#![warn(missing_docs, missing_debug_implementations, rust_2018_idioms)]
#![allow(clippy::useless_conversion, clippy::unnecessary_cast, unused_unsafe)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/smol-rs/smol/master/assets/images/logo_fullsize_transparent.png"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/smol-rs/smol/master/assets/images/logo_fullsize_transparent.png"
)]

use std::cell::Cell;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use cfg_if::cfg_if;

cfg_if! {
    // Note: This cfg is intended to make it easy for polling developers to test
    // the backend that uses poll, and is not a public API.
    if #[cfg(polling_test_poll_backend)] {
        mod poll;
        use poll as sys;
    } else if #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "redox"
    ))] {
        mod epoll;
        use epoll as sys;
    } else if #[cfg(any(
        target_os = "illumos",
        target_os = "solaris",
    ))] {
        mod port;
        use port as sys;
    } else if #[cfg(any(
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
    ))] {
        mod kqueue;
        use kqueue as sys;
    } else if #[cfg(any(
        target_os = "vxworks",
        target_os = "hermit",
        target_os = "fuchsia",
        target_os = "horizon",
        unix,
    ))] {
        mod poll;
        use poll as sys;
    } else if #[cfg(target_os = "windows")] {
        mod iocp;
        use iocp as sys;
    } else {
        compile_error!("polling does not support this target OS");
    }
}

pub mod os;

/// Key associated with notifications.
const NOTIFY_KEY: usize = usize::MAX;

/// Indicates that a file descriptor or socket can read or write without blocking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    /// Key identifying the file descriptor or socket.
    /// 
    /// 资源标识。可能是文件描述符或套接字。
    pub key: usize,
    /// Can it do a read operation without blocking?
    /// 
    /// 标记是否读就绪。(投递了可读事件)
    pub readable: bool,
    /// Can it do a write operation without blocking?
    /// 
    /// 标记是否写就绪。(投递了可写事件)
    pub writable: bool,
    /// System-specific event data.
    /// 
    /// 平台特有的额外数据。(每个平台不一样)
    extra: sys::EventExtra,
}

/// The mode in which the poller waits for I/O events.
/// 
/// 轮询器事件投递选项。(可组合)
/// 
/// 注册到轮询器中时，每个Event都对应一个PollMode，如果不指定则取默认PollMode::Oneshot。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum PollMode {
    /// Poll in oneshot mode.
    ///
    /// In this mode, the poller will only deliver one event per file descriptor or socket.
    /// Once an event has been delivered, interest in the event needs to be re-enabled
    /// by calling `Poller::modify` or `Poller::add`.
    ///
    /// This is the default mode.
    /// 
    /// 单发选项
    /// - 每次注册一次，就只限收取一次事件投递。
    /// - 想再次收取后续产生的事件，需要刷新注册(modify或add)。
    Oneshot,

    /// Poll in level-triggered mode.
    ///
    /// Once an event has been delivered, polling will continue to deliver that event
    /// until interest in the event is disabled by calling `Poller::modify` or `Poller::delete`.
    ///
    /// Not all operating system support this mode. Trying to register a file descriptor with
    /// this mode in an unsupported operating system will raise an error. You can check if
    /// the operating system supports this mode by calling `Poller::supports_level`.
    /// 
    /// 触发模式：水平触发
    /// - 依据当前资源状态是否符合预期状态触发事件：只要是期望状态，就会触发事件并投递。
    /// - 例子：轮询时只要读缓冲区有数据，就会投递读就绪事件。(无论有没有向历史轮询投递过)
    /// - 并非所有平台都支持此模式。可通过`Poller::supports_level`查看是否支持。
    Level,

    /// Poll in edge-triggered mode.
    ///
    /// Once an event has been delivered, polling will not deliver that event again unless
    /// a new event occurs.
    ///
    /// Not all operating system support this mode. Trying to register a file descriptor with
    /// this mode in an unsupported operating system will raise an error. You can check if
    /// the operating system supports this mode by calling `Poller::supports_edge`.
    /// 
    /// 触发模式：边缘触发
    /// - 依据当前资源的状态是否发生过转换且转为了预期状态来触发事件。
    /// - 例子：读缓冲区由空变为不空时。
    /// - 换句话说，对单个注册资源，每当有状态切换且切换到了预期状态都会触发事件。
    Edge,

    /// Poll in both edge-triggered and oneshot mode.
    ///
    /// This mode is similar to the `Oneshot` mode, but it will only deliver one event per new
    /// event.
    ///
    /// Not all operating system support this mode. Trying to register a file descriptor with
    /// this mode in an unsupported operating system will raise an error. You can check if
    /// the operating system supports this mode by calling `Poller::supports_edge`.
    /// 
    /// 组合选项：边缘触发且单发
    /// - 一次注册只能收取一次事件。(为边缘触发加了这个限制，等于是每次都要重新注册的边缘触发)
    /// - 事件触发模式为边缘触发。
    EdgeOneshot,
}

impl Event {
    /// Create a new event.
    pub const fn new(key: usize, readable: bool, writable: bool) -> Event {
        Event {
            key,
            readable,
            writable,
            extra: sys::EventExtra::empty(),
        }
    }

    /// All kinds of events (readable and writable).
    ///
    /// Equivalent to: `Event::new(key, true, true)`
    /// 
    /// 期望所有事件
    #[inline]
    pub const fn all(key: usize) -> Event {
        Event::new(key, true, true)
    }

    /// Only the readable event.
    ///
    /// Equivalent to: `Event::new(key, true, false)`
    /// 
    /// 期望读就绪事件。
    #[inline]
    pub const fn readable(key: usize) -> Event {
        Event::new(key, true, false)
    }

    /// Only the writable event.
    ///
    /// Equivalent to: `Event::new(key, false, true)`
    /// 
    /// 期望写就绪事件。
    #[inline]
    pub const fn writable(key: usize) -> Event {
        Event::new(key, false, true)
    }

    /// No events.
    ///
    /// Equivalent to: `Event::new(key, false, false)`
    /// 
    /// 无期望的事件。
    #[inline]
    pub const fn none(key: usize) -> Event {
        Event::new(key, false, false)
    }

    /// Add interruption events to this interest.
    ///
    /// This usually indicates that the file descriptor or socket has been closed. It corresponds
    /// to the `EPOLLHUP` and `POLLHUP` events.
    ///
    /// Interruption events are only supported on the following platforms:
    ///
    /// - `epoll`
    /// - `poll`
    /// - IOCP
    /// - Event Ports
    ///
    /// On other platforms, this function is a no-op.
    /// 
    /// 期望中断事件，是非通用的事件放在平台特定的数据中。
    /// - 通常用于是指文件或套接字被"关闭"时所触发的事件。对应EPOLLHUP和POLLHUP。
    /// - 支持此属性的框架：epoll, poll, IOCP, Event Ports
    #[inline]
    pub fn set_interrupt(&mut self, active: bool) {
        self.extra.set_hup(active);
    }

    /// Add interruption events to this interest.
    ///
    /// This usually indicates that the file descriptor or socket has been closed. It corresponds
    /// to the `EPOLLHUP` and `POLLHUP` events.
    ///
    /// Interruption events are only supported on the following platforms:
    ///
    /// - `epoll`
    /// - `poll`
    /// - IOCP
    /// - Event Ports
    ///
    /// On other platforms, this function is a no-op.
    /// 
    /// 创建一个期望中断事件的实例。
    #[inline]
    pub fn with_interrupt(mut self) -> Self {
        self.set_interrupt(true);
        self
    }

    /// Add priority events to this interest.
    ///
    /// This indicates that there is urgent data to read. It corresponds to the `EPOLLPRI` and
    /// `POLLPRI` events.
    ///
    /// Priority events are only supported on the following platforms:
    ///
    /// - `epoll`
    /// - `poll`
    /// - IOCP
    /// - Event Ports
    ///
    /// On other platforms, this function is a no-op.
    /// 
    /// 期望优先级事件
    /// - 用于指示有紧急数据到达，已读就绪。对应EPOLLPRI和POLLPRI。
    /// - 支持此属性的框架：epoll, poll, IOCP, Event Ports。
    #[inline]
    pub fn set_priority(&mut self, active: bool) {
        self.extra.set_pri(active);
    }

    /// Add priority events to this interest.
    ///
    /// This indicates that there is urgent data to read. It corresponds to the `EPOLLPRI` and
    /// `POLLPRI` events.
    ///
    /// Priority events are only supported on the following platforms:
    ///
    /// - `epoll`
    /// - `poll`
    /// - IOCP
    /// - Event Ports
    ///
    /// On other platforms, this function is a no-op.
    /// 
    /// 创建一个期望优先级事件的实例。
    #[inline]
    pub fn with_priority(mut self) -> Self {
        self.set_priority(true);
        self
    }

    /// Tell if this event is the result of an interrupt notification.
    ///
    /// This usually indicates that the file descriptor or socket has been closed. It corresponds
    /// to the `EPOLLHUP` and `POLLHUP` events.
    ///
    /// Interruption events are only supported on the following platforms:
    ///
    /// - `epoll`
    /// - `poll`
    /// - IOCP
    /// - Event Ports
    ///
    /// On other platforms, this always returns `false`.
    /// 
    /// 判定平台是否支持中断事件。
    #[inline]
    pub fn is_interrupt(&self) -> bool {
        self.extra.is_hup()
    }

    /// Tell if this event is the result of a priority notification.
    ///
    /// This indicates that there is urgent data to read. It corresponds to the `EPOLLPRI` and
    /// `POLLPRI` events.
    ///
    /// Priority events are only supported on the following platforms:
    ///
    /// - `epoll`
    /// - `poll`
    /// - IOCP
    /// - Event Ports
    ///
    /// On other platforms, this always returns `false`.
    /// 
    /// 判定平台是否支持优先级事件。
    #[inline]
    pub fn is_priority(&self) -> bool {
        self.extra.is_pri()
    }

    /// Tells if this event is the result of a connection failure.
    ///
    /// This function checks if a TCP connection has failed. It corresponds to the `EPOLLERR`  or `EPOLLHUP` event in Linux
    /// and `CONNECT_FAILED` event in Windows IOCP.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::{io, net};
    /// // Assuming polling and socket2 are included as dependencies in Cargo.toml
    /// use polling::Event;
    /// use socket2::Type;
    ///
    /// fn main() -> io::Result<()> {
    ///     let socket = socket2::Socket::new(socket2::Domain::IPV4, Type::STREAM, None)?;
    ///     let poller = polling::Poller::new()?;
    ///     unsafe {
    ///         poller.add(&socket, Event::new(0, true, true))?;
    ///     }
    ///     let addr = net::SocketAddr::new(net::Ipv4Addr::LOCALHOST.into(), 8080);
    ///     socket.set_nonblocking(true)?;
    ///     let _ = socket.connect(&addr.into());
    ///
    ///     let mut events = polling::Events::new();
    ///
    ///     events.clear();
    ///     poller.wait(&mut events, None)?;
    ///
    ///     let event = events.iter().next();
    ///
    ///     let event = match event {
    ///         Some(event) => event,
    ///         None => {
    ///             println!("no event");
    ///             return Ok(());
    ///         },
    ///     };
    ///
    ///     println!("event: {:?}", event);
    ///     if event
    ///         .is_connect_failed()
    ///         .unwrap_or_default()
    ///     {
    ///         println!("connect failed");
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// Returns `Some(true)` if the connection has failed, `Some(false)` if the connection has not failed,
    /// or `None` if the platform does not support detecting this condition.
    #[inline]
    #[deprecated(
        since = "3.4.0",
        note = "use `is_err` in combination of is_hup instead, see documentation for `is_err`"
    )]
    pub fn is_connect_failed(&self) -> Option<bool> {
        self.extra.is_connect_failed()
    }

    /// Tells if this event is the result of a connection failure.
    ///
    /// This function checks if an error exist, particularly useful in detecting if TCP connection failed. It corresponds to the `EPOLLERR` event in Linux
    /// and `CONNECT_FAILED` event in Windows IOCP.
    ///
    /// ## Caveats
    ///
    /// In `epoll`, a TCP connection failure is indicated by `EPOLLERR` + `EPOLLHUP`, though just `EPOLLERR` is enough to indicate a connection failure.
    /// EPOLLHUP may happen when we haven't event called `connect` on the socket, but it is still a valid event to check for.
    ///
    /// Returns `Some(true)` if the connection has failed, `Some(false)` if there is no error,
    /// or `None` if the platform does not support detecting this condition.
    /// 
    /// 判定投递的事件是否为连接失败。
    #[inline]
    pub fn is_err(&self) -> Option<bool> {
        self.extra.is_err()
    }

    /// Remove any extra information from this event.
    /// 
    /// 清空平台特有数据。
    #[inline]
    pub fn clear_extra(&mut self) {
        self.extra = sys::EventExtra::empty();
    }

    /// Get a version of this event with no extra information.
    ///
    /// This is useful for comparing events with `==`.
    /// 
    /// 创建无平台特定数据的事件。
    #[inline]
    pub fn with_no_extra(mut self) -> Self {
        self.clear_extra();
        self
    }
}

/// Waits for I/O events.
/// 
/// 对外暴露的轮询器。代码是跨平台的，编译时根据目标平台会自动切换底层的IO框架。
pub struct Poller {
    poller: sys::Poller,
    lock: Mutex<()>,
    notified: AtomicBool,
}

impl Poller {
    /// Creates a new poller.
    ///
    /// # Examples
    ///
    /// ```
    /// use polling::Poller;
    ///
    /// let poller = Poller::new()?;
    /// # std::io::Result::Ok(())
    /// ```
    pub fn new() -> io::Result<Poller> {
        Ok(Poller {
            poller: sys::Poller::new()?,
            lock: Mutex::new(()),
            notified: AtomicBool::new(false),
        })
    }

    /// Tell whether or not this `Poller` supports level-triggered polling.
    /// 
    /// 查看是否支持水平触发。
    pub fn supports_level(&self) -> bool {
        self.poller.supports_level()
    }

    /// Tell whether or not this `Poller` supports edge-triggered polling.
    /// 
    /// 查看是否支持边缘触发。
    pub fn supports_edge(&self) -> bool {
        self.poller.supports_edge()
    }

    /// Adds a file descriptor or socket to the poller.
    ///
    /// A file descriptor or socket is considered readable or writable when a read or write
    /// operation on it would not block. This doesn't mean the read or write operation will
    /// succeed, it only means the operation will return immediately.
    /// 
    /// 如果读或写操作变得可以无阻塞的情况下执行完成，则可认为此资源是可读的或可惜的。
    /// 但是，即使是可读的或可写的，不能保证读或写操作的结果一定是成功的。
    ///
    /// If interest is set in both readability and writability, the two kinds of events might be
    /// delivered either separately or together.
    ///
    /// For example, interest in `Event { key: 7, readable: true, writable: true }` might result in
    /// a single [`Event`] of the same form, or in two separate [`Event`]s:
    /// - `Event { key: 7, readable: true, writable: false }`
    /// - `Event { key: 7, readable: false, writable: true }`
    /// 
    /// 在通过注册单个Event同时关注可读性和可写性时，返回的Event可能是一个整合的Event也可能是两个独立的Event。
    ///
    /// Note that interest in I/O events needs to be re-enabled using
    /// [`modify()`][`Poller::modify()`] again after an event is delivered if we're interested in
    /// the next event of the same kind.
    /// 
    /// 如果已经投递了一个事件，如果还想等待相同的事件则需要更新注册的预期事件。
    ///
    /// It is possible to register interest in the same file descriptor or socket using multiple
    /// separate [`Poller`] instances. When the event is delivered, one or more [`Poller`]s are
    /// notified with that event. The exact number of [`Poller`]s notified depends on the
    /// underlying platform. When registering multiple sources into one event, the user should
    /// be careful to accommodate for events lost to other pollers.
    /// 
    /// 如果同一个预期事件注册到多个轮询器，则多个轮询器都可能被投递事件，取决于底层平台。
    /// 如果通过单个Event注册了多个源，用户需要注意其它轮询器可能不会被投递。
    ///
    /// One may also register one source into other, non-`polling` event loops, like GLib's
    /// context. While the plumbing will vary from platform to platform, in general the [`Poller`]
    /// will act as if the source was registered with another [`Poller`], with the same caveats
    /// as above.
    ///
    /// # Safety
    ///
    /// The source must be [`delete()`]d from this `Poller` before it is dropped.
    /// 
    /// # 安全性
    /// 
    /// 遗弃资源对象之前**必须**调用delete()从IO框架中删除资源。
    ///
    /// [`delete()`]: Poller::delete
    ///
    /// # Errors
    ///
    /// This method returns an error in the following situations:
    ///
    /// * If `key` equals `usize::MAX` because that key is reserved for internal use.
    /// * If an error is returned by the syscall.
    /// 
    /// # 错误说明
    /// 
    /// - 错误1：如果预期事件设置的KEY(token)等于2^64会导致错误，此值预留为内部使用。
    /// - 错误2：系统调用也可能返返回错误。
    ///
    /// # Examples
    ///
    /// Set interest in all events:
    ///
    /// ```no_run
    /// use polling::{Event, Poller};
    ///
    /// let source = std::net::TcpListener::bind("127.0.0.1:0")?;
    /// source.set_nonblocking(true)?;
    /// let key = 7;
    ///
    /// let poller = Poller::new()?;
    /// unsafe {
    ///     poller.add(&source, Event::all(key))?;
    /// }
    /// poller.delete(&source)?;
    /// # std::io::Result::Ok(())
    /// ```
    pub unsafe fn add(&self, source: impl AsRawSource, interest: Event) -> io::Result<()> {
        self.add_with_mode(source, interest, PollMode::Oneshot)
    }

    /// Adds a file descriptor or socket to the poller in the specified mode.
    ///
    /// This is identical to the `add()` function, but allows specifying the
    /// polling mode to use for this socket.
    ///
    /// # Safety
    ///
    /// The source must be [`delete()`]d from this `Poller` before it is dropped.
    ///
    /// [`delete()`]: Poller::delete
    ///
    /// # Errors
    ///
    /// If the operating system does not support the specified mode, this function
    /// will return an error.
    pub unsafe fn add_with_mode(
        &self,
        source: impl AsRawSource,
        interest: Event,
        mode: PollMode,
    ) -> io::Result<()> {
        if interest.key == NOTIFY_KEY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the key is not allowed to be `usize::MAX`",
            ));
        }
        self.poller.add(source.raw(), interest, mode)
    }

    /// Modifies the interest in a file descriptor or socket.
    ///
    /// This method has the same behavior as [`add()`][`Poller::add()`] except it modifies the
    /// interest of a previously added file descriptor or socket.
    ///
    /// To use this method with a file descriptor or socket, you must first add it using
    /// [`add()`][`Poller::add()`].
    ///
    /// Note that interest in I/O events needs to be re-enabled using
    /// [`modify()`][`Poller::modify()`] again after an event is delivered if we're interested in
    /// the next event of the same kind.
    ///
    /// # Errors
    ///
    /// This method returns an error in the following situations:
    ///
    /// * If `key` equals `usize::MAX` because that key is reserved for internal use.
    /// * If an error is returned by the syscall.
    ///
    /// # Examples
    ///
    /// To enable interest in all events:
    ///
    /// ```no_run
    /// # use polling::{Event, Poller};
    /// # let source = std::net::TcpListener::bind("127.0.0.1:0")?;
    /// # let key = 7;
    /// # let poller = Poller::new()?;
    /// # unsafe { poller.add(&source, Event::none(key))?; }
    /// poller.modify(&source, Event::all(key))?;
    /// # std::io::Result::Ok(())
    /// ```
    ///
    /// To enable interest in readable events and disable interest in writable events:
    ///
    /// ```no_run
    /// # use polling::{Event, Poller};
    /// # let source = std::net::TcpListener::bind("127.0.0.1:0")?;
    /// # let key = 7;
    /// # let poller = Poller::new()?;
    /// # unsafe { poller.add(&source, Event::none(key))?; }
    /// poller.modify(&source, Event::readable(key))?;
    /// # poller.delete(&source)?;
    /// # std::io::Result::Ok(())
    /// ```
    ///
    /// To disable interest in readable events and enable interest in writable events:
    ///
    /// ```no_run
    /// # use polling::{Event, Poller};
    /// # let poller = Poller::new()?;
    /// # let key = 7;
    /// # let source = std::net::TcpListener::bind("127.0.0.1:0")?;
    /// # unsafe { poller.add(&source, Event::none(key))? };
    /// poller.modify(&source, Event::writable(key))?;
    /// # poller.delete(&source)?;
    /// # std::io::Result::Ok(())
    /// ```
    ///
    /// To disable interest in all events:
    ///
    /// ```no_run
    /// # use polling::{Event, Poller};
    /// # let source = std::net::TcpListener::bind("127.0.0.1:0")?;
    /// # let key = 7;
    /// # let poller = Poller::new()?;
    /// # unsafe { poller.add(&source, Event::none(key))?; }
    /// poller.modify(&source, Event::none(key))?;
    /// # poller.delete(&source)?;
    /// # std::io::Result::Ok(())
    /// ```
    pub fn modify(&self, source: impl AsSource, interest: Event) -> io::Result<()> {
        self.modify_with_mode(source, interest, PollMode::Oneshot)
    }

    /// Modifies interest in a file descriptor or socket to the poller, but with the specified
    /// mode.
    ///
    /// This is identical to the `modify()` function, but allows specifying the polling mode
    /// to use for this socket.
    ///
    /// # Performance Notes
    ///
    /// This function can be used to change a source from one polling mode to another. However,
    /// on some platforms, this switch can cause delays in the delivery of events.
    ///
    /// # Errors
    ///
    /// If the operating system does not support the specified mode, this function will return
    /// an error.
    pub fn modify_with_mode(
        &self,
        source: impl AsSource,
        interest: Event,
        mode: PollMode,
    ) -> io::Result<()> {
        if interest.key == NOTIFY_KEY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the key is not allowed to be `usize::MAX`",
            ));
        }
        self.poller.modify(source.source(), interest, mode)
    }

    /// Removes a file descriptor or socket from the poller.
    ///
    /// Unlike [`add()`][`Poller::add()`], this method only removes the file descriptor or
    /// socket from the poller without putting it back into blocking mode.
    ///
    /// # Examples
    ///
    /// ```
    /// use polling::{Event, Poller};
    /// use std::net::TcpListener;
    ///
    /// let socket = TcpListener::bind("127.0.0.1:0")?;
    /// socket.set_nonblocking(true)?;
    /// let key = 7;
    ///
    /// let poller = Poller::new()?;
    /// unsafe { poller.add(&socket, Event::all(key))?; }
    /// poller.delete(&socket)?;
    /// # std::io::Result::Ok(())
    /// ```
    pub fn delete(&self, source: impl AsSource) -> io::Result<()> {
        self.poller.delete(source.source())
    }

    /// Waits for at least one I/O event and returns the number of new events.
    ///
    /// New events will be appended to `events`. If necessary, make sure to clear the
    /// [`Events`][Events::clear()] before calling [`wait()`][`Poller::wait()`]!
    ///
    /// This method will return with no new events if a notification is delivered by the
    /// [`notify()`] method, or the timeout is reached. Sometimes it may even return with no events
    /// spuriously.
    ///
    /// Only one thread can wait on I/O. If another thread is already in [`wait()`], concurrent
    /// calls to this method will return immediately with no new events.
    /// 
    /// 同一时刻只能一个线程执行wait操作，其它线程如果同时wait会立即返回且无法取到投递事件。
    ///
    /// If the operating system is ready to deliver a large number of events at once, this method
    /// may decide to deliver them in smaller batches.
    /// 
    /// 如果操作系统一次投递了大量事件，则此方法会自行决定分组投递。
    ///
    /// [`notify()`]: `Poller::notify()`
    /// [`wait()`]: `Poller::wait()`
    ///
    /// # Examples
    ///
    /// ```
    /// use polling::{Event, Events, Poller};
    /// use std::net::TcpListener;
    /// use std::time::Duration;
    ///
    /// let socket = TcpListener::bind("127.0.0.1:0")?;
    /// socket.set_nonblocking(true)?;
    /// let key = 7;
    ///
    /// let poller = Poller::new()?;
    /// unsafe {
    ///     poller.add(&socket, Event::all(key))?;
    /// }
    ///
    /// let mut events = Events::new();
    /// let n = poller.wait(&mut events, Some(Duration::from_secs(1)))?;
    /// poller.delete(&socket)?;
    /// # std::io::Result::Ok(())
    /// ```
    pub fn wait(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<usize> {
        let span = tracing::trace_span!("Poller::wait", ?timeout);
        let _enter = span.enter();

        if let Ok(_lock) = self.lock.try_lock() {
            // 计算本次操作的最终截止时间
            let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));

            loop {
                // Figure out how long to wait for.
                // 根据最终截止时间，计算出本轮循环中wait的等待时间。
                let timeout =
                    deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));

                // Wait for I/O events.
                if let Err(e) = self.poller.wait(&mut events.events, timeout) {
                    // If the wait was interrupted by a signal, clear events and try again.
                    // 如果等待时被中断，则清空投递数组，重新等待。
                    // 如果等到时报错了，则返回错误。
                    if e.kind() == io::ErrorKind::Interrupted {
                        events.clear();
                        continue;
                    } else {
                        return Err(e);
                    }
                }

                // Clear the notification, if any.
                // 当轮询器收到通知时(self.notified=true)，才会执行wait操作。
                // 如果成功执行完本次wait，则需将轮询器恢复为未通知状态，标明历史投递的事件已被取走。
                self.notified.swap(false, Ordering::SeqCst);

                // Indicate number of events.
                return Ok(events.len());
            }
        } else {
            tracing::trace!("wait: skipping because another thread is already waiting on I/O");
            Ok(0)
        }
    }

    /// Wakes up the current or the following invocation of [`wait()`].
    ///
    /// If no thread is calling [`wait()`] right now, this method will cause the following call
    /// to wake up immediately.
    /// 
    /// 唤醒轮询器：
    /// - 如果存在正在阻塞的wait，则wait会解除阻塞。
    /// - 如果不存在正在阻塞的wait，则下一次调用wait时不会阻塞。
    /// 
    /// 
    /// 原理(epoll)：
    /// - 利用：wait方法一旦取到投递事件就会解除阻塞的性质，向轮询器中注册了一个特殊的文件(event_fd或pipe_read_fd)。
    /// - 当想让wait方法解除阻塞时，就向此文件写入内容使文件变得可读，进而投递读就绪事件，wait取到投递的事件解除了阻塞。
    /// 
    /// 步骤：
    /// - 1.将轮询器标记为已唤醒状态。
    /// - 2.利用操作系统底层的功能执行唤醒。
    /// 
    /// Linux的唤醒原理：(epoll)
    /// - 
    ///
    /// [`wait()`]: `Poller::wait()`
    ///
    /// # Examples
    ///
    /// ```
    /// use polling::{Events, Poller};
    ///
    /// let poller = Poller::new()?;
    ///
    /// // Notify the poller.
    /// poller.notify()?;
    ///
    /// let mut events = Events::new();
    /// poller.wait(&mut events, None)?; // wakes up immediately
    /// assert!(events.is_empty());
    /// # std::io::Result::Ok(())
    /// ```
    pub fn notify(&self) -> io::Result<()> {
        let span = tracing::trace_span!("Poller::notify");
        let _enter = span.enter();

        if self
            .notified
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.poller.notify()?;
        }
        Ok(())
    }
}

/// A container for I/O events.
/// 
/// 用于在wait中获取所有已投递事件。是IO框架提供的Event的列表。Vec<xx::Event>。
pub struct Events {
    events: sys::Events,

    /// This is intended to be used from &mut, thread locally, so we should make it !Sync
    /// for consistency with the rest of the API.
    _not_sync: PhantomData<Cell<()>>,
}

impl Default for Events {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Events {
    /// Create a new container for events, using the default capacity.
    ///
    /// The default capacity is 1024.
    ///
    /// # Examples
    ///
    /// ```
    /// use polling::Events;
    ///
    /// let events = Events::new();
    /// ```
    #[inline]
    pub fn new() -> Self {
        // ESP-IDF has a low amount of RAM, so we use a smaller default capacity.
        #[cfg(target_os = "espidf")]
        const DEFAULT_CAPACITY: usize = 32;

        #[cfg(not(target_os = "espidf"))]
        const DEFAULT_CAPACITY: usize = 1024;

        Self::with_capacity(NonZeroUsize::new(DEFAULT_CAPACITY).unwrap())
    }

    /// Create a new container with the provided capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use polling::Events;
    /// use std::num::NonZeroUsize;
    ///
    /// let capacity = NonZeroUsize::new(1024).unwrap();
    /// let events = Events::with_capacity(capacity);
    /// ```
    #[inline]
    pub fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self {
            events: sys::Events::with_capacity(capacity.get()),
            _not_sync: PhantomData,
        }
    }

    /// Create a new iterator over I/O events.
    ///
    /// This returns all of the events in the container, excluding the notification event.
    ///
    /// 注意：需要过滤掉值为2^64的预留token。
    /// 
    /// # Examples
    ///
    /// ```
    /// use polling::{Event, Events, Poller};
    /// use std::time::Duration;
    ///
    /// # fn main() -> std::io::Result<()> {
    /// let poller = Poller::new()?;
    /// let mut events = Events::new();
    ///
    /// poller.wait(&mut events, Some(Duration::from_secs(0)))?;
    /// assert!(events.iter().next().is_none());
    /// # Ok(()) }
    /// ```
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = Event> + '_ {
        self.events.iter().filter(|ev| ev.key != NOTIFY_KEY)
    }

    /// Delete all of the events in the container.
    /// 
    /// 清空容器。
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use polling::{Event, Events, Poller};
    ///
    /// # fn main() -> std::io::Result<()> {
    /// let poller = Poller::new()?;
    /// let mut events = Events::new();
    ///
    /// /* register some sources */
    ///
    /// poller.wait(&mut events, None)?;
    ///
    /// events.clear();
    /// # Ok(()) }
    /// ```
    #[inline]
    pub fn clear(&mut self) {
        self.events.clear();
    }

    /// Returns the number of events in the container.
    ///
    /// # Examples
    ///
    /// ```
    /// use polling::Events;
    ///
    /// let events = Events::new();
    /// assert_eq!(events.len(), 0);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Returns `true` if the container contains no events.
    ///
    /// # Examples
    ///
    /// ```
    /// use polling::Events;
    ///
    /// let events = Events::new();
    /// assert!(events.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the total capacity of the list.
    ///
    /// # Examples
    ///
    /// ```
    /// use polling::Events;
    /// use std::num::NonZeroUsize;
    ///
    /// let cap = NonZeroUsize::new(10).unwrap();
    /// let events = Events::with_capacity(std::num::NonZeroUsize::new(10).unwrap());
    /// assert_eq!(events.capacity(), cap);
    /// ```
    #[inline]
    pub fn capacity(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.events.capacity()).unwrap()
    }
}

impl fmt::Debug for Events {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Events { .. }")
    }
}

#[cfg(all(
    any(
        target_os = "linux",
        target_os = "android",
        target_os = "redox",
        target_os = "illumos",
        target_os = "solaris",
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
    ),
    not(polling_test_poll_backend),
))]
#[cfg_attr(
    docsrs,
    doc(cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "redox",
        target_os = "illumos",
        target_os = "solaris",
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
    )))
)]
mod raw_fd_impl {
    use crate::Poller;
    use std::os::unix::io::{AsFd, AsRawFd, BorrowedFd, RawFd};

    impl AsRawFd for Poller {
        fn as_raw_fd(&self) -> RawFd {
            self.poller.as_raw_fd()
        }
    }

    impl AsFd for Poller {
        fn as_fd(&self) -> BorrowedFd<'_> {
            self.poller.as_fd()
        }
    }
}

#[cfg(windows)]
#[cfg_attr(docsrs, doc(cfg(windows)))]
mod raw_handle_impl {
    use crate::Poller;
    use std::os::windows::io::{AsHandle, AsRawHandle, BorrowedHandle, RawHandle};

    impl AsRawHandle for Poller {
        fn as_raw_handle(&self) -> RawHandle {
            self.poller.as_raw_handle()
        }
    }

    impl AsHandle for Poller {
        fn as_handle(&self) -> BorrowedHandle<'_> {
            self.poller.as_handle()
        }
    }
}

impl fmt::Debug for Poller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.poller.fmt(f)
    }
}

cfg_if! {
    if #[cfg(any(unix, target_os = "hermit"))] {
        #[cfg(unix)]
        use std::os::unix::io::{AsRawFd, RawFd, AsFd, BorrowedFd};
        #[cfg(target_os = "hermit")]
        use std::os::hermit::io::{AsRawFd, RawFd, AsFd, BorrowedFd};

        /// A resource with a raw file descriptor.
        pub trait AsRawSource {
            /// Returns the raw file descriptor.
            fn raw(&self) -> RawFd;
        }

        impl<T: AsRawFd> AsRawSource for &T {
            fn raw(&self) -> RawFd {
                self.as_raw_fd()
            }
        }

        impl AsRawSource for RawFd {
            fn raw(&self) -> RawFd {
                *self
            }
        }

        /// A resource with a borrowed file descriptor.
        pub trait AsSource: AsFd {
            /// Returns the borrowed file descriptor.
            fn source(&self) -> BorrowedFd<'_> {
                self.as_fd()
            }
        }

        impl<T: AsFd> AsSource for T {}
    } else if #[cfg(windows)] {
        use std::os::windows::io::{AsRawSocket, RawSocket, AsSocket, BorrowedSocket};

        /// A resource with a raw socket.
        pub trait AsRawSource {
            /// Returns the raw socket.
            fn raw(&self) -> RawSocket;
        }

        impl<T: AsRawSocket> AsRawSource for &T {
            fn raw(&self) -> RawSocket {
                self.as_raw_socket()
            }
        }

        impl AsRawSource for RawSocket {
            fn raw(&self) -> RawSocket {
                *self
            }
        }

        /// A resource with a borrowed socket.
        pub trait AsSource: AsSocket {
            /// Returns the borrowed socket.
            fn source(&self) -> BorrowedSocket<'_> {
                self.as_socket()
            }
        }

        impl<T: AsSocket> AsSource for T {}
    }
}

#[allow(unused)]
fn unsupported_error(err: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, err.into())
}

fn _assert_send_and_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<Poller>();
    assert_sync::<Poller>();

    assert_send::<Event>();
    assert_sync::<Event>();

    assert_send::<Events>();
    // Events can be !Sync
}
