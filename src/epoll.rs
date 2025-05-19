//! Bindings to epoll (Linux, Android).

use std::io;
use std::os::unix::io::{AsFd, AsRawFd, BorrowedFd, RawFd};
use std::time::Duration;

#[cfg(not(target_os = "redox"))]
use rustix::event::{eventfd, EventfdFlags};
#[cfg(not(target_os = "redox"))]
use rustix::time::{
    timerfd_create, timerfd_settime, Itimerspec, TimerfdClockId, TimerfdFlags, TimerfdTimerFlags,
};

use rustix::buffer::spare_capacity;
use rustix::event::{epoll, Timespec};
use rustix::fd::OwnedFd;
use rustix::fs::{fcntl_getfl, fcntl_setfl, OFlags};
use rustix::io::{fcntl_getfd, fcntl_setfd, read, write, FdFlags};
use rustix::pipe::{pipe, pipe_with, PipeFlags};

use crate::{Event, PollMode};

/// Interface to epoll.
/// 
/// 封装Linux的epoll。
#[derive(Debug)]
pub struct Poller {
    /// File descriptor for the epoll instance.
    /// 
    /// epoll实例的句柄。
    epoll_fd: OwnedFd,

    /// Notifier used to wake up epoll.
    /// 
    /// 通知器，用来解除正在阻塞中的wait()函数。
    notifier: Notifier,

    /// File descriptor for the timerfd that produces timeouts.
    ///
    /// Redox does not support timerfd.
    /// 
    /// 利用timerfd实现超时功能。不支持Redox。(why)
    #[cfg(not(target_os = "redox"))]
    timer_fd: Option<OwnedFd>,
}

impl Poller {
    /// Creates a new poller.
    /// 
    /// 创建Poller。
    /// - epoll_fd
    /// - Notifier：event_fd 或 pipe。将读期望注册到epoll。
    /// - timer_fd。注册到epoll。
    pub fn new() -> io::Result<Poller> {
        // Create an epoll instance.
        //
        // Use `epoll_create1` with `EPOLL_CLOEXEC`.
        let epoll_fd = epoll::create(epoll::CreateFlags::CLOEXEC)?;

        // Set up notifier and timerfd.
        let notifier = Notifier::new()?;
        #[cfg(not(target_os = "redox"))]
        let timer_fd = timerfd_create(
            TimerfdClockId::Monotonic,
            TimerfdFlags::CLOEXEC | TimerfdFlags::NONBLOCK,
        )
        .ok();

        let poller = Poller {
            epoll_fd,
            notifier,
            #[cfg(not(target_os = "redox"))]
            timer_fd,
        };

        unsafe {
            #[cfg(not(target_os = "redox"))]
            if let Some(ref timer_fd) = poller.timer_fd {
                poller.add(
                    timer_fd.as_raw_fd(),
                    Event::none(crate::NOTIFY_KEY),
                    PollMode::Oneshot,
                )?;
            }

            poller.add(
                poller.notifier.as_fd().as_raw_fd(),
                Event::readable(crate::NOTIFY_KEY),
                PollMode::Oneshot,
            )?;
        }

        tracing::trace!(
            epoll_fd = ?poller.epoll_fd.as_raw_fd(),
            notifier = ?poller.notifier,
            "new",
        );
        Ok(poller)
    }

    /// Whether this poller supports level-triggered events.
    pub fn supports_level(&self) -> bool {
        true
    }

    /// Whether the poller supports edge-triggered events.
    pub fn supports_edge(&self) -> bool {
        true
    }

    /// Adds a new file descriptor.
    ///
    /// # Safety
    ///
    /// The `fd` must be a valid file descriptor. The usual condition of remaining registered in
    /// the `Poller` doesn't apply to `epoll`.
    pub unsafe fn add(&self, fd: RawFd, ev: Event, mode: PollMode) -> io::Result<()> {
        let span = tracing::trace_span!(
            "add",
            epoll_fd = ?self.epoll_fd.as_raw_fd(),
            ?fd,
            ?ev,
        );
        let _enter = span.enter();

        epoll::add(
            &self.epoll_fd,
            unsafe { rustix::fd::BorrowedFd::borrow_raw(fd) },
            epoll::EventData::new_u64(ev.key as u64),
            epoll_flags(&ev, mode) | ev.extra.flags,
        )?;

        Ok(())
    }

    /// Modifies an existing file descriptor.
    pub fn modify(&self, fd: BorrowedFd<'_>, ev: Event, mode: PollMode) -> io::Result<()> {
        let span = tracing::trace_span!(
            "modify",
            epoll_fd = ?self.epoll_fd.as_raw_fd(),
            ?fd,
            ?ev,
        );
        let _enter = span.enter();

        epoll::modify(
            &self.epoll_fd,
            fd,
            epoll::EventData::new_u64(ev.key as u64),
            epoll_flags(&ev, mode) | ev.extra.flags,
        )?;

        Ok(())
    }

    /// Deletes a file descriptor.
    pub fn delete(&self, fd: BorrowedFd<'_>) -> io::Result<()> {
        let span = tracing::trace_span!(
            "delete",
            epoll_fd = ?self.epoll_fd.as_raw_fd(),
            ?fd,
        );
        let _enter = span.enter();

        epoll::delete(&self.epoll_fd, fd)?;

        Ok(())
    }

    /// Waits for I/O events with an optional timeout.
    /// 
    /// 等待操作系统向epoll投递事件。
    /// 
    /// 对于非redox系统，利用timer_fd实现超时：
    /// 1.将超时时间转为timer_fd的超时时间。
    /// 2.将timer_fd注册到epoll框架，一旦timer_fd到期，epoll的wait会收到timer_fd事件，wait解除阻塞。
    /// 
    /// 对于redox系统，利用epoll_wait自带的超时功能。
    /// 
    /// todo: 为什么不全都使用epoll_wait自带的超时？
    /// 
    /// 最后：别忘了重置通知器状态，并刷新通知器句柄，因为每次的wait调用都收到通知以后才会被调用。
    #[allow(clippy::needless_update)]
    pub fn wait(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        let span = tracing::trace_span!(
            "wait",
            epoll_fd = ?self.epoll_fd.as_raw_fd(),
            ?timeout,
        );
        let _enter = span.enter();

        #[cfg(not(target_os = "redox"))]
        if let Some(ref timer_fd) = self.timer_fd {
            // Configure the timeout using timerfd.
            let new_val = Itimerspec {
                it_interval: TS_ZERO,
                it_value: match timeout {
                    None => TS_ZERO,
                    Some(t) => {
                        let mut ts = TS_ZERO;
                        ts.tv_sec = t.as_secs() as _;
                        ts.tv_nsec = t.subsec_nanos() as _;
                        ts
                    }
                },
                ..unsafe { std::mem::zeroed() }
            };

            timerfd_settime(timer_fd, TimerfdTimerFlags::empty(), &new_val)?;

            // Set interest in timerfd.
            self.modify(
                timer_fd.as_fd(),
                Event::readable(crate::NOTIFY_KEY),
                PollMode::Oneshot,
            )?;
        }

        #[cfg(not(target_os = "redox"))]
        let timer_fd = &self.timer_fd;
        #[cfg(target_os = "redox")]
        let timer_fd: Option<core::convert::Infallible> = None;

        // Timeout for epoll. In case of overflow, use no timeout.
        //
        // 如果是redox，不会启用timer_fd，非None的超时时间设置epoll_wait。
        // 如果非redox，启用timer_fd，并给epoll_wait设置None超时时间。
        let timeout = match (timer_fd, timeout) {
            (_, Some(t)) if t == Duration::from_secs(0) => Some(Timespec::default()),
            (None, Some(t)) => Timespec::try_from(t).ok(),
            _ => None,
        };

        // Wait for I/O events.
        epoll::wait(
            &self.epoll_fd,
            spare_capacity(&mut events.list),
            timeout.as_ref(),
        )?;
        tracing::trace!(
            epoll_fd = ?self.epoll_fd.as_raw_fd(),
            res = ?events.list.len(),
            "new events",
        );

        // Clear the notification (if received) and re-register interest in it.
        self.notifier.clear();
        self.modify(
            self.notifier.as_fd(),
            Event::readable(crate::NOTIFY_KEY),
            PollMode::Oneshot,
        )?;
        Ok(())
    }

    /// Sends a notification to wake up the current or next `wait()` call.
    pub fn notify(&self) -> io::Result<()> {
        let span = tracing::trace_span!(
            "notify",
            epoll_fd = ?self.epoll_fd.as_raw_fd(),
            notifier = ?self.notifier,
        );
        let _enter = span.enter();

        self.notifier.notify();
        Ok(())
    }
}

impl AsRawFd for Poller {
    fn as_raw_fd(&self) -> RawFd {
        self.epoll_fd.as_raw_fd()
    }
}

impl AsFd for Poller {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.epoll_fd.as_fd()
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        let span = tracing::trace_span!(
            "drop",
            epoll_fd = ?self.epoll_fd.as_raw_fd(),
            notifier = ?self.notifier,
        );
        let _enter = span.enter();

        #[cfg(not(target_os = "redox"))]
        if let Some(timer_fd) = self.timer_fd.take() {
            let _ = self.delete(timer_fd.as_fd());
        }
        let _ = self.delete(self.notifier.as_fd());
    }
}

/// `timespec` value that equals zero.
#[cfg(not(target_os = "redox"))]
const TS_ZERO: Timespec = unsafe { std::mem::transmute([0u8; std::mem::size_of::<Timespec>()]) };

/// Get the EPOLL flags for the interest.
/// 
/// 拼装epoll_add方法中的标志位，包含：
/// - 事件投递模式。
/// - 预期的事件类型。
fn epoll_flags(interest: &Event, mode: PollMode) -> epoll::EventFlags {
    let mut flags = match mode {
        PollMode::Oneshot => epoll::EventFlags::ONESHOT,
        PollMode::Level => epoll::EventFlags::empty(),
        PollMode::Edge => epoll::EventFlags::ET,
        PollMode::EdgeOneshot => epoll::EventFlags::ET | epoll::EventFlags::ONESHOT,
    };
    if interest.readable {
        flags |= read_flags();
    }
    if interest.writable {
        flags |= write_flags();
    }
    flags
}

/// Epoll flags for all possible readability events.
/// 
/// 所有可导致读就绪的事件。（可以唤醒读取操作)
/// 
/// 包括：Epoll::IN | Epoll::HUP | Epoll::ERR | Epoll::PRI。
fn read_flags() -> epoll::EventFlags {
    use epoll::EventFlags as Epoll;
    Epoll::IN | Epoll::HUP | Epoll::ERR | Epoll::PRI
}

/// Epoll flags for all possible writability events.
/// 
/// 所有可导致写就绪的事件。（可以唤醒写入操作)
/// 
/// 包括：Epoll::OUT | Epoll::HUP | Epoll::ERR。
fn write_flags() -> epoll::EventFlags {
    use epoll::EventFlags as Epoll;
    Epoll::OUT | Epoll::HUP | Epoll::ERR
}

/// A list of reported I/O events.
/// 
/// 已投递事件的原始列表。
pub struct Events {
    list: Vec<epoll::Event>,
}

unsafe impl Send for Events {}

impl Events {
    /// Creates an empty list.
    pub fn with_capacity(cap: usize) -> Events {
        Events {
            list: Vec::with_capacity(cap),
        }
    }

    /// Iterates over I/O events.
    /// 
    /// 将已投递的原始事件列表适配为读写就绪事件。
    /// 因为外部使用者只需要知道读写操作是否可以继续执行。
    /// 
    /// 原始事件包含了IO相关的数据事件、失败事件等，这些事件最终都会传递给读写操作。
    pub fn iter(&self) -> impl Iterator<Item = Event> + '_ {
        self.list.iter().map(|ev| {
            let flags = ev.flags;
            Event {
                key: ev.data.u64() as usize,
                readable: flags.intersects(read_flags()),
                writable: flags.intersects(write_flags()),
                extra: EventExtra { flags },
            }
        })
    }

    /// Clear the list.
    pub fn clear(&mut self) {
        self.list.clear();
    }

    /// Get the capacity of the list.
    pub fn capacity(&self) -> usize {
        self.list.capacity()
    }
}

/// Extra information about this event.
/// 
/// 投递事件对应的额外数据，对于epoll来说是所有标志位。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventExtra {
    flags: epoll::EventFlags,
}

impl EventExtra {
    /// Create an empty version of the data.
    #[inline]
    pub const fn empty() -> EventExtra {
        EventExtra {
            flags: epoll::EventFlags::empty(),
        }
    }

    /// Add the interrupt flag to this event.
    #[inline]
    pub fn set_hup(&mut self, active: bool) {
        self.flags.set(epoll::EventFlags::HUP, active);
    }

    /// Add the priority flag to this event.
    #[inline]
    pub fn set_pri(&mut self, active: bool) {
        self.flags.set(epoll::EventFlags::PRI, active);
    }

    /// Tell if the interrupt flag is set.
    #[inline]
    pub fn is_hup(&self) -> bool {
        self.flags.contains(epoll::EventFlags::HUP)
    }

    /// Tell if the priority flag is set.
    #[inline]
    pub fn is_pri(&self) -> bool {
        self.flags.contains(epoll::EventFlags::PRI)
    }

    #[inline]
    pub fn is_connect_failed(&self) -> Option<bool> {
        Some(
            self.flags.contains(epoll::EventFlags::ERR)
                && self.flags.contains(epoll::EventFlags::HUP),
        )
    }

    #[inline]
    pub fn is_err(&self) -> Option<bool> {
        Some(self.flags.contains(epoll::EventFlags::ERR))
    }
}

/// The notifier for Linux.
///
/// Certain container runtimes do not expose eventfd to the client, as it relies on the host and
/// can be used to "escape" the container under certain conditions. Gramine is the prime example,
/// see [here](gramine). In this case, fall back to using a pipe.
///
/// [gramine]: https://gramine.readthedocs.io/en/stable/manifest-syntax.html#allowing-eventfd
/// 
/// 通知器作用：
/// - todo.. 
/// 
/// 针对Linux提供了两种通知器
/// - EventFd，需要event_fd支持。(一个特殊的文件，可实现类似管道的功能)。
/// - Pipe，不支持EventFd时(redox)或专门测试管道时(polling_test_epoll_pipe)使用。
/// - 注意：创建这两种文件句柄时，都禁止子进程继承：EventfdFlags::CLOEXEC。
#[derive(Debug)]
enum Notifier {
    /// The primary notifier, using eventfd.
    #[cfg(not(target_os = "redox"))]
    EventFd(OwnedFd),

    /// The fallback notifier, using a pipe.
    Pipe {
        /// The read end of the pipe.
        read_pipe: OwnedFd,

        /// The write end of the pipe.
        write_pipe: OwnedFd,
    },
}

impl Notifier {
    /// Create a new notifier.
    fn new() -> io::Result<Self> {
        // Skip eventfd for testing if necessary.
        // 利用系统调用创建event_fd文件
        // 注意：在测试管道(polling_test_epoll_pipe)或平台为redox时，禁用此方式。
        #[cfg(not(target_os = "redox"))]
        {
            if !cfg!(polling_test_epoll_pipe) {
                // Try to create an eventfd.
                match eventfd(0, EventfdFlags::CLOEXEC | EventfdFlags::NONBLOCK) {
                    Ok(fd) => {
                        tracing::trace!("created eventfd for notifier");
                        return Ok(Notifier::EventFd(fd));
                    }

                    Err(err) => {
                        tracing::warn!(
                            "eventfd() failed with error ({}), falling back to pipe",
                            err
                        );
                    }
                }
            }
        }

        // 利用系统调用创建管道
        let (read, write) = pipe_with(PipeFlags::CLOEXEC).or_else(|_| {
            let (read, write) = pipe()?;
            fcntl_setfd(&read, fcntl_getfd(&read)? | FdFlags::CLOEXEC)?;
            fcntl_setfd(&write, fcntl_getfd(&write)? | FdFlags::CLOEXEC)?;
            io::Result::Ok((read, write))
        })?;

        fcntl_setfl(&read, fcntl_getfl(&read)? | OFlags::NONBLOCK)?;
        Ok(Notifier::Pipe {
            read_pipe: read,
            write_pipe: write,
        })
    }

    /// The file descriptor to register in the poller.
    /// 
    /// 获取读端的句柄。
    fn as_fd(&self) -> BorrowedFd<'_> {
        match self {
            #[cfg(not(target_os = "redox"))]
            Notifier::EventFd(fd) => fd.as_fd(),
            Notifier::Pipe {
                read_pipe: read, ..
            } => read.as_fd(),
        }
    }

    /// Notify the poller.
    /// 
    /// 通知轮询器：
    /// - event_fd：向文件中以本地字节序写入u64类型的1。
    /// - pipe：从写端写入一个全零字节。
    fn notify(&self) {
        match self {
            #[cfg(not(target_os = "redox"))]
            Self::EventFd(fd) => {
                let buf: [u8; 8] = 1u64.to_ne_bytes();
                let _ = write(fd, &buf);
            }

            Self::Pipe { write_pipe, .. } => {
                write(write_pipe, &[0; 1]).ok();
            }
        }
    }

    /// Clear the notification.
    /// 
    /// 清除通知：
    /// - event_fd：读取一次文件即可清除。
    /// - pipe：读端读取一次即可清除。
    fn clear(&self) {
        match self {
            #[cfg(not(target_os = "redox"))]
            Self::EventFd(fd) => {
                let mut buf = [0u8; 8];
                let _ = read(fd, &mut buf);
            }

            Self::Pipe { read_pipe, .. } => while read(read_pipe, &mut [0u8; 1024]).is_ok() {},
        }
    }
}
