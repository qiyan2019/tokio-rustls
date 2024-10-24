use std::io;
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, RawSocket};
use std::pin::Pin;
#[cfg(feature = "early-data")]
use std::task::Waker;
use std::task::{Context, Poll};

use rustls::ClientConnection;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::common::{IoSession, Stream, TlsState};

/// A wrapper around an underlying raw stream which implements the TLS or SSL
/// protocol.
#[derive(Debug)]
pub struct TlsStream<IO> {
    pub(crate) io: IO,
    pub(crate) session: ClientConnection,
    pub(crate) state: TlsState,
}

impl<IO> TlsStream<IO> {
    #[inline]
    pub fn get_ref(&self) -> (&IO, &ClientConnection) {
        (&self.io, &self.session)
    }

    #[inline]
    pub fn get_mut(&mut self) -> (&mut IO, &mut ClientConnection) {
        (&mut self.io, &mut self.session)
    }

    #[inline]
    pub fn into_inner(self) -> (IO, ClientConnection) {
        (self.io, self.session)
    }
}

#[cfg(unix)]
impl<S> AsRawFd for TlsStream<S>
where
    S: AsRawFd,
{
    fn as_raw_fd(&self) -> RawFd {
        self.get_ref().0.as_raw_fd()
    }
}

#[cfg(windows)]
impl<S> AsRawSocket for TlsStream<S>
where
    S: AsRawSocket,
{
    fn as_raw_socket(&self) -> RawSocket {
        self.get_ref().0.as_raw_socket()
    }
}

impl<IO> IoSession for TlsStream<IO> {
    type Io = IO;
    type Session = ClientConnection;

    #[inline]
    fn skip_handshake(&self) -> bool {
        self.state.is_early_data()
    }

    #[inline]
    fn get_mut(&mut self) -> (&mut TlsState, &mut Self::Io, &mut Self::Session) {
        (&mut self.state, &mut self.io, &mut self.session)
    }

    #[inline]
    fn into_io(self) -> Self::Io {
        self.io
    }
}

impl<IO> AsyncRead for TlsStream<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.state {
            #[cfg(feature = "early-data")]
            TlsState::EarlyData(..) => {
                ready!(self.as_mut().poll_flush(cx))?;
                self.as_mut().poll_read(cx, buf)
            }
            TlsState::Stream | TlsState::WriteShutdown => {
                let this = self.get_mut();
                let mut stream =
                    Stream::new(&mut this.io, &mut this.session).set_eof(!this.state.readable());
                let prev = buf.remaining();

                match stream.as_mut_pin().poll_read(cx, buf) {
                    Poll::Ready(Ok(())) => {
                        if prev == buf.remaining() || stream.eof {
                            this.state.shutdown_read();
                        }

                        Poll::Ready(Ok(()))
                    }
                    Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::ConnectionAborted => {
                        this.state.shutdown_read();
                        Poll::Ready(Err(err))
                    }
                    output => output,
                }
            }
            TlsState::ReadShutdown | TlsState::FullyShutdown => Poll::Ready(Ok(())),
        }
    }
}

impl<IO> AsyncWrite for TlsStream<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Note: that it does not guarantee the final data to be sent.
    /// To be cautious, you must manually call `flush`.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let mut stream =
            Stream::new(&mut this.io, &mut this.session).set_eof(!this.state.readable());

        #[cfg(feature = "early-data")]
        {
            let bufs = [io::IoSlice::new(buf)];
            let written = ready!(poll_handle_early_data(
                &mut this.state,
                &mut stream,
                cx,
                &bufs
            ))?;
            if written != 0 {
                return Poll::Ready(Ok(written));
            }
        }

        stream.as_mut_pin().poll_write(cx, buf)
    }

    /// Note: that it does not guarantee the final data to be sent.
    /// To be cautious, you must manually call `flush`.
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let mut stream =
            Stream::new(&mut this.io, &mut this.session).set_eof(!this.state.readable());

        #[cfg(feature = "early-data")]
        {
            let written = ready!(poll_handle_early_data(
                &mut this.state,
                &mut stream,
                cx,
                bufs
            ))?;
            if written != 0 {
                return Poll::Ready(Ok(written));
            }
        }

        stream.as_mut_pin().poll_write_vectored(cx, bufs)
    }

    #[inline]
    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let mut stream =
            Stream::new(&mut this.io, &mut this.session).set_eof(!this.state.readable());

        #[cfg(feature = "early-data")]
        ready!(poll_handle_early_data(
            &mut this.state,
            &mut stream,
            cx,
            &[]
        ))?;

        stream.as_mut_pin().poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        #[cfg(feature = "early-data")]
        {
            // complete handshake
            if matches!(self.state, TlsState::EarlyData(..)) {
                ready!(self.as_mut().poll_flush(cx))?;
            }
        }

        if self.state.writeable() {
            self.session.send_close_notify();
            self.state.shutdown_write();
        }

        let this = self.get_mut();
        let mut stream =
            Stream::new(&mut this.io, &mut this.session).set_eof(!this.state.readable());
        stream.as_mut_pin().poll_shutdown(cx)
    }
}

#[cfg(feature = "early-data")]
fn poll_handle_early_data<IO>(
    state: &mut TlsState,
    stream: &mut Stream<IO, ClientConnection>,
    cx: &mut Context<'_>,
    bufs: &[io::IoSlice<'_>],
) -> Poll<io::Result<usize>>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    if let TlsState::EarlyData(pos, data) = state {
        use std::io::Write;

        // write early data
        if let Some(mut early_data) = stream.session.early_data() {
            let mut written = 0;

            for buf in bufs {
                if buf.is_empty() {
                    continue;
                }

                let len = match early_data.write(buf) {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(err) => return Poll::Ready(Err(err)),
                };

                written += len;
                data.extend_from_slice(&buf[..len]);

                if len < buf.len() {
                    break;
                }
            }

            if written != 0 {
                return Poll::Ready(Ok(written));
            }
        }

        // complete handshake
        while stream.session.is_handshaking() {
            ready!(stream.handshake(cx))?;
        }

        // write early data (fallback)
        if !stream.session.is_early_data_accepted() {
            while *pos < data.len() {
                let len = ready!(stream.as_mut_pin().poll_write(cx, &data[*pos..]))?;
                *pos += len;
            }
        }

        // end
        *state = TlsState::Stream;
    }

    Poll::Ready(Ok(0))
}
