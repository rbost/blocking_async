use std::sync::Arc;
use std::task::{Context, Poll};

use crossbeam_channel::*;
use futures::stream::FusedStream;
use futures::task::AtomicWaker;
use futures::Stream;

use pin_project::pin_project;

pub struct ChannelState<T> {
    waker: AtomicWaker,
    phantom: std::marker::PhantomData<T>,
}

unsafe impl<T> Send for ChannelState<T> {}
unsafe impl<T> Sync for ChannelState<T> {}

pub struct Sender<T> {
    shared_state: Arc<ChannelState<T>>,
    inner_sender: crossbeam_channel::Sender<T>,
}

impl<T> Sender<T> {
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        println!("Send value");
        self.inner_sender
            .send(msg)
            .map(|()| self.shared_state.waker.wake())
    }
}

// We have to manually implement Clone: Sender is Clone even when T is not Clone
impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            shared_state: self.shared_state.clone(),
            inner_sender: self.inner_sender.clone(),
        }
    }
}

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let (inner_sender, inner_receiver) = crossbeam_channel::unbounded::<T>();
    let shared_state = Arc::new(ChannelState::<T> {
        waker: AtomicWaker::new(),
        phantom: std::marker::PhantomData,
    });

    let sender = Sender {
        shared_state: shared_state.clone(),
        inner_sender,
    };
    let receiver = Receiver {
        shared_state,
        inner_receiver,
        finished: false,
    };

    (sender, receiver)
}

pub fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    let (inner_sender, inner_receiver) = crossbeam_channel::bounded::<T>(cap);
    let shared_state = Arc::new(ChannelState::<T> {
        waker: AtomicWaker::new(),
        phantom: std::marker::PhantomData,
    });

    let sender = Sender {
        shared_state: shared_state.clone(),
        inner_sender,
    };
    let receiver = Receiver {
        shared_state,
        inner_receiver,
        finished: false,
    };

    (sender, receiver)
}

#[pin_project]
pub struct Receiver<T> {
    #[pin]
    shared_state: Arc<ChannelState<T>>,
    #[pin]
    inner_receiver: crossbeam_channel::Receiver<T>,
    finished: bool,
}

impl<T> Receiver<T> {
    pub fn recv(&self) -> Result<T, RecvError> {
        self.inner_receiver.recv()
    }
}

impl<T> Stream for Receiver<T>
where
    T: Send,
{
    type Item = T;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        println!("Poll...");

        // fast path to avoid registering a waker if not needed
        match self.inner_receiver.try_recv() {
            Ok(v) => {
                println!("Found value (fast path)");
                return Poll::Ready(Some(v));
            }
            Err(TryRecvError::Disconnected) => {
                println!("End of stream (fast path)");
                *self.project().finished = true;
                return Poll::Ready(None);
            }
            Err(TryRecvError::Empty) => (),
        }

        self.shared_state.waker.register(cx.waker());

        match self.inner_receiver.try_recv() {
            Ok(v) => {
                println!("Found value");
                Poll::Ready(Some(v))
            }
            Err(TryRecvError::Disconnected) => {
                println!("End of stream");
                *self.project().finished = true;
                Poll::Ready(None)
            }
            Err(TryRecvError::Empty) => Poll::Pending,
        }
    }
}

impl<T> FusedStream for Receiver<T>
where
    T: Send,
{
    fn is_terminated(&self) -> bool {
        self.finished
    }
}
