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

// SAFETY: ChannelState is Send + Sync in every case as it does not contain a
// T object, just a PhantomData that prevents auto implementation of these
// traits
unsafe impl<T> Send for ChannelState<T> {}
unsafe impl<T> Sync for ChannelState<T> {}

pub struct Sender<T> {
    shared_state: Arc<ChannelState<T>>,
    inner_sender: crossbeam_channel::Sender<T>,
}

impl<T> Sender<T> {
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
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
        // fast path to avoid registering a waker if not needed
        match self.inner_receiver.try_recv() {
            Ok(v) => {
                return Poll::Ready(Some(v));
            }
            Err(TryRecvError::Disconnected) => {
                *self.project().finished = true;
                return Poll::Ready(None);
            }
            Err(TryRecvError::Empty) => (),
        }

        self.shared_state.waker.register(cx.waker());

        match self.inner_receiver.try_recv() {
            Ok(v) => Poll::Ready(Some(v)),
            Err(TryRecvError::Disconnected) => {
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
