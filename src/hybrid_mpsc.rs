use std::sync::Arc;
use std::task::{Context, Poll};

use crossbeam_channel::*;
use futures::task::AtomicWaker;
use futures::Stream;

pub struct ChannelState<T> {
    waker: AtomicWaker,
    phantom: std::marker::PhantomData<T>,
}

#[derive(Clone)]
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
    };

    (sender, receiver)
}

// #[derive(Debug)]
pub struct Receiver<T> {
    shared_state: Arc<ChannelState<T>>,
    inner_receiver: crossbeam_channel::Receiver<T>,
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
                Poll::Ready(None)
            }
            Err(TryRecvError::Empty) => Poll::Pending,
        }
    }
}
