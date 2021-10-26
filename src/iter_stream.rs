#![allow(unused_imports)]
use core::panic;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::RwLock;
use std::task::{Context, Poll};

use core::iter::Iterator;

use futures::channel::oneshot::Receiver;
use futures::future::Pending;
use futures::pin_mut;
use futures::{Future, Stream, StreamExt};

use crate::hybrid_mpsc;
use crate::utils;

enum StreamIterState<I, V> {
    Iterator(I),
    Receiver(Receiver<(Option<V>, I)>),
}

pub struct StreamIter<I, V> {
    state: Option<StreamIterState<I, V>>,
}

impl<I, V> Unpin for StreamIter<I, V> {}

pub fn stream_iter<I, V>(i: I) -> StreamIter<I::IntoIter, V>
where
    I: IntoIterator<Item = V>,
    I::IntoIter: Send + 'static,
    V: Send + 'static,
{
    let iter = i.into_iter();

    utils::assert_stream::<I::Item, _>(StreamIter {
        state: Some(StreamIterState::Iterator(iter)),
    })
}

impl<I, V> Stream for StreamIter<I, V>
where
    I: Iterator<Item = V> + Send + 'static,
    V: Send + 'static,
{
    type Item = I::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<I::Item>> {
        let state = self.state.take();

        match state {
            None => panic!("Invalid StreamIter state"),

            Some(StreamIterState::Receiver(mut receiver)) => {
                // Why not using `pin_mut!` here?
                // We need to reuse `receiver` is case the future is pending, so we cannot move it inside the macro.
                // SAFETY: It is ok to use `new_unchecked` as receiver cannot get dropped while pinned. Indeed, it stays on the stack.
                let pined_receiver = unsafe { Pin::new_unchecked(&mut receiver) };
                let status = pined_receiver.poll(cx);

                match status {
                    Poll::Pending => {
                        self.state = Some(StreamIterState::Receiver(receiver));
                        Poll::Pending
                    }
                    Poll::Ready(Err(_)) => {
                        panic!("Unexpected oneshot cancel")
                    }
                    Poll::Ready(Ok((v, iter))) => {
                        self.state = Some(StreamIterState::Iterator(iter));
                        Poll::Ready(v)
                    }
                }
            }
            Some(StreamIterState::Iterator(iter)) => {
                let (sender, receiver) =
                    futures::channel::oneshot::channel::<(Option<Self::Item>, I)>();

                let mut iter = iter;
                rayon::spawn(move || {
                    let v = iter.next();
                    let _ = sender.send((v, iter));
                });
                self.state = Some(StreamIterState::Receiver(receiver));
                self.poll_next(cx)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.state {
            None => panic!("Invalid StreamIter state"),
            Some(StreamIterState::Iterator(iter)) => iter.size_hint(),
            _ => (0, None),
        }
    }
}
