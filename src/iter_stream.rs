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
use futures::stream::select_all::Iter;
use futures::{Future, Stream, StreamExt};

use crate::hybrid_mpsc;
use crate::utils;

use futures::stream::iter;

enum CPUIntensiveIterStreamState<I, V> {
    Iterator(I),
    Receiver(Receiver<(Option<V>, I)>),
}

pub struct CPUIntensiveIterStream<I>
where
    I: Iterator,
{
    state: Option<CPUIntensiveIterStreamState<I, I::Item>>,
}

impl<I: Iterator> Unpin for CPUIntensiveIterStream<I> {}

pub fn cpu_intensive_iter<I, V>(i: I) -> CPUIntensiveIterStream<I::IntoIter>
where
    I: IntoIterator<Item = V>,
    I::IntoIter: Send + 'static,
    V: Send + 'static,
{
    let iter = i.into_iter();

    utils::assert_stream::<I::Item, _>(CPUIntensiveIterStream {
        state: Some(CPUIntensiveIterStreamState::Iterator(iter)),
    })
}

impl<I> Stream for CPUIntensiveIterStream<I>
where
    I: Iterator + Send + 'static,
    I::Item: Send + 'static,
{
    type Item = I::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<I::Item>> {
        let state = self.state.take();

        match state {
            None => panic!("Invalid StreamIter state"),

            Some(CPUIntensiveIterStreamState::Receiver(mut receiver)) => {
                // Why not using `pin_mut!` here?
                // We need to reuse `receiver` is case the future is pending, so we cannot move it inside the macro.
                // SAFETY: It is ok to use `new_unchecked` as receiver cannot get dropped while pinned. Indeed, it stays on the stack.
                let pined_receiver = unsafe { Pin::new_unchecked(&mut receiver) };
                let status = pined_receiver.poll(cx);

                match status {
                    Poll::Pending => {
                        self.state = Some(CPUIntensiveIterStreamState::Receiver(receiver));
                        Poll::Pending
                    }
                    Poll::Ready(Err(_)) => {
                        // We should not panic here, nor should we silently
                        // discard the error
                        panic!("Unexpected oneshot cancel")
                    }
                    Poll::Ready(Ok((v, iter))) => {
                        self.state = Some(CPUIntensiveIterStreamState::Iterator(iter));
                        Poll::Ready(v)
                    }
                }
            }
            Some(CPUIntensiveIterStreamState::Iterator(iter)) => {
                let (sender, receiver) =
                    futures::channel::oneshot::channel::<(Option<Self::Item>, I)>();

                let mut iter = iter;
                rayon::spawn(move || {
                    let v = iter.next();
                    let _ = sender.send((v, iter));
                });
                self.state = Some(CPUIntensiveIterStreamState::Receiver(receiver));
                self.poll_next(cx)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.state {
            None => panic!("Invalid StreamIter state"),
            Some(CPUIntensiveIterStreamState::Iterator(iter)) => iter.size_hint(),
            _ => (0, None),
        }
    }
}

pub trait IterStreamExt: IntoIterator + Sized {
    fn into_stream(self) -> futures::stream::Iter<Self::IntoIter> {
        iter(self.into_iter())
    }

    fn into_cpu_intensive_stream(self) -> CPUIntensiveIterStream<Self::IntoIter>
    where
        Self::IntoIter: Send + 'static,
        Self::Item: Send + 'static,
    {
        cpu_intensive_iter(self.into_iter())
    }
}

impl<T: core::iter::IntoIterator> IterStreamExt for T {}
