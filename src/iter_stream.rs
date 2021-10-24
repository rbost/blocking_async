#![allow(unused_imports)]
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::RwLock;
use std::task::{Context, Poll};

use core::iter::Iterator;

use futures::channel::oneshot::Receiver;
use futures::pin_mut;
use futures::{Future, Stream, StreamExt};

use crate::hybrid_mpsc;
use crate::utils;

pub struct StreamIter<I, V> {
    iter: Option<I>,
    state: Option<Receiver<(Option<V>, I)>>,
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
        iter: Some(iter),
        state: None,
    })
}

impl<I, V> Stream for StreamIter<I, V>
where
    I: Iterator<Item = V> + Send + 'static,
    V: Send + 'static,
{
    type Item = I::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<I::Item>> {
        match &mut self.state {
            Some(receiver) => {
                pin_mut!(receiver);
                receiver.poll(cx).map(|res| match res {
                    Err(_) => {
                        panic!("Unexpected oneshot cancel")
                    }
                    Ok((v, iter)) => {
                        self.state = None;
                        self.iter = Some(iter);
                        v
                    }
                })
            }
            None => {
                let (sender, receiver) =
                    futures::channel::oneshot::channel::<(Option<Self::Item>, I)>();

                let mut iter = self.iter.take().unwrap();

                rayon::spawn(move || {
                    let v = iter.next();
                    let _ = sender.send((v, iter));
                });
                self.state = Some(receiver);
                self.poll_next(cx)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.iter {
            None => (0, None),
            Some(iter) => iter.size_hint(),
        }
    }
}
