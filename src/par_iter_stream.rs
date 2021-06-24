#![allow(unused_imports)]
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;

use pin_project::pin_project;

use futures::pin_mut;
use futures::{Stream, StreamExt};
use rayon::iter::ParallelIterator;

use crate::hybrid_mpsc;

#[pin_project]
pub struct ParIterStream<I: ParallelIterator> {
    // #[pin]
    par_iter: Option<I>,
    #[pin]
    receiver: Option<hybrid_mpsc::Receiver<I::Item>>,
}

pub fn from_par_iter<I: ParallelIterator>(par_iter: I) -> ParIterStream<I> {
    ParIterStream {
        par_iter: Some(par_iter),
        receiver: None,
    }
}

impl<I> Stream for ParIterStream<I>
where
    I: ParallelIterator + Send + Sync + 'static,
    I::Item: Clone + Send + Sync + 'static,
{
    type Item = I::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();

        if this.receiver.is_none() {
            // the receiver has not been initialized yet

            // create and run all the machinery necessary to run the parallel iterator

            let (sender, receiver) = hybrid_mpsc::unbounded::<I::Item>();

            // launch the rayon tasks
            let par_iter = this.par_iter.take().unwrap();

            rayon::spawn(move || {
                par_iter.for_each_with(sender, |s, item| {
                    s.send(item).unwrap();
                })
            });

            // set our receiver
            this.receiver.set(Some(receiver));
        }
        // poll the receiver
        this.receiver.as_pin_mut().unwrap().poll_next(cx)
    }
}
