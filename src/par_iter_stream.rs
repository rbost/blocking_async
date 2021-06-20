use std::ops::DerefMut;
use std::pin::Pin;

use pin_project::pin_project;

use futures::pin_mut;
use futures::{Stream, StreamExt};
use rayon::iter::ParallelIterator;

use crate::hybrid_mpsc;

#[pin_project]
pub struct ParIterStream<I: ParallelIterator> {
    par_iter: I,
    #[pin]
    receiver: Option<hybrid_mpsc::Receiver<I::Item>>,
}

impl<I> Stream for ParIterStream<I>
where
    I: ParallelIterator,
{
    type Item = I::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();

        match this.receiver.as_pin_mut() {
            Some(recv) => recv.poll_next(cx),
            None => std::task::Poll::Pending,
        }
    }
}
