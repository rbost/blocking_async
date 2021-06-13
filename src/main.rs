use futures::channel::oneshot::Receiver;
use futures::pin_mut;
use futures::stream;
use futures::TryStreamExt;
use futures::{Future, Stream, StreamExt};

use rand::distributions::{Distribution, Uniform};
use rand::thread_rng;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::thread::sleep;
use std::time::Duration;

use std::ops::FnOnce;

pub fn long_blocking_task() -> u64 {
    let duration: u64 = 100 * Uniform::from(0u64..10u64).sample(&mut thread_rng());

    println!("Task duration: {} ms", duration);
    sleep(Duration::from_millis(duration));

    duration
}

pub async fn single_blocking_call() -> u64 {
    let (sender, receiver) = futures::channel::oneshot::channel::<u64>();

    rayon::spawn(move || sender.send(long_blocking_task()).unwrap());

    receiver.await.unwrap_or(0)
}

pub async fn async_cpu_intensive<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (sender, receiver) = futures::channel::oneshot::channel::<F::Output>();
    rayon::spawn(move || {
        let _ = sender.send(f());
    });

    receiver.await.unwrap()
}

pub struct StreamIter<I, V> {
    iter: Arc<Mutex<I>>,
    state: Option<Receiver<Option<V>>>,
    size_hint: (usize, Option<usize>),
}

impl<I, V> Unpin for StreamIter<I, V> {}

pub(crate) fn assert_stream<T, S>(stream: S) -> S
where
    S: Stream<Item = T>,
{
    stream
}

pub fn stream_iter<I, V>(i: I) -> StreamIter<I::IntoIter, V>
where
    I: IntoIterator<Item = V>,
    I::IntoIter: Sync + Send + 'static,
    V: Send + 'static,
{
    let iter = i.into_iter();
    let size_hint = iter.size_hint();
    println!("Initial size hint {:?}", size_hint);

    assert_stream::<I::Item, _>(StreamIter {
        iter: Arc::new(Mutex::new(iter)),
        state: None,
        size_hint,
    })
}

impl<I, V> Stream for StreamIter<I, V>
where
    I: Iterator<Item = V> + Sync + Send + 'static,
    V: Send + 'static,
{
    type Item = I::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<I::Item>> {
        match &mut self.state {
            Some(receiver) => {
                pin_mut!(receiver);
                let res = receiver.poll(cx).map(|res| match res {
                    Err(_) => {
                        panic!("Unexpected oneshot cancel")
                    }
                    Ok(v) => {
                        self.state = None;

                        if (v.is_some()) {
                            self.size_hint.0 -= 1;
                            self.size_hint.1 = self.size_hint.1.map(|s| s - 1);
                            println!("New size hint {:?}", self.size_hint);
                        } else {
                            println!("Finished stream");
                        }
                        v
                    }
                });
                res
            }
            None => {
                let arc = self.iter.clone();
                let (sender, receiver) = futures::channel::oneshot::channel::<Option<Self::Item>>();
                rayon::spawn(move || {
                    let mut iter = arc.lock().unwrap();
                    let _ = sender.send(iter.next());
                });
                self.state = Some(receiver);
                self.poll_next(cx)
            }
        }
        // let (sender, receiver) = futures::channel::oneshot::channel::<u64>();

        // Poll::Ready(self.iter.next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.size_hint
    }
}

// pub fn iter_to_stream<I, V, F, Fut>(iter: I) -> futures::stream::Unfold<I, F, Fut>
// where
//     I: Iterator<Item = V> + Send + 'static,
//     V: Send + 'static,
//     F: FnMut(I) -> Fut,
//     Fut: Future<Output = Option<(V, I)>>,
// {
//     stream::unfold(iter, |mut iter: I| async move {
//         let (v, next_state) = async_cpu_intensive(|| {
//             let v = iter.next();
//             (v, iter)
//         })
//         .await;
//         v.map(|val| (val, next_state))
//     })
// }

#[tokio::main]
async fn main() {
    let iter = (0..25).map(|_| long_blocking_task());
    let stream = stream_iter(iter);
    // let stream = stream::iter(iter);
    let mut sum = 0;
    // while let Some(item) = stream.next().await {
    // sum += item;
    // }
    let v = stream.collect::<Vec<u64>>().await;
    println!("Task values: {:?}", v);
    // let value = async_cpu_intensive(long_blocking_task).await;

    // println!("Task value: {}", value);
}
