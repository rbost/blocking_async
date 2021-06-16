use futures::channel::oneshot::Receiver;
use futures::future::join;
use futures::pin_mut;
use futures::stream;
use futures::TryStreamExt;
use futures::{Future, Stream, StreamExt};

use std::collections::BTreeSet;
use std::sync::mpsc::channel;

use rand::distributions::{Distribution, Uniform};
use rand::thread_rng;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::thread::sleep;
use std::time::Duration;

use rayon::prelude::*;

use lazy_static::*;

use std::ops::FnOnce;

lazy_static! {
    static ref WAITING_THREADPOOL: rayon::ThreadPool = {
        rayon::ThreadPoolBuilder::new()
            .num_threads(50)
            .build()
            .unwrap()
    };
}

pub fn long_blocking_task(index: usize) -> (u64, usize) {
    let duration: u64 = 100 * Uniform::from(1u64..11u64).sample(&mut thread_rng());
    let thread_index = rayon::current_thread_index().unwrap_or(0xFFFF);

    println!(
        "Task {} duration: {} ms. Thread index: {}",
        index, duration, thread_index
    );
    sleep(Duration::from_millis(duration));

    (duration, thread_index)
}

pub async fn single_blocking_call() -> u64 {
    let (sender, receiver) = futures::channel::oneshot::channel::<u64>();

    rayon::spawn(move || sender.send(long_blocking_task(0).0).unwrap());

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
    I::IntoIter: Send + 'static,
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
    I: Iterator<Item = V> + Send + 'static,
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

                        if v.is_some() {
                            // self.size_hint.0 -= 1;
                            // self.size_hint.1 = self.size_hint.1.map(|s| s - 1);

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
                // rayon::spawn(move || {
                // tokio::task::spawn_blocking(move || {
                WAITING_THREADPOOL.spawn(move || {
                    let thread_index = rayon::current_thread_index().unwrap_or(0xFFFF);

                    println!("Attempt locking from thread {}", thread_index);

                    let mut iter = arc.lock().unwrap();
                    let _ = sender.send(iter.next());
                    println!("Send item from thread {}", thread_index);
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

async fn basic_par_iter_to_stream() {
    let (sender, receiver) = channel::<u64>();

    let producing_fut = async_cpu_intensive(|| {
        (0..50).into_par_iter().for_each_with(sender, |s, i| {
            s.send(long_blocking_task(i).0).unwrap();
        })
    });

    // producing_fut.await;
    let receiving_fut = stream_iter(receiver).collect::<Vec<u64>>();

    // let (_, v) = tokio::join!(producing_fut, receiving_fut);
    // let (_, v) = futures::join!(producing_fut, receiving_fut);
    let (v, _) = futures::join!(receiving_fut, producing_fut);

    // let v = receiving_stream.collect::<Vec<u64>>().await;
    // let v = receiver.into_iter().collect::<Vec<u64>>();
    println!("Task values: {:?}", v);
}

// #[tokio::main]
// async fn main() {
//     let iter = (0..25).map(|i| long_blocking_task(i));
//     let stream = stream_iter(iter);
//     // let stream = stream::iter(iter);
//     let mut sum = 0;
//     // while let Some(item) = stream.next().await {
//     // sum += item;
//     // }
//     let v = stream.collect::<Vec<u64>>().await;
//     println!("Task values: {:?}", v);
//     // let value = async_cpu_intensive(long_blocking_task).await;

//     // println!("Task value: {}", value);
// }

#[tokio::main]
async fn main() {
    basic_par_iter_to_stream().await;
}
