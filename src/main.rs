use futures::stream::iter;
use futures::StreamExt;

// use std::collections::BTreeSet;
use std::sync::mpsc::channel;

use rand::distributions::{Distribution, Uniform};
use rand::thread_rng;

use std::thread::sleep;
use std::time::Duration;

use rayon::prelude::*;

use lazy_static::*;

use std::ops::FnOnce;

pub mod cpu_intensive_stream;
pub mod hybrid_mpsc;
pub mod iter_stream;
pub mod par_iter_stream;
pub mod simple_latch;
pub(crate) mod utils;

use crate::cpu_intensive_stream::*;
use crate::iter_stream::IterStreamExt;

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

// async fn basic_par_iter_to_stream() {
//     let (sender, receiver) = channel::<u64>();

//     let producing_fut = async_cpu_intensive(|| {
//         (0..50).into_par_iter().for_each_with(sender, |s, i| {
//             s.send(long_blocking_task(i).0).unwrap();
//         })
//     });

//     // producing_fut.await;
//     let receiving_fut = receiver
//         // .into_iter()
//         .into_cpu_intensive_stream()
//         .collect::<Vec<u64>>();

//     // let (_, v) = tokio::join!(producing_fut, receiving_fut);
//     // let (_, v) = futures::join!(producing_fut, receiving_fut);
//     let (v, _) = futures::join!(receiving_fut, producing_fut);

//     // let v = receiving_stream.collect::<Vec<u64>>().await;
//     // let v = receiver.into_iter().collect::<Vec<u64>>();
//     println!("Task values: {:?}", v);
// }

async fn hybrid_mpsc_par_iter_to_stream() {
    let (sender, receiver) = hybrid_mpsc::unbounded::<u64>();
    let (waiter, notifier) = simple_latch::simple_latch();

    let producing_fut = async_cpu_intensive(|| {
        (0..50)
            .into_par_iter()
            .for_each_with((sender, waiter), |(s, w), i| {
                w.wait();
                s.send(long_blocking_task(i).0).unwrap();
            })
    });

    // let receiving_fut = receiver.collect::<Vec<u64>>();
    // let receiving_fut = notifier
    // .async_drop_latch()
    // .and_then(|()| receiver.collect::<Vec<u64>>());

    let receiving_fut = async {
        notifier.drop_latch();
        receiver.collect::<Vec<u64>>().await
    };
    // let (_, v) = tokio::join!(producing_fut, receiving_fut);
    // let (_, v) = futures::join!(producing_fut, receiving_fut);
    let (v, _) = futures::join!(receiving_fut, producing_fut);

    println!("Task values: {:?}", v);
}

async fn stream_par_iter() {
    let stream =
        par_iter_stream::from_par_iter((0..50).into_par_iter().map(|i| long_blocking_task(i).0));

    let v = stream.collect::<Vec<u64>>().await;
    println!("Task values: {:?}", v);
}

async fn cpu_intensive_iter_to_stream() {
    let iter = (0..10).map(|i| long_blocking_task(i).0);
    let stream = iter.into_cpu_intensive_stream();
    let vector = stream.collect::<Vec<u64>>().await;
    println!("Task values: {:?}", vector);
}

async fn cpu_intensive_map() {
    let stream = iter(0..10)
        .cpu_intensive_map(long_blocking_task)
        .filter_map(|res| async move { res.map(|x| x.0).ok() });
    let vector = stream.collect::<Vec<u64>>().await;
    println!("Task values: {:?}", vector);
}
// async fn mut_stream_par_iter() {
//     let mut v: Vec<usize> = (0..50).collect();
//     v.par_iter_mut().for_each(|i| *i += 1);
//     // let stream = par_iter_stream::from_par_iter(v.par_iter_mut().map(|i| long_blocking_task(*i).0));

//     // let vector = stream.collect::<Vec<u64>>().await;
//     // println!("Task values: {:?}", vector);
// }

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
    // cpu_intensive_map().await;
    cpu_intensive_iter_to_stream().await;
    // basic_par_iter_to_stream().await;
    // hybrid_mpsc_par_iter_to_stream().await;
    // stream_par_iter().await;
}
