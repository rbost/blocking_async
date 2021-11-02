use std::sync::Arc;

use futures::stream::Then;
use futures::{Future, FutureExt, Stream, StreamExt, TryStream, TryStreamExt};
use rayon::prelude::*;

pub fn to_cpu_intensive_future<F, I, T>(
    f: F,
) -> impl Fn(I) -> futures::channel::oneshot::Receiver<T>
where
    I: 'static + Send,
    F: 'static + Fn(I) -> T + Sync + Send,
    T: 'static + Send,
{
    let f_arc = Arc::new(f);
    let g = move |input| {
        let (sender, receiver) = futures::channel::oneshot::channel::<T>();
        let f_arc = f_arc.clone();
        rayon::spawn(move || {
            let val = f_arc(input);
            sender
                .send(val)
                .unwrap_or_else(|_| panic!("Receiver dropped"));
        });

        receiver
        // .await.unwrap_or(0)
        // let out = f(input);
        // futures::future::ready(out)
    };
    g
}
pub trait CPUIntensiveStreamExt: StreamExt + Sized {
    // fn cpu_intensive_map<Fut, F, T, G>(self, f: F) -> Then<Self, Fut, G>
    // where
    //     F: FnMut(Self::Item) -> T,
    //     G: FnMut(Self::Item) -> Fut,
    //     Fut: Future,
    //     Self: Sized,
    // {
    //     let g_fut = |input| async move { f(input) };
    //     // let fut = |input| async move { f(input) };
    //     self.then(|input| async move { f(input) })
    //     // self.then(|x| async move { x + 3 })
    // }
    // fn try_cpu_intensive_map<F, T, O>(
    //     self,
    //     f: F,
    //     // ) -> impl Stream<Item = Result<T, futures::channel::oneshot::Canceled>>
    // ) -> O
    // // impl Stream<Item = Result<T, futures::channel::oneshot::Canceled>>
    // where
    //     Self: 'static + Stream,
    //     Self::Item: Send,
    //     F: 'static + Fn(Self::Item) -> T + Sync + Send,
    //     T: 'static + Send,
    //     O: Stream<Item = Result<T, futures::channel::oneshot::Canceled>>,
    // {
    //     let f_arc = Arc::new(f);
    //     let g = move |input| {
    //         let (sender, receiver) = futures::channel::oneshot::channel::<T>();
    //         let f_arc = f_arc.clone();
    //         rayon::spawn(move || {
    //             let val = f_arc(input);
    //             sender
    //                 .send(val)
    //                 .unwrap_or_else(|_| panic!("Receiver dropped"));
    //         });

    //         receiver
    //         // .await.unwrap_or(0)
    //         // let out = f(input);
    //         // futures::future::ready(out)
    //     };

    //     self.then(g)
    // }

    fn try_cpu_intensive_map<F, T, O, Fut, G>(self, f: F) -> Then<Self, Fut, G>
    where
        Self: 'static + Stream,
        Self::Item: Send,
        F: 'static + Fn(Self::Item) -> T + Sync + Send,
        T: 'static + Send,
        // Fut: futures::channel::oneshot::Receiver<T>,
        Fut: Future<Output = Result<T, futures::channel::oneshot::Canceled>>,
        G: Fn(Self::Item) -> Fut,
    {
        self.then(to_cpu_intensive_future(f))
    }
}

// impl<T: ?Sized> CPUIntensiveStreamExt for T where T: StreamExt {}

pub fn try_cpu_intensive_map<S, F, T>(
    stream: S,
    f: F,
) -> impl Stream<Item = Result<T, futures::channel::oneshot::Canceled>>
where
    S: 'static + Stream,
    S::Item: Send,
    F: 'static + Fn(S::Item) -> T + Sync + Send,
    T: 'static + Send,
{
    stream.then(to_cpu_intensive_future(f))
}

pub fn cpu_intensive_map<S, F, T>(stream: S, f: F) -> impl Stream<Item = T>
where
    S: 'static + Stream,
    S::Item: Send,
    F: 'static + Fn(S::Item) -> T + Sync + Send,
    T: 'static + Send,
{
    // try_cpu_intensive_map(stream, f).map(|res| res.expect("Sender dropped"))
    try_cpu_intensive_map(stream, f).filter_map(|res| async move { res.ok() })
}

pub fn cpu_intensive_map_test<'a, S, F, T>(stream: S, mut f: F) -> impl Stream + 'a
//Then<S, Fut, G>
where
    S: 'a + Stream,
    F: 'a + FnMut(S::Item) -> T,
    T: 'a,
{
    let g = move |input| {
        let out = f(input);
        futures::future::ready(out)
    };

    stream.then(g)
}
