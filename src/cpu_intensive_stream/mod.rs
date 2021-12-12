use std::sync::Arc;

use futures::StreamExt;

pub mod map;

pub use map::*;

pub fn to_cpu_intensive_future<F, I, T>(
    f: F,
) -> impl Fn(I) -> futures::channel::oneshot::Receiver<T>
where
    I: 'static + Send,
    F: 'static + Fn(I) -> T + Sync + Send,
    T: 'static + Send,
{
    let f_arc = Arc::new(f);

    move |input| {
        let (sender, receiver) = futures::channel::oneshot::channel::<T>();
        let f_arc = f_arc.clone();
        rayon::spawn(move || {
            let val = f_arc(input);
            sender
                .send(val)
                .unwrap_or_else(|_| panic!("Receiver dropped"));
        });

        receiver
    }
}

impl<T: ?Sized> CPUIntensiveStreamExt for T where T: StreamExt {}

pub trait CPUIntensiveStreamExt: StreamExt {
    fn cpu_intensive_map<F, T>(self, f: F) -> CPUIntensiveMap<Self, F, T>
    where
        Self: Sized,
    {
        CPUIntensiveMap::new(self, f)
    }
}
