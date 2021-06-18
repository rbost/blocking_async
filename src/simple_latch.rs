use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::{Condvar, Mutex};

struct SimpleLatchState {
    flag: AtomicBool,
    cond_var: Condvar,
    mutex: Mutex<()>, // the mutex is here only for the condition variable
}

pub struct SimpleLatchNotifier {
    state: Arc<SimpleLatchState>,
}

#[derive(Clone)]
pub struct SimpleLatchWaiter {
    state: Arc<SimpleLatchState>,
}

impl SimpleLatchState {
    pub(self) fn wait(&self) {
        // fast path if the flag is set
        if !self.flag.load(Ordering::Acquire) {
            let _guard = self
                .cond_var
                .wait_while(self.mutex.lock().unwrap(), |()| {
                    self.flag.load(Ordering::Acquire)
                })
                .unwrap();
        }
    }

    pub(self) fn drop_latch(&self) {
        self.flag.store(true, Ordering::Release);
        self.cond_var.notify_all();
    }
}

impl SimpleLatchWaiter {
    pub fn wait(&self) {
        self.state.wait();
    }
}

impl SimpleLatchNotifier {
    pub fn drop_latch(&self) {
        self.state.drop_latch();
    }

    pub async fn async_drop_latch(&self) {
        self.drop_latch();
    }
}

pub fn simple_latch() -> (SimpleLatchWaiter, SimpleLatchNotifier) {
    let flag = AtomicBool::new(false);
    let cond_var = Condvar::new();
    let mutex = Mutex::new(());

    let state = Arc::new(SimpleLatchState {
        flag,
        cond_var,
        mutex,
    });

    let waiter = SimpleLatchWaiter {
        state: state.clone(),
    };

    let notifier = SimpleLatchNotifier { state };

    (waiter, notifier)
}
