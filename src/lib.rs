use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

/// A self-contained, single-threaded bi-directional channel that abuses the rust async stuff to get a simple channel abstraction.
///
/// Each instance is self-sufficient and does not rely on a global executor or task queue.
pub struct Channel<T, R> {
    handler: Rc<RefCell<dyn FnMut(T) -> Pin<Box<dyn Future<Output = R>>>>>,
}

impl<T, R> Channel<T, R> {
    /// Creates a new channel with an async handler.
    ///
    /// The handler is called every time an item is sent to the channel.
    /// The handler is a closure that captures and mutates local state (FnMut).
    pub fn new<F, Fut>(mut handler: F) -> Self
    where
        F: FnMut(T) -> Fut + 'static,
        Fut: Future<Output = R> + 'static,
    {
        Self {
            handler: Rc::new(RefCell::new(
                move |item| -> Pin<Box<dyn Future<Output = R>>> { Box::pin(handler(item)) },
            )),
        }
    }

    /// Sends an item onto the channel and returns the result synchronously.
    ///
    /// This method drives the internal async handler to completion.
    pub fn send(&self, item: T) -> R {
        let mut future = (self.handler.borrow_mut())(item);
        let state = Rc::new(WakerState {
            woken: Cell::new(true),
        });
        let waker = create_waker(state.clone());
        let mut cx = Context::from_waker(&waker);

        loop {
            if state.woken.get() {
                state.woken.set(false);
                match future.as_mut().poll(&mut cx) {
                    Poll::Ready(res) => return res,
                    Poll::Pending => {}
                }
            } else {
                // If it's pending but not woken, we have nothing to do.
                // In a single-threaded library without external events, this is a deadlock.
                panic!("Deadlock: future yielded without waking itself");
            }
        }
    }
}

impl<T, R> Clone for Channel<T, R> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
        }
    }
}

// Internal waker implementation to avoid global executor state
struct WakerState {
    woken: Cell<bool>,
}

fn create_waker(state: Rc<WakerState>) -> Waker {
    let ptr = Rc::into_raw(state) as *const ();
    let vtable = &RawWakerVTable::new(clone_raw, wake_raw, wake_by_ref_raw, drop_raw);
    unsafe { Waker::from_raw(RawWaker::new(ptr, vtable)) }
}

unsafe fn clone_raw(ptr: *const ()) -> RawWaker {
    let state = unsafe { Rc::from_raw(ptr as *const WakerState) };
    let cloned = state.clone();
    let _ = Rc::into_raw(state);
    let new_ptr = Rc::into_raw(cloned) as *const ();
    let vtable = &RawWakerVTable::new(clone_raw, wake_raw, wake_by_ref_raw, drop_raw);
    RawWaker::new(new_ptr, vtable)
}

unsafe fn wake_raw(ptr: *const ()) {
    let state = unsafe { Rc::from_raw(ptr as *const WakerState) };
    state.woken.set(true);
}

unsafe fn wake_by_ref_raw(ptr: *const ()) {
    let state = unsafe { Rc::from_raw(ptr as *const WakerState) };
    state.woken.set(true);
    let _ = Rc::into_raw(state);
}

unsafe fn drop_raw(ptr: *const ()) {
    let _ = unsafe { Rc::from_raw(ptr as *const WakerState) };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_accumulator(initial: i32) -> Channel<i32, i32> {
        let mut sum = initial;
        Channel::new(move |val: i32| {
            sum += val;
            let current = sum;
            async move { current }
        })
    }

    #[test]
    fn test_channel_basic() {
        let channel = Channel::new(|val: i32| async move { val * 2 });

        let res = channel.send(42);
        assert_eq!(res, 84);
    }

    #[test]
    fn test_multiple_requests() {
        let channel = Channel::new(|s: String| async move { s.len() });

        assert_eq!(channel.send("hello".to_string()), 5);
        assert_eq!(channel.send("world!".to_string()), 6);
    }

    #[test]
    fn test_nested_channels() {
        let doubler = Channel::new(|val: i32| async move { val * 2 });
        let adder = Channel::new({
            let doubler = doubler.clone();
            move |val: i32| {
                let doubler = doubler.clone();
                async move {
                    let doubled = doubler.send(val);
                    doubled + 1
                }
            }
        });

        assert_eq!(adder.send(10), 21);
    }

    #[test]
    fn test_mutable_state() {
        use std::cell::RefCell;
        let counter = Rc::new(RefCell::new(0));

        let channel = Channel::new({
            let counter = counter.clone();
            move |val: i32| {
                let counter = counter.clone();
                async move {
                    let mut count = counter.borrow_mut();
                    *count += val;
                    *count
                }
            }
        });

        assert_eq!(channel.send(5), 5);
        assert_eq!(channel.send(10), 15);
        assert_eq!(channel.send(3), 18);
        assert_eq!(*counter.borrow(), 18);
    }

    #[test]
    fn test_fn_mut_state() {
        let mut count = 0;
        let channel = Channel::new(move |val: i32| {
            count += val;
            let current = count;
            async move { current }
        });

        assert_eq!(channel.send(5), 5);
        assert_eq!(channel.send(10), 15);
        assert_eq!(channel.send(3), 18);
    }

    #[test]
    fn test_multiple_instances_with_factory() {
        let chan1 = make_accumulator(0);
        let chan2 = make_accumulator(100);

        assert_eq!(chan1.send(10), 10);
        assert_eq!(chan2.send(10), 110);

        assert_eq!(chan1.send(5), 15);
        assert_eq!(chan2.send(20), 130);

        // Verify chan1 was not affected by chan2
        assert_eq!(chan1.send(1), 16);
        // Verify chan2 was not affected by chan1
        assert_eq!(chan2.send(1), 131);
    }

    #[test]
    fn test_channel_clone_shared_state() {
        let chan1 = make_accumulator(0);
        let chan2 = chan1.clone();

        // Both should point to the same state
        assert_eq!(chan1.send(5), 5);
        assert_eq!(chan2.send(10), 15);
        assert_eq!(chan1.send(3), 18);
        assert_eq!(chan2.send(2), 20);
    }
}
