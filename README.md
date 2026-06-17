# CH: Self-Contained Async Bi-Directional Channel

A minimal, single-threaded Rust library for bi-directional communication using async handlers, with zero external dependencies.
It is purposely not possible to share channels across threads.

## Features

- **Sync API, Async Logic**: Completely hides `async/await` implementation details from the caller.
- **Stateful Handlers**: Supports `FnMut` closures, allowing handlers to maintain and mutate their own state across requests.
- **Zero Dependencies**: Built entirely on the Rust standard library (`std`).
- **Self-Sufficient**: Each channel instance is independent and contains its own internal executor logic—no global state or background threads required.
- **Bi-Directional**: Send a request and block until a response is received from an asynchronous handler.
- **Lightweight**: Ideal for embedded systems or local task coordination where a full-blown async runtime is overkill.

## Usage

### Basic Example

```rust
use ch::Channel;

fn main() {
    // Create a channel with an async handler
    let channel = Channel::new(|val: i32| async move {
        val * 2
    });

    // Send items synchronously; the async logic is driven to completion internally
    let result = channel.send(42);
    assert_eq!(result, 84);
}
```

### Stateful Handlers (`FnMut`)

You can capture and mutate state directly within the handler closure.

```rust
use ch::Channel;

let mut count = 0;
let channel = Channel::new(async move |val: i32| {
    count += val;
    let current = count;
    async move { current }
});

assert_eq!(channel.send(5), 5);
assert_eq!(channel.send(10), 15);
```

### Nested Communication

Channels can be cloned and shared, even across other channel handlers.

```rust
use ch::Channel;

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
```

## How it Works

`ch` implements a minimal "micro-executor" inside each `send` call. When you call `send()`, the library:
1. Invokes your async handler to get a `Future`.
2. Creates a local `Waker` tied to the current stack frame.
3. Polls the future to completion in a loop, ensuring the library remains single-threaded and avoids global task queues.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
ch = "0.1.0"
```

## Running Tests

```bash
cargo test
```
