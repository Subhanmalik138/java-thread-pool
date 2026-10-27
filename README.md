# java-thread-pool

A fixed-size thread pool built from scratch in Java — no `ExecutorService`
underneath. Workers pull tasks from a bounded blocking queue written out by
hand (circular array + condition variables), and overload is handled by an
explicit rejection policy instead of an unbounded queue that grows until the
JVM dies.

## What it does

- Fixed pool of worker threads, each looping on `queue.take()`
- `submit(Callable)` / `submit(Runnable)` returning a `Future` with `get`,
  timed `get`, `cancel`, and `isDone`
- Four rejection policies for a full queue: `ABORT`, `CALLER_RUNS`,
  `DISCARD`, `DISCARD_OLDEST`
- Graceful `shutdown()` (drains queued work, then terminates) and
  `shutdownNow()` (drops queued work)
- Live counters: active tasks, queue size, completed tasks
- A throwing task never kills its worker — the exception is delivered to the
  task's `Future` as an `ExecutionException`, and the worker moves on

## Design decisions

1. **Fixed pool, no resizing.** A fixed pool has predictable latency and no
   thread-creation storms under load. Resizing pools look clever and mostly
   just add tuning knobs nobody sets correctly.
2. **Bounded queue with an explicit policy.** An unbounded queue silently
   absorbs load until memory runs out. A bounded queue forces the caller to
   decide, up front, what overload means: fail fast, slow the caller down,
   or drop work.
3. **Dropped tasks fail their futures.** A discarded task reports
   cancellation instead of leaving `get()` blocked forever — a dropped task
   that hangs its caller is worse than a dropped task.
4. **Poison-pill shutdown.** Workers exit on a sentinel task enqueued after
   all real work, so shutdown drains the queue in order instead of cutting
   tasks off mid-flight.

## Build and run

Requires JDK 17+.

```bash
./build.sh            # compile + build out/thread-pool.jar
./build.sh test       # compile + run the JUnit 5 suite
java -jar out/thread-pool.jar            # demo: 20 tasks, 4 workers
java -jar out/thread-pool.jar 2 4        # demo: 2 workers, queue of 4
```

The test run downloads the JUnit Platform console standalone jar on first
use (or set `JUNIT_JAR` to point at one you already have).

## Usage

```java
ThreadPool pool = new ThreadPool(4, 16, RejectionPolicy.CALLER_RUNS);

Future<Integer> result = pool.submit(() -> {
    // ... real work ...
    return 42;
});
System.out.println(result.get()); // 42

pool.shutdown();
pool.awaitTermination(10, TimeUnit.SECONDS);
```

## Project layout

```
src/main/java/com/subhanmalik/threadpool/
  ThreadPool.java       pool, workers, bounded queue, rejection handling
  SimpleFuture.java     minimal Future backed by one task outcome
  RejectionPolicy.java  ABORT / CALLER_RUNS / DISCARD / DISCARD_OLDEST
  Main.java             CLI demo
src/test/java/com/subhanmalik/threadpool/
  ThreadPoolTest.java   12 JUnit 5 tests
```

## Tech stack

Java 21, JUnit 5, zero dependencies. The build is a plain `javac`/`jar`
script — no Maven or Gradle wrapper needed to try it.
