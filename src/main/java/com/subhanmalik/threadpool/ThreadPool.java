package com.subhanmalik.threadpool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A small fixed-size thread pool built from scratch on top of a bounded
 * blocking queue — the same shape as {@code java.util.concurrent.ThreadPoolExecutor}
 * with a fixed pool size, minus the parts nobody needs in a demo.
 *
 * <p>Design decisions:</p>
 * <ul>
 *   <li>One dedicated worker thread per slot, each looping on
 *       {@code queue.take()}. No dynamic resizing: a fixed pool has
 *       predictable latency and no thread-creation storms under load.</li>
 *   <li>A bounded queue with an explicit {@link RejectionPolicy} instead of an
 *       unbounded one. An unbounded queue silently absorbs load until the JVM
 *       runs out of memory; a bounded queue forces the caller to decide what
 *       overload means, up front.</li>
 *   <li>Graceful shutdown via a poison pill per worker, so in-flight tasks
 *       finish and queued tasks drain before the pool reports terminated.</li>
 *   <li>Tasks are wrapped so a throwing task never kills its worker: the
 *       exception is captured into the returned {@link Future} and the worker
 *       loops on to the next task.</li>
 * </ul>
 */
public final class ThreadPool {

    /** Marker task that tells a worker to exit its loop. */
    private static final Runnable POISON = () -> {};

    private final int poolSize;
    private final BoundedQueue queue;
    private final RejectionPolicy rejectionPolicy;
    private final List<Thread> workers = new ArrayList<>();
    private final AtomicLong completedTasks = new AtomicLong();
    private final AtomicInteger activeTasks = new AtomicInteger();

    private final ReentrantLock stateLock = new ReentrantLock();
    private final Condition terminated = stateLock.newCondition();
    private volatile boolean shutdown;
    private volatile int terminatedWorkers;

    /**
     * @param poolSize number of worker threads; must be positive
     * @param queueCapacity maximum number of tasks waiting for a worker; must be positive
     * @param rejectionPolicy what to do when the queue is full
     */
    public ThreadPool(int poolSize, int queueCapacity, RejectionPolicy rejectionPolicy) {
        if (poolSize <= 0) {
            throw new IllegalArgumentException("poolSize must be positive");
        }
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be positive");
        }
        if (rejectionPolicy == null) {
            throw new IllegalArgumentException("rejectionPolicy must not be null");
        }
        this.poolSize = poolSize;
        this.queue = new BoundedQueue(queueCapacity);
        this.rejectionPolicy = rejectionPolicy;
        for (int i = 0; i < poolSize; i++) {
            Thread worker = new Thread(this::workerLoop, "threadpool-worker-" + i);
            worker.setDaemon(false);
            worker.start();
            workers.add(worker);
        }
    }

    /** Submit a task and receive a {@link Future} for its result. */
    public <V> Future<V> submit(Callable<V> task) {
        if (task == null) {
            throw new NullPointerException("task must not be null");
        }
        SimpleFuture<V> future = new SimpleFuture<>();
        offerOrReject(new TaskWrapper<>(task, future));
        return future;
    }

    /** Submit a {@link Runnable}; the future completes with {@code null}. */
    public Future<?> submit(Runnable task) {
        return submit(() -> {
            task.run();
            return null;
        });
    }

    /** Fire-and-forget submission; the returned future is still usable for errors. */
    public Future<?> execute(Runnable task) {
        return submit(task);
    }

    /**
     * Stop accepting new tasks. Workers finish queued work, then exit.
     * Idempotent.
     */
    public void shutdown() {
        if (shutdown) {
            return;
        }
        shutdown = true;
        for (int i = 0; i < poolSize; i++) {
            // Poison pills go through the same bounded queue so they are
            // processed only after all previously queued work.
            queue.putUninterruptibly(POISON);
        }
    }

    /**
     * Like {@link #shutdown()} but also discards tasks still waiting in the
     * queue. Already-running tasks are left to finish.
     */
    public void shutdownNow() {
        for (Runnable dropped : queue.clear()) {
            cancelIfTask(dropped);
        }
        shutdown();
    }

    /** Block until all workers have exited, or the timeout elapses. */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        stateLock.lock();
        try {
            while (terminatedWorkers < poolSize) {
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                terminated.awaitNanos(remaining);
            }
            return true;
        } finally {
            stateLock.unlock();
        }
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public boolean isTerminated() {
        return terminatedWorkers == poolSize;
    }

    /** Tasks currently executing on workers (excludes queued tasks). */
    public int getActiveCount() {
        return activeTasks.get();
    }

    /** Tasks waiting in the queue (excludes running tasks). */
    public int getQueueSize() {
        return queue.size();
    }

    /** Tasks that have finished since the pool was created. */
    public long getCompletedTaskCount() {
        return completedTasks.get();
    }

    private void offerOrReject(Runnable task) {
        if (shutdown) {
            throw new RejectedExecutionException("pool is shut down");
        }
        if (queue.offer(task)) {
            return;
        }
        switch (rejectionPolicy) {
            case ABORT:
                throw new RejectedExecutionException("work queue is full");
            case CALLER_RUNS:
                task.run();
                break;
            case DISCARD:
                // Never leave the caller's future hanging: a dropped task
                // reports cancellation instead of blocking get() forever.
                cancelIfTask(task);
                break;
            case DISCARD_OLDEST:
                cancelIfTask(queue.poll()); // drop the oldest waiting task, if any
                if (!queue.offer(task)) {
                    // A worker grabbed the slot between poll and offer; retry
                    // once before giving up rather than silently losing the task.
                    if (!queue.offer(task)) {
                        cancelIfTask(task);
                        throw new RejectedExecutionException("work queue is full");
                    }
                }
                break;
            default:
                throw new IllegalStateException("unknown policy: " + rejectionPolicy);
        }
    }

    /** A dropped queued task must fail its future, never leave it hanging. */
    private static void cancelIfTask(Runnable r) {
        if (r instanceof TaskWrapper<?> wrapper) {
            wrapper.cancel();
        }
    }

    /**
     * Wraps a user task with completion bookkeeping. A throwing task is
     * captured into its future instead of killing the worker thread.
     */
    private final class TaskWrapper<V> implements Runnable {
        private final Callable<V> task;
        private final SimpleFuture<V> future;

        TaskWrapper(Callable<V> task, SimpleFuture<V> future) {
            this.task = task;
            this.future = future;
        }

        void cancel() {
            future.cancel(false);
        }

        @Override
        public void run() {
            if (future.isCancelled()) {
                return; // cancelled while queued: skip, don't run
            }
            activeTasks.incrementAndGet();
            try {
                future.complete(task.call());
            } catch (Throwable t) {
                future.completeExceptionally(t);
            } finally {
                activeTasks.decrementAndGet();
                completedTasks.incrementAndGet();
            }
        }
    }

    private void workerLoop() {
        try {
            while (true) {
                Runnable task = queue.take();
                if (task == POISON) {
                    break;
                }
                task.run();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            stateLock.lock();
            try {
                terminatedWorkers++;
                if (terminatedWorkers == poolSize) {
                    terminated.signalAll();
                }
            } finally {
                stateLock.unlock();
            }
        }
    }

    /**
     * A bounded FIFO queue built on a circular array with explicit
     * not-full / not-empty conditions — the same mechanics as
     * {@code ArrayBlockingQueue}, written out so the blocking behaviour is
     * visible instead of hidden behind a library call.
     */
    private static final class BoundedQueue {
        private final Runnable[] items;
        private int head;
        private int tail;
        private int count;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition notFull = lock.newCondition();
        private final Condition notEmpty = lock.newCondition();

        BoundedQueue(int capacity) {
            items = new Runnable[capacity];
        }

        boolean offer(Runnable task) {
            lock.lock();
            try {
                if (count == items.length) {
                    return false;
                }
                items[tail] = task;
                tail = (tail + 1) % items.length;
                count++;
                notEmpty.signal();
                return true;
            } finally {
                lock.unlock();
            }
        }

        Runnable take() throws InterruptedException {
            lock.lock();
            try {
                while (count == 0) {
                    notEmpty.await();
                }
                Runnable task = items[head];
                items[head] = null;
                head = (head + 1) % items.length;
                count--;
                notFull.signal();
                return task;
            } finally {
                lock.unlock();
            }
        }

        Runnable poll() {
            lock.lock();
            try {
                if (count == 0) {
                    return null;
                }
                Runnable task = items[head];
                items[head] = null;
                head = (head + 1) % items.length;
                count--;
                notFull.signal();
                return task;
            } finally {
                lock.unlock();
            }
        }

        void putUninterruptibly(Runnable task) {
            lock.lock();
            try {
                while (count == items.length) {
                    notFull.awaitUninterruptibly();
                }
                items[tail] = task;
                tail = (tail + 1) % items.length;
                count++;
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }

        /** Remove every queued task and return them so callers can fail their futures. */
        List<Runnable> clear() {
            lock.lock();
            try {
                List<Runnable> dropped = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    dropped.add(items[(head + i) % items.length]);
                    items[(head + i) % items.length] = null;
                }
                head = 0;
                tail = 0;
                count = 0;
                notFull.signalAll();
                return dropped;
            } finally {
                lock.unlock();
            }
        }

        int size() {
            lock.lock();
            try {
                return count;
            } finally {
                lock.unlock();
            }
        }
    }
}
