package com.subhanmalik.threadpool;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A minimal {@link Future} implementation backed by a single task's outcome.
 * The worker thread completes it exactly once; waiters block on a monitor
 * until the result (or failure, or cancellation) is available.
 */
public final class SimpleFuture<V> implements Future<V> {

    private V result;
    private Throwable failure;
    private boolean done;
    private boolean cancelled;

    /** Called by the worker thread when the task finishes normally. */
    synchronized void complete(V value) {
        if (done) {
            return;
        }
        result = value;
        done = true;
        notifyAll();
    }

    /** Called by the worker thread when the task throws. */
    synchronized void completeExceptionally(Throwable t) {
        if (done) {
            return;
        }
        failure = t;
        done = true;
        notifyAll();
    }

    @Override
    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
        // Tasks already running cannot be interrupted by this pool (they run
        // on worker threads the pool does not track individually), but a task
        // still sitting in the queue can be cancelled before it starts.
        if (done) {
            return false;
        }
        cancelled = true;
        done = true;
        notifyAll();
        return true;
    }

    @Override
    public synchronized boolean isCancelled() {
        return cancelled;
    }

    @Override
    public synchronized boolean isDone() {
        return done;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        try {
            return get(0, null);
        } catch (TimeoutException e) {
            // get(0, null) never times out; unreachable.
            throw new AssertionError(e);
        }
    }

    @Override
    public synchronized V get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        long deadlineNanos = unit == null ? 0 : System.nanoTime() + unit.toNanos(timeout);
        while (!done) {
            if (unit == null) {
                wait();
            } else {
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    throw new TimeoutException("timed out waiting for task result");
                }
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
        }
        if (cancelled) {
            throw new CancellationException("task was cancelled");
        }
        if (failure != null) {
            throw new ExecutionException("task threw an exception", failure);
        }
        return result;
    }
}
