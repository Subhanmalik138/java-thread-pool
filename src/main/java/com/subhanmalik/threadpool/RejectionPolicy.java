package com.subhanmalik.threadpool;

/**
 * What the pool does when a task is submitted but no worker is free and the
 * work queue is full.
 */
public enum RejectionPolicy {
    /** Throw {@link java.util.concurrent.RejectedExecutionException}. */
    ABORT,
    /** Run the task on the submitting (calling) thread instead of dropping it. */
    CALLER_RUNS,
    /** Silently drop the task. */
    DISCARD,
    /** Drop the oldest queued task, then enqueue the new one. */
    DISCARD_OLDEST
}
