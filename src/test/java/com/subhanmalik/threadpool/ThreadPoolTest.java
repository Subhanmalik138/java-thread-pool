package com.subhanmalik.threadpool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ThreadPoolTest {

    private static void shutdownQuietly(ThreadPool pool) throws InterruptedException {
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "pool did not terminate");
    }

    @Test
    void callableResultIsReturnedThroughFuture() throws Exception {
        ThreadPool pool = new ThreadPool(2, 4, RejectionPolicy.ABORT);
        Future<Integer> future = pool.submit(() -> 6 * 7);
        assertEquals(42, future.get(5, TimeUnit.SECONDS));
        shutdownQuietly(pool);
    }

    @Test
    void tasksRunInParallelAcrossWorkers() throws Exception {
        ThreadPool pool = new ThreadPool(4, 8, RejectionPolicy.ABORT);
        CountDownLatch allStarted = new CountDownLatch(4);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            futures.add(pool.submit(() -> {
                allStarted.countDown();
                assertTrue(allStarted.await(5, TimeUnit.SECONDS),
                        "workers did not overlap: pool is serialising tasks");
                return null;
            }));
        }
        for (Future<?> f : futures) {
            f.get(5, TimeUnit.SECONDS);
        }
        shutdownQuietly(pool);
    }

    @Test
    void throwingTaskSurfacesAsExecutionExceptionAndWorkerSurvives() throws Exception {
        ThreadPool pool = new ThreadPool(1, 4, RejectionPolicy.ABORT);
        Future<?> bad = pool.submit(() -> {
            throw new IllegalStateException("boom");
        });
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> bad.get(5, TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof IllegalStateException);
        // Same single worker must still accept the next task.
        assertEquals("ok", pool.submit(() -> "ok").get(5, TimeUnit.SECONDS));
        shutdownQuietly(pool);
    }

    @Test
    void abortPolicyRejectsWhenQueueIsFull() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ThreadPool pool = new ThreadPool(1, 1, RejectionPolicy.ABORT);
        pool.submit(() -> {
            started.countDown();
            release.await();
            return null;
        });
        assertTrue(started.await(5, TimeUnit.SECONDS), "worker never picked up the task");
        pool.submit(() -> null); // fills the single queue slot
        assertThrows(RejectedExecutionException.class, () -> pool.submit(() -> null));
        release.countDown();
        shutdownQuietly(pool);
    }

    @Test
    void callerRunsPolicyExecutesOnSubmittingThread() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ThreadPool pool = new ThreadPool(1, 1, RejectionPolicy.CALLER_RUNS);
        pool.submit(() -> {
            started.countDown();
            release.await();
            return null;
        });
        assertTrue(started.await(5, TimeUnit.SECONDS), "worker never picked up the task");
        pool.submit(() -> null); // fills the queue
        AtomicInteger ranOn = new AtomicInteger();
        String callerThread = Thread.currentThread().getName();
        // This submission overflows, so it must run right here, synchronously.
        pool.submit(() -> {
            if (Thread.currentThread().getName().equals(callerThread)) {
                ranOn.incrementAndGet();
            }
            return null;
        });
        assertEquals(1, ranOn.get(), "overflow task did not run on the caller thread");
        release.countDown();
        shutdownQuietly(pool);
    }

    @Test
    void discardPolicyDropsOverflowAndFailsItsFuture() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ThreadPool pool = new ThreadPool(1, 1, RejectionPolicy.DISCARD);
        pool.submit(() -> {
            started.countDown();
            release.await();
            return null;
        });
        assertTrue(started.await(5, TimeUnit.SECONDS), "worker never picked up the task");
        pool.submit(() -> null); // fills the queue
        Future<?> dropped = pool.submit(() -> null); // discarded
        assertTrue(dropped.isDone(), "discarded task's future must complete, not hang");
        assertThrows(CancellationException.class, () -> dropped.get(5, TimeUnit.SECONDS));
        release.countDown();
        shutdownQuietly(pool);
    }

    @Test
    void discardOldestPolicyDropsOldestQueuedTask() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ThreadPool pool = new ThreadPool(1, 1, RejectionPolicy.DISCARD_OLDEST);
        List<String> order = Collections.synchronizedList(new ArrayList<>());
        pool.submit(() -> {
            started.countDown();
            release.await();
            order.add("blocker");
            return null;
        });
        assertTrue(started.await(5, TimeUnit.SECONDS), "worker never picked up the task");
        Future<?> oldest = pool.submit(() -> {
            order.add("oldest");
            return null;
        });
        Future<?> newest = pool.submit(() -> {
            order.add("newest");
            return null;
        });
        release.countDown();
        assertThrows(CancellationException.class, () -> oldest.get(5, TimeUnit.SECONDS));
        newest.get(5, TimeUnit.SECONDS);
        shutdownQuietly(pool);
        assertFalse(order.contains("oldest"));
        assertTrue(order.contains("newest"));
    }

    @Test
    void submitAfterShutdownIsRejected() throws Exception {
        ThreadPool pool = new ThreadPool(1, 4, RejectionPolicy.ABORT);
        pool.shutdown();
        assertThrows(RejectedExecutionException.class, () -> pool.submit(() -> null));
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    void shutdownDrainsQueuedTasksBeforeTerminating() throws Exception {
        ThreadPool pool = new ThreadPool(1, 8, RejectionPolicy.ABORT);
        AtomicInteger ran = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            pool.submit(() -> {
                ran.incrementAndGet();
                return null;
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(5, ran.get());
        assertEquals(5, pool.getCompletedTaskCount());
    }

    @Test
    void getTimesOutWhileTaskIsStillRunning() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        ThreadPool pool = new ThreadPool(1, 4, RejectionPolicy.ABORT);
        Future<?> slow = pool.submit(() -> {
            release.await();
            return null;
        });
        assertThrows(TimeoutException.class,
                () -> slow.get(100, TimeUnit.MILLISECONDS));
        release.countDown();
        slow.get(5, TimeUnit.SECONDS);
        shutdownQuietly(pool);
    }

    @Test
    void countersTrackCompletedAndQueuedWork() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ThreadPool pool = new ThreadPool(1, 4, RejectionPolicy.ABORT);
        pool.submit(() -> {
            started.countDown();
            release.await();
            return null;
        });
        assertTrue(started.await(5, TimeUnit.SECONDS), "worker never picked up the task");
        pool.submit(() -> null);
        pool.submit(() -> null);
        // One running, two queued (queue capacity 4, no overflow).
        assertEquals(2, pool.getQueueSize());
        release.countDown();
        shutdownQuietly(pool);
        assertEquals(3, pool.getCompletedTaskCount());
        assertEquals(0, pool.getQueueSize());
    }

    @Test
    void constructorRejectsBadArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> new ThreadPool(0, 4, RejectionPolicy.ABORT));
        assertThrows(IllegalArgumentException.class,
                () -> new ThreadPool(2, 0, RejectionPolicy.ABORT));
        assertThrows(IllegalArgumentException.class,
                () -> new ThreadPool(2, 4, null));
    }
}
