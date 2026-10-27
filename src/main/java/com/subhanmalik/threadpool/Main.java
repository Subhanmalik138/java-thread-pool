package com.subhanmalik.threadpool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Small demo: submit 20 squaring tasks to a 4-thread pool with a queue of 8
 * and print results as they complete, then show the pool's counters.
 *
 * <p>Usage: {@code java -jar thread-pool.jar [threads] [queueCapacity]}
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        int threads = args.length > 0 ? Integer.parseInt(args[0]) : 4;
        int queueCapacity = args.length > 1 ? Integer.parseInt(args[1]) : 8;

        ThreadPool pool = new ThreadPool(threads, queueCapacity, RejectionPolicy.CALLER_RUNS);
        System.out.printf("pool: %d workers, queue capacity %d, policy CALLER_RUNS%n",
                threads, queueCapacity);

        List<Future<Integer>> futures = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            int n = i;
            futures.add(pool.submit(() -> {
                Thread.sleep(50); // pretend this is real work
                return n * n;
            }));
        }

        for (int i = 0; i < futures.size(); i++) {
            System.out.printf("task %2d -> %4d%n", i + 1, futures.get(i).get());
        }

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
        System.out.printf("terminated=%s completed=%d active=%d queued=%d%n",
                pool.isTerminated(), pool.getCompletedTaskCount(),
                pool.getActiveCount(), pool.getQueueSize());
    }
}
