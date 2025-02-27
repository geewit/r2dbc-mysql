/*
 * Copyright 2023 geewit.io projects
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.geewit.persistence.r2dbc.mysql.client;

import reactor.util.concurrent.Queues;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;

abstract class LeftPadding {

    long p0, p1, p2, p3, p4, p5, p6, p7;
}

abstract class ActiveStatus extends LeftPadding {

    static final int DISPOSE = -1;

    static final int IDLE = 0;

    static final int ACTIVE = 1;

    static final AtomicIntegerFieldUpdater<ActiveStatus> STATUS_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(ActiveStatus.class, "status");

    protected volatile int status = IDLE; // p8 first part

    int p8; // p8 second part

    long p9, pa, pb, pc, pd, pe, pf;
}

/**
 * Request queue to collect incoming exchange requests.
 * <p>
 * Submission conditionally queues requests if an ongoing exchange was active by the time of subscription.
 * Drains queued commands on exchange completion if there are queued commands or disable active flag.
 * <p>
 * It should discard all tasks when it is discarded by connection.
 */
final class RequestQueue extends ActiveStatus implements Runnable {

    private final Queue<RequestTask<?>> queue = Queues.<RequestTask<?>>small().get();

    private final ReentrantLock lock = new ReentrantLock();

    private volatile RuntimeException disposed;

    /**
     * Current exchange completed, refresh to next exchange or set to inactive.
     */
    @Override
    public void run() {
        for (;;) {
            RequestTask<?> task = queue.poll();
            final int status = this.status;

            if (task == null) {
                // Queue was empty, set it to idle if it is not disposed.
                if (status != ACTIVE || STATUS_UPDATER.compareAndSet(this, ACTIVE, IDLE) && queue.isEmpty()) {
                    return;
                }
            } else if (status == DISPOSE) {
                // Cancel and no need clear queue because it should be cleared by other one.
                task.cancel(requireDisposed());
                return;
            } else {
                task.run();
                // The execution of a canceled task would result in a stall of the request queue.
                // refer: https://github.com/geewit-io/r2dbc-mysql/issues/114
                if (!task.isCancelled()) {
                    return;
            }
        }
    }
}

    /**
     * Submit an exchange task. If the queue is inactive, it will execute directly instead of queuing.
     * Otherwise it will be queuing.
     *
     * @param task the exchange task includes request messages sending and response messages processor.
     * @param <T> the type argument of {@link RequestTask}.
     */
    <T> void submit(RequestTask<T> task) {
        int status = this.status;

        if (status == DISPOSE) {
            // Cancel and no need clear queue because it should be cleared by other one.
            task.cancel(requireDisposed());
            return;
        }

        // Prev task may be completing before queue offer, so queue may be idle now.
        if (!queue.offer(task)) {
            task.cancel(new IllegalStateException("Request queue is full"));
            return;
        }

        if (STATUS_UPDATER.compareAndSet(this, IDLE, ACTIVE)) {
            // Try execute if queue is idle.
            run();
        } else {
            // Check dispose again after offer success and not idle.
            status = this.status;

            if (status == DISPOSE) {
                // Disposed, should clear queue because an element has just been offered to the queue.
                cancelAll(requireDisposed());
            }
        }
    }

    /**
     * Keep padding, maybe useful, maybe useless, whatever we should make sure padding would not be reduced by
     * compiler.
     *
     * @param v any value, because it is not matter
     * @return {@code v} self which type is {@code long}
     */
    long keeping(int v) {
        return p0 = p1 = p2 = p3 = p4 = p5 = p6 = p7 = p9 = pa = pb = pc = pd = pe = pf = p8 = v;
    }

    void dispose() {
        STATUS_UPDATER.set(this, DISPOSE);
        cancelAll(requireDisposed());
    }

    private RuntimeException requireDisposed() {
        RuntimeException disposed = this.disposed;

        if (disposed == null) {
            lock.lock();
            try {
                disposed = this.disposed;

                if (disposed == null) {
                    this.disposed = disposed = new IllegalStateException("Request queue was disposed");
                }

                return disposed;
            } finally {
                lock.unlock();
            }
        }

        return disposed;
    }

    private void cancelAll(RuntimeException e) {
        RequestTask<?> task;

        while ((task = queue.poll()) != null) {
            task.cancel(e);
        }
    }
}
