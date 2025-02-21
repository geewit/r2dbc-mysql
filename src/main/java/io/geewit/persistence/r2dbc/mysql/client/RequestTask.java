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

import io.geewit.persistence.r2dbc.mysql.message.client.ClientMessage;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.MonoSink;

/**
 * A task for execute, propagate errors and release resources.
 * <p>
 * If task executed, resources should been released by {@code supplier} instead of task self.
 *
 * @param <T> the task result type.
 */
final class RequestTask<T> {

    private final Disposable disposable;

    private final MonoSink<T> sink;

    private final T supplier;

    private volatile boolean isCancelled;

    private RequestTask(Disposable disposable, MonoSink<T> sink, T supplier) {
        this.disposable = disposable;
        this.sink = sink;
        this.supplier = supplier;
    }

    void run() {
        sink.success(supplier);
    }

    /**
     * Cancel task and release resources.
     *
     * @param e cancelled by which error
     */
    void cancel(Throwable e) {
        cancel0();
        sink.error(e);
    }

    boolean isCancelled() {
        return isCancelled;
    }

    private void cancel0() {
        if (disposable != null) {
            disposable.dispose();
        }
        isCancelled = true;
    }

    static <T> RequestTask<T> wrap(ClientMessage message, MonoSink<T> sink, T supplier) {
        final RequestTask<T> task;
        if (message instanceof Disposable disposable) {
            task = new RequestTask<>(disposable, sink, supplier);
        } else {
            task = new RequestTask<>(null, sink, supplier);

        }
        sink.onCancel(task::cancel0);
        return task;
    }

    static <T> RequestTask<T> wrap(Flux<? extends ClientMessage> messages,
                                   MonoSink<T> sink, T supplier) {
        final RequestTask<T> task =  new RequestTask<>(new DisposableFlux(messages), sink, supplier);
        sink.onCancel(task::cancel0);
        return task;
    }

    static <T> RequestTask<T> wrap(MonoSink<T> sink, T supplier) {
        final RequestTask<T> task = new RequestTask<>(null, sink, supplier);
        sink.onCancel(task::cancel0);
        return task;
    }

    private record DisposableFlux(Flux<? extends ClientMessage> messages) implements Disposable {
        @Override
            public void dispose() {
                Flux.from(messages).subscribe(it -> {
                    if (it instanceof Disposable disposable) {
                        disposable.dispose();
                    }
                });
            }
        }
}
