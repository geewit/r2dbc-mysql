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

package io.geewit.persistence.r2dbc.mysql.codec.lob;

import io.geewit.persistence.r2dbc.mysql.internal.util.NettyBufferUtils;
import io.geewit.persistence.r2dbc.mysql.internal.util.OperatorUtils;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Base class considers multiple {@link ByteBuf}s and drains/disposes {@link ByteBuf}s on cancellation.
 *
 * @param <T> the emit data type, it should be {@code ByteBuffer} or {@link CharSequence}.
 */
abstract class MultiLob<T> {

    private static final Consumer<ByteBuf> RELEASE = ByteBuf::release;

    private final AtomicReference<List<ByteBuf>> buffers;

    MultiLob(List<ByteBuf> buffers) {
        this.buffers = new AtomicReference<>(buffers);
    }

    public final Flux<T> stream() {
        return Flux.defer(() -> {
            List<ByteBuf> buffers = this.buffers.getAndSet(null);

            if (buffers == null) {
                return Flux.error(new IllegalStateException("Source has been released"));
            }

            return OperatorUtils.discardOnCancel(Flux.fromIterable(buffers))
                .doOnDiscard(ByteBuf.class, RELEASE)
                .map(this::consume);
        });
    }

    public final Mono<Void> discard() {
        return Mono.fromRunnable(() -> {
            List<ByteBuf> buffers = this.buffers.getAndSet(null);

            if (buffers != null) {
                NettyBufferUtils.releaseAll(buffers);
            }
        });
    }

    protected abstract T convert(ByteBuf buf);

    private T consume(ByteBuf buf) {
        try {
            return convert(buf);
        } finally {
            buf.release();
        }
    }
}
