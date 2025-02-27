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

package io.geewit.persistence.r2dbc.mysql.internal.util;

import io.geewit.persistence.r2dbc.mysql.constant.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;

import java.util.concurrent.atomic.AtomicInteger;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * Operator utility.
 * <p>
 * This is a slightly altered version of R2DBC SQL Server's implementation:
 * <a href="https://github.com/r2dbc/r2dbc-mssql">r2dbc-mssql</a>
 */
public final class OperatorUtils {

    /**
     * Replay signals from {@link Flux the source} until cancellation. Drains the source for data signals if
     * the subscriber cancels the subscription.
     * <p>
     * Draining data is required to complete a particular request/response window and clear the protocol state
     * as client code expects to start a request/response conversation without leaving previous frames on the
     * stack.
     *
     * @param source the source to decorate.
     * @param <T>    The type of values in both source and output sequences.
     * @return decorated {@link Flux}.
     * @throws IllegalArgumentException if {@code source} is {@code null}.
     */
    public static <T> Flux<T> discardOnCancel(Flux<? extends T> source) {
        requireNonNull(source, "source must not be null");

        if (source instanceof Fuseable) {
            return new FluxDiscardOnCancelFuseable<>(source);
        }

        return new FluxDiscardOnCancel<>(source);
    }

    public static Flux<ByteBuf> envelope(Flux<? extends ByteBuf> source, ByteBufAllocator allocator,
        AtomicInteger sequenceId, boolean cumulate) {
        requireNonNull(source, "source must not be null");
        requireNonNull(allocator, "allocator must not be null");
        requireNonNull(sequenceId, "sequenceId must not be null");

        return new FluxEnvelope(source, allocator, Packets.MAX_PAYLOAD_SIZE, sequenceId, cumulate);
    }

    private OperatorUtils() { }
}
