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
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of {@link Flux}{@code <}{@link ByteBuf}{@code >} that considers cumulate buffers as
 * envelopes of the MySQL socket protocol.
 */
final class FluxEnvelope extends FluxOperator<ByteBuf, ByteBuf> {

    private final ByteBufAllocator alloc;

    private final int size;

    private final AtomicInteger sequenceId;

    private final boolean cumulate;

    FluxEnvelope(Flux<? extends ByteBuf> source, ByteBufAllocator alloc, int size, AtomicInteger sequenceId,
        boolean cumulate) {
        super(source);

        this.alloc = alloc;
        this.size = size;
        this.sequenceId = sequenceId;
        this.cumulate = cumulate;
    }

    @Override
    public void subscribe(CoreSubscriber<? super ByteBuf> actual) {
        if (cumulate) {
            this.source.subscribe(new CumulateEnvelopeSubscriber(actual, alloc, size, sequenceId));
        } else {
            this.source.subscribe(new DirectEnvelopeSubscriber(actual, alloc, sequenceId));
        }
    }
}

final class DirectEnvelopeSubscriber implements CoreSubscriber<ByteBuf>, Scannable, Subscription {

    private final CoreSubscriber<? super ByteBuf> actual;

    private final ByteBufAllocator alloc;

    private final AtomicInteger sequenceId;

    private boolean done;

    private Subscription s;

    DirectEnvelopeSubscriber(CoreSubscriber<? super ByteBuf> actual, ByteBufAllocator alloc,
        AtomicInteger sequenceId) {
        this.actual = actual;
        this.alloc = alloc;
        this.sequenceId = sequenceId;
    }

    @Override
    public void onSubscribe(Subscription s) {
        if (Operators.validate(this.s, s)) {
            this.s = s;
            this.actual.onSubscribe(this);
        }
    }

    @Override
    public void onNext(ByteBuf buf) {
        if (done) {
            // Do not release the buffer, it should be handled by OperatorUtils.discardOnCancel() or Context.
            Operators.onNextDropped(buf, actual.currentContext());
            return;
        }

        try {
            ByteBuf header = this.alloc.ioBuffer(Packets.NORMAL_HEADER_SIZE)
                .writeMediumLE(buf.readableBytes())
                .writeByte(this.sequenceId.getAndIncrement());

            this.actual.onNext(header);
            this.actual.onNext(buf);
        } catch (Throwable e) {
            Throwable t = Operators.onNextError(buf, e, this.actual.currentContext(), this.s);

            if (t == null) {
                s.request(1);
            } else {
                onError(t);
            }
        }
    }

    @Override
    public void onError(Throwable t) {
        if (this.done) {
            Operators.onErrorDropped(t, this.actual.currentContext());
            return;
        }

        this.done = true;
        this.actual.onError(t);
    }

    @Override
    public void onComplete() {
        if (this.done) {
            return;
        }

        this.done = true;
        this.actual.onComplete();
    }

    @Override
    public void request(long n) {
        this.s.request(n);
    }

    @Override
    public void cancel() {
        this.s.cancel();
    }

    @Override
    public Context currentContext() {
        return this.actual.currentContext();
    }

    @Override
    public Object scanUnsafe(Attr key) {
        if (key == Attr.PARENT) {
            return this.s;
        } else if (key == Attr.ACTUAL) {
            return this.actual;
        } else if (key == Attr.TERMINATED) {
            return this.done;
        } else {
            return null;
        }
    }
}

final class CumulateEnvelopeSubscriber implements CoreSubscriber<ByteBuf>, Scannable, Subscription {

    private final CoreSubscriber<? super ByteBuf> actual;

    private final ByteBufAllocator alloc;

    private final int size;

    private final AtomicInteger sequenceId;

    private boolean done;

    private Subscription s;

    private ByteBuf cumulated;

    CumulateEnvelopeSubscriber(CoreSubscriber<? super ByteBuf> actual, ByteBufAllocator alloc, int size,
        AtomicInteger sequenceId) {
        this.actual = actual;
        this.alloc = alloc;
        this.size = size;
        this.sequenceId = sequenceId;
    }

    @Override
    public void onSubscribe(Subscription s) {
        if (Operators.validate(this.s, s)) {
            this.s = s;
            this.actual.onSubscribe(this);
        }
    }

    @Override
    public void onNext(ByteBuf buf) {
        if (done) {
            // Do not release the buffer, it should be handled by OperatorUtils.discardOnCancel() or Context.
            Operators.onNextDropped(buf, actual.currentContext());
            return;
        }

        if (!buf.isReadable()) {
            // Ignore empty buffer, useless for cumulated buffers.
            buf.release();
            return;
        }

        try {
            // The buf will be released by cumulate.
            ByteBuf cumulated = this.cumulated = cumulate(this.alloc, this.cumulated, buf);

            while (cumulated.readableBytes() >= this.size) {
                // It will make the cumulated be shared (e.g. refCnt() > 1), that means
                // the reallocation of the cumulated may not be safe, see cumulate(...).
                this.actual.onNext(this.alloc.ioBuffer(Packets.NORMAL_HEADER_SIZE)
                    .writeMediumLE(this.size)
                    .writeByte(this.sequenceId.getAndIncrement()));
                this.actual.onNext(cumulated.readRetainedSlice(this.size));
            }

            if (!cumulated.isReadable()) {
                // Don't need a buffer that is not readable.
                this.cumulated = null;
                cumulated.release();
            }
        } catch (Throwable e) {
            Throwable t = Operators.onNextError(buf, e, this.actual.currentContext(), this.s);

            if (t == null) {
                s.request(1);
            } else {
                onError(t);
            }
        }
    }

    @Override
    public void onError(Throwable t) {
        if (this.done) {
            Operators.onErrorDropped(t, this.actual.currentContext());
            return;
        }

        this.done = true;

        ByteBuf cumulated = this.cumulated;
        if (cumulated != null) {
            cumulated.release();
        }

        this.actual.onError(t);
    }

    @Override
    public void onComplete() {
        if (this.done) {
            return;
        }
        this.done = true;

        ByteBuf cumulated = this.cumulated;
        this.cumulated = null;

        // The protocol need least one envelope, and the last must smaller than maximum size of envelopes.
        // - If there has no previous envelope, then the cumulated is null, should produce an empty
        //   envelope header.
        // - If previous envelope is a max-size envelope, then the cumulated is null, should produce an
        //   empty envelope header.
        int size = cumulated == null ? 0 : cumulated.readableBytes();
        ByteBuf header = null;

        try {
            header = this.alloc.ioBuffer(Packets.NORMAL_HEADER_SIZE);
            header.writeMediumLE(size).writeByte(this.sequenceId.getAndIncrement());
        } catch (Throwable e) {
            if (cumulated != null) {
                cumulated.release();
            }
            if (header != null) {
                header.release();
            }
            this.actual.onError(e);
            return;
        }

        this.actual.onNext(header);

        if (cumulated != null) {
            if (size > 0) {
                this.actual.onNext(cumulated);
            } else {
                cumulated.release();
            }
        }

        this.actual.onComplete();
    }

    @Override
    public void request(long n) {
        this.s.request(n);
    }

    @Override
    public void cancel() {
        this.s.cancel();
    }

    @Override
    public Context currentContext() {
        return this.actual.currentContext();
    }

    @Override
    public Object scanUnsafe(Attr key) {
        if (key == Attr.PARENT) {
            return this.s;
        } else if (key == Attr.ACTUAL) {
            return this.actual;
        } else if (key == Attr.TERMINATED) {
            return this.done;
        } else {
            return null;
        }
    }

    /**
     * Cumulate buffers with copied or wrote.
     *
     * @param alloc     the allocator for expand cumulated buffer.
     * @param cumulated the previous cumulated buffer.
     * @param buf       the upstream inbounded buffer.
     * @return the cumulated buffer that's combined by current {@code cumulated} and {@code buf}.
     */
    private static ByteBuf cumulate(ByteBufAllocator alloc,
                                    ByteBuf cumulated,
                                    ByteBuf buf) {
        if (cumulated == null) {
            return buf;
        }

        ByteBuf releasing = null;

        try {
            int needBytes = buf.readableBytes();
            if (needBytes > cumulated.maxWritableBytes() ||
                (needBytes > cumulated.maxFastWritableBytes() && cumulated.refCnt() > 1) ||
                cumulated.isReadOnly()) {
                // Merging and replacing the cumulated under the following conditions:
                // - the cumulated cannot be resized to accommodate the following data
                // - the cumulated is assumed to be shared (i.e. refCnt() > 1), so the reallocation may not
                //   be safe, see onNext(...).
                int oldBytes = cumulated.readableBytes();
                int bufBytes = buf.readableBytes();
                int newBytes = oldBytes + bufBytes;
                int newCapacity = alloc.calculateNewCapacity(newBytes, Integer.MAX_VALUE);
                ByteBuf result = releasing = alloc.ioBuffer(newCapacity);

                // Avoid to calling writeBytes(...) with redundancy check and stack depth comparison.
                result.setBytes(0, cumulated, cumulated.readerIndex(), oldBytes)
                    .setBytes(oldBytes, buf, buf.readerIndex(), bufBytes)
                    .writerIndex(newBytes);
                buf.readerIndex(buf.writerIndex());
                // Release the old cumulated If write succeed.
                releasing = cumulated;

                return result;
            } else {
                cumulated.writeBytes(buf, buf.readerIndex(), needBytes);
                buf.readerIndex(buf.writerIndex());

                return cumulated;
            }
        } finally {
            // Must release if the cumulated is not null, because
            // it will not be released outside the method.
            buf.release();

            if (releasing != null) {
                releasing.release();
            }
        }
    }
}
