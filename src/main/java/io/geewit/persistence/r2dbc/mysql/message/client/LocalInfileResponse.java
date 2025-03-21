/*
 * Copyright 2024 geewit.io projects
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

package io.geewit.persistence.r2dbc.mysql.message.client;

import io.geewit.persistence.r2dbc.mysql.ConnectionContext;
import io.geewit.persistence.r2dbc.mysql.internal.util.NettyBufferUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * A message considers as a chunk of a local in-file data.
 */
public final class LocalInfileResponse implements SubsequenceClientMessage {

    private final String path;

    private final SynchronousSink<?> errorSink;

    public LocalInfileResponse(String path, SynchronousSink<?> errorSink) {
        requireNonNull(path, "path must not be null");

        this.path = path;
        this.errorSink = errorSink;
    }

    @Override
    public boolean isCumulative() {
        return false;
    }

    @Override
    public Flux<ByteBuf> encode(ByteBufAllocator allocator, ConnectionContext context) {
        return Flux.defer(() -> {
            int bufferSize = context.getLocalInfileBufferSize();
            AtomicReference<Throwable> error = new AtomicReference<>();

            return Mono.<Path>create(sink -> {
                try {
                    Path safePath = context.getLocalInfilePath();
                    Path file = Paths.get(this.path);

                    if (safePath == null) {
                        String message = "Allowed local file path not set, but attempted to load '" + file +
                            '\'';
                        sink.error(new R2dbcPermissionDeniedException(message));
                    } else if (file.startsWith(safePath)) {
                        sink.success(file);
                    } else {
                        String message = String.format("The file '%s' is not under the safe path '%s'",
                            file, safePath);
                        sink.error(new R2dbcPermissionDeniedException(message));
                    }
                } catch (InvalidPathException e) {
                    sink.error(new R2dbcNonTransientResourceException("Invalid path: " + this.path, e));
                } catch (Throwable e) {
                    sink.error(e);
                }
            }).flatMapMany(p -> NettyBufferUtils.readFile(p, allocator, bufferSize)).onErrorComplete(e -> {
                // Server needs an empty buffer, so emit error to upstream instead of encoding stream.
                error.set(e);
                return true;
            }).concatWith(Flux.just(allocator.buffer(0, 0))).doAfterTerminate(() -> {
                Throwable e = error.getAndSet(null);

                if (e != null) {
                    errorSink.error(e);
                }
            });
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof LocalInfileResponse that) {
            return path.equals(that.path);
        }
        return false;

    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    @Override
    public String toString() {
        return "LocalInfileResponse{path='" + path + "'}";
    }
}
