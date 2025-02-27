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

package io.geewit.persistence.r2dbc.mysql.codec;

import io.geewit.persistence.r2dbc.mysql.MySqlParameter;
import io.geewit.persistence.r2dbc.mysql.ParameterWriter;
import io.geewit.persistence.r2dbc.mysql.api.MySqlReadableMetadata;
import io.geewit.persistence.r2dbc.mysql.constant.MySqlType;
import io.geewit.persistence.r2dbc.mysql.internal.util.VarIntUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

import static io.geewit.persistence.r2dbc.mysql.internal.util.InternalArrays.EMPTY_BYTES;

/**
 * Codec for {@link ByteBuffer}.
 */
final class ByteBufferCodec extends AbstractClassedCodec<ByteBuffer> {

    static final ByteBufferCodec INSTANCE = new ByteBufferCodec();

    private ByteBufferCodec() {
        super(ByteBuffer.class);
    }

    @Override
    public ByteBuffer decode(ByteBuf value, MySqlReadableMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        if (!value.isReadable()) {
            return ByteBuffer.wrap(EMPTY_BYTES);
        }

        ByteBuffer result = ByteBuffer.allocate(value.readableBytes());

        value.readBytes(result);
        result.flip();

        return result;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new ByteBufferMySqlParameter((ByteBuffer) value);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof ByteBuffer;
    }

    @Override
    protected boolean doCanDecode(MySqlReadableMetadata metadata) {
        return metadata.getType().isBinary();
    }

    private static final class ByteBufferMySqlParameter extends AbstractMySqlParameter {

        private final ByteBuffer buffer;

        private ByteBufferMySqlParameter(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> {
                if (!buffer.hasRemaining()) {
                    // It is zero of var int, not terminal.
                    return allocator.buffer(Byte.BYTES).writeByte(0);
                }

                int size = buffer.remaining();
                ByteBuf buf = allocator.buffer(VarIntUtils.varIntBytes(size) + size);

                try {
                    VarIntUtils.writeVarInt(buf, size);
                    return buf.writeBytes(buffer);
                } catch (Throwable e) {
                    buf.release();
                    throw e;
                }
            });
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeHex(buffer));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.VARBINARY;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof ByteBufferMySqlParameter that) {
                return buffer.equals(that.buffer);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return buffer.hashCode();
        }

        @Override
        public String toString() {
            return buffer.toString();
        }
    }
}
