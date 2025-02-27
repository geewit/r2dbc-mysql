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
import io.netty.buffer.ByteBufUtil;
import reactor.core.publisher.Mono;

import java.util.Arrays;

import static io.geewit.persistence.r2dbc.mysql.internal.util.InternalArrays.EMPTY_BYTES;

/**
 * Codec for {@code byte[]}.
 */
final class ByteArrayCodec extends AbstractClassedCodec<byte[]> {

    static final ByteArrayCodec INSTANCE = new ByteArrayCodec();

    private ByteArrayCodec() {
        super(byte[].class);
    }

    @Override
    public byte[] decode(ByteBuf value, MySqlReadableMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        if (!value.isReadable()) {
            return EMPTY_BYTES;
        }

        return ByteBufUtil.getBytes(value);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof byte[];
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new ByteArrayMySqlParameter((byte[]) value);
    }

    @Override
    protected boolean doCanDecode(MySqlReadableMetadata metadata) {
        return metadata.getType().isBinary();
    }

    static ByteBuf encodeBytes(ByteBufAllocator alloc, byte[] value) {
        int size = value.length;

        if (size == 0) {
            // It is zero of var int, not terminal.
            return alloc.buffer(Byte.BYTES).writeByte(0);
        }

        ByteBuf buf = alloc.buffer(VarIntUtils.varIntBytes(size) + size);

        try {
            VarIntUtils.writeVarInt(buf, size);
            return buf.writeBytes(value);
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }

    private static final class ByteArrayMySqlParameter extends AbstractMySqlParameter {

        private final byte[] value;

        private ByteArrayMySqlParameter(byte[] value) {
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> encodeBytes(allocator, value));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeHex(value));
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
            if (o instanceof ByteArrayMySqlParameter that) {
                return Arrays.equals(value, that.value);
            }
            return false;

        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }

        @Override
        public String toString() {
            return Arrays.toString(value);
        }
    }
}
