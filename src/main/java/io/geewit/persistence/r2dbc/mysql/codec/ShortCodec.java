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
import io.geewit.persistence.r2dbc.mysql.codec.ByteCodec.ByteMySqlParameter;
import io.geewit.persistence.r2dbc.mysql.constant.MySqlType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

/**
 * Codec for {@code short}.
 */
final class ShortCodec extends AbstractPrimitiveCodec<Short> {

    static final ShortCodec INSTANCE = new ShortCodec();

    private ShortCodec() {
        super(Short.TYPE, Short.class);
    }

    @Override
    public Short decode(ByteBuf value,
                        MySqlReadableMetadata metadata,
                        Class<?> target,
                        boolean binary,
                        CodecContext context) {
        return (short) IntegerCodec.decodeInt(value, binary, metadata.getType());
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Short;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        short v = (Short) value;

        if ((byte) v == v) {
            return new ByteMySqlParameter((byte) v);
        }

        return new ShortMySqlParameter(v);
    }

    @Override
    public boolean doCanDecode(MySqlReadableMetadata metadata) {
        return metadata.getType().isNumeric();
    }

    static final class ShortMySqlParameter extends AbstractMySqlParameter {

        private final short value;

        ShortMySqlParameter(short value) {
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> allocator.buffer(Short.BYTES).writeShortLE(value));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeInt(value));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.SMALLINT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof ShortMySqlParameter that) {
                return value == that.value;
            }
            return false;

        }

        @Override
        public int hashCode() {
            return value;
        }

        @Override
        public String toString() {
            return Short.toString(value);
        }
    }
}
