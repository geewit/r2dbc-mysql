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
import io.geewit.persistence.r2dbc.mysql.codec.IntegerCodec.IntMySqlParameter;
import io.geewit.persistence.r2dbc.mysql.codec.ShortCodec.ShortMySqlParameter;
import io.geewit.persistence.r2dbc.mysql.constant.MySqlType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * Codec for {@code long}.
 */
final class LongCodec extends AbstractPrimitiveCodec<Long> {

    static final LongCodec INSTANCE = new LongCodec();

    private LongCodec() {
        super(Long.TYPE, Long.class);
    }

    @Override
    public Long decode(ByteBuf value, MySqlReadableMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        MySqlType type = metadata.getType();

        if (binary) {
            return decodeBinary(value, type);
        }

        return switch (type) {
            case FLOAT -> (long) Float.parseFloat(value.toString(StandardCharsets.US_ASCII));
            case DOUBLE -> (long) Double.parseDouble(value.toString(StandardCharsets.US_ASCII));
            case DECIMAL -> decimalLong(value);
            default -> CodecUtils.parseLong(value);
        };
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Long;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return encodeLong((Long) value);
    }

    @Override
    public boolean doCanDecode(MySqlReadableMetadata metadata) {
        return metadata.getType().isNumeric();
    }

    static MySqlParameter encodeLong(long v) {
        if ((byte) v == v) {
            return new ByteMySqlParameter((byte) v);
        }
        if ((short) v == v) {
            return new ShortMySqlParameter((short) v);
        }
        if ((int) v == v) {
            return new IntMySqlParameter((int) v);
        }

        return new LongMySqlParameter(v);
    }

    private static long decodeBinary(ByteBuf buf, MySqlType type) {
        return switch (type) {
            case BIGINT_UNSIGNED, BIGINT -> buf.readLongLE();
            case INT_UNSIGNED -> buf.readUnsignedIntLE();
            case INT, MEDIUMINT_UNSIGNED, MEDIUMINT ->
                // Note: MySQL return 32-bits two's complement for 24-bits integer
                    buf.readIntLE();
            case SMALLINT_UNSIGNED -> buf.readUnsignedShortLE();
            case SMALLINT, YEAR -> buf.readShortLE();
            case TINYINT_UNSIGNED -> buf.readUnsignedByte();
            case TINYINT -> buf.readByte();
            case DECIMAL -> decimalLong(buf);
            case FLOAT -> (long) buf.readFloatLE();
            case DOUBLE -> (long) buf.readDoubleLE();
            default -> throw new IllegalStateException("Cannot decode type " + type + " as a Long");
        };

    }

    private static long decimalLong(ByteBuf buf) {
        return new BigDecimal(buf.toString(StandardCharsets.US_ASCII)).longValue();
    }

    private static final class LongMySqlParameter extends AbstractMySqlParameter {

        private final long value;

        private LongMySqlParameter(long value) {
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> allocator.buffer(Long.BYTES).writeLongLE(value));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeLong(value));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.BIGINT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof LongMySqlParameter longValue) {
                return value == longValue.value;
            }
            return false;

        }

        @Override
        public int hashCode() {
            return Long.hashCode(value);
        }

        @Override
        public String toString() {
            return Long.toString(value);
        }
    }
}
