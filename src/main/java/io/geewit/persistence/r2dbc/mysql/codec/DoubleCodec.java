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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

/**
 * Codec for {@code double}.
 */
final class DoubleCodec extends AbstractPrimitiveCodec<Double> {

    static final DoubleCodec INSTANCE = new DoubleCodec();

    private DoubleCodec() {
        super(Double.TYPE, Double.class);
    }

    @Override
    public Double decode(ByteBuf value,
                         MySqlReadableMetadata metadata,
                         Class<?> target,
                         boolean binary,
                         CodecContext context) {
        MySqlType type = metadata.getType();

        if (binary) {
            return decodeBinary(value, type);
        }

        return switch (metadata.getType()) {
            case FLOAT, DOUBLE, DECIMAL, BIGINT_UNSIGNED ->
                    Double.parseDouble(value.toString(StandardCharsets.US_ASCII));
            default -> (double) CodecUtils.parseLong(value);
        };
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Double;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new DoubleMySqlParameter((Double) value);
    }

    @Override
    public boolean doCanDecode(MySqlReadableMetadata metadata) {
        return metadata.getType().isNumeric();
    }

    private static double decodeBinary(ByteBuf buf, MySqlType type) {
        switch (type) {
            case BIGINT_UNSIGNED:
                long v = buf.readLongLE();

                if (v < 0) {
                    return CodecUtils.unsignedBigInteger(v).doubleValue();
                }

                return v;
            case BIGINT:
                return buf.readLongLE();
            case INT_UNSIGNED:
                return buf.readUnsignedIntLE();
            case INT:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT:
                return buf.readIntLE();
            case SMALLINT_UNSIGNED:
                return buf.readUnsignedShortLE();
            case SMALLINT:
            case YEAR:
                return buf.readShortLE();
            case TINYINT_UNSIGNED:
                return buf.readUnsignedByte();
            case TINYINT:
                return buf.readByte();
            case DECIMAL:
                return Double.parseDouble(buf.toString(StandardCharsets.US_ASCII));
            case FLOAT:
                return buf.readFloatLE();
            case DOUBLE:
                return buf.readDoubleLE();
        }

        throw new IllegalStateException("Cannot decode type " + type + " as a Double");
    }

    private static final class DoubleMySqlParameter extends AbstractMySqlParameter {

        private final double value;

        private DoubleMySqlParameter(double value) {
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> {
                ByteBuf buf = allocator.buffer(Double.BYTES);
                try {
                    return buf.writeDoubleLE(value);
                } catch (Throwable e) {
                    buf.release();
                    throw e;
                }
            });
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeDouble(value));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.DOUBLE;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof DoubleMySqlParameter that) {
                return Double.compare(that.value, value) == 0;
            }
            return false;

        }

        @Override
        public int hashCode() {
            return Double.hashCode(value);
        }

        @Override
        public String toString() {
            return Double.toString(value);
        }
    }
}
