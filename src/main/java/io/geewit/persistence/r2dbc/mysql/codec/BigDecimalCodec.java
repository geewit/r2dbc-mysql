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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * Codec for {@link BigDecimal}.
 */
final class BigDecimalCodec extends AbstractClassedCodec<BigDecimal> {

    static final BigDecimalCodec INSTANCE = new BigDecimalCodec();

    private BigDecimalCodec() {
        super(BigDecimal.class);
    }

    @Override
    public BigDecimal decode(ByteBuf value, MySqlReadableMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        MySqlType type = metadata.getType();

        if (binary) {
            return decodeBinary(value, type);
        }

        switch (type) {
            case FLOAT:
                return BigDecimal.valueOf(Float.parseFloat(value.toString(StandardCharsets.US_ASCII)));
            case DOUBLE:
                return BigDecimal.valueOf(Double.parseDouble(value.toString(StandardCharsets.US_ASCII)));
            case DECIMAL:
                return parseBigDecimal(value);
            case BIGINT_UNSIGNED:
                if (value.getByte(value.readerIndex()) == '+') {
                    value.skipBytes(1);
                }

                String num = value.toString(StandardCharsets.US_ASCII);

                if (CodecUtils.isGreaterThanLongMax(num)) {
                    return new BigDecimal(num);
                }

                return BigDecimal.valueOf(CodecUtils.parsePositive(num));
            default:
                return BigDecimal.valueOf(CodecUtils.parseLong(value));
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof BigDecimal;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new BigDecimalMySqlParameter((BigDecimal) value);
    }

    @Override
    protected boolean doCanDecode(MySqlReadableMetadata metadata) {
        return metadata.getType().isNumeric();
    }

    private static BigDecimal decodeBinary(ByteBuf buf, MySqlType type) {
        switch (type) {
            case BIGINT_UNSIGNED:
                long v = buf.readLongLE();

                if (v < 0) {
                    return new BigDecimal(CodecUtils.unsignedBigInteger(v));
                }

                return BigDecimal.valueOf(v);
            case BIGINT:
                return BigDecimal.valueOf(buf.readLongLE());
            case INT_UNSIGNED:
                return BigDecimal.valueOf(buf.readUnsignedIntLE());
            case INT:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT:
                return BigDecimal.valueOf(buf.readIntLE());
            case SMALLINT_UNSIGNED:
                return BigDecimal.valueOf(buf.readUnsignedShortLE());
            case SMALLINT:
            case YEAR:
                return BigDecimal.valueOf(buf.readShortLE());
            case TINYINT_UNSIGNED:
                return BigDecimal.valueOf(buf.readUnsignedByte());
            case TINYINT:
                return BigDecimal.valueOf(buf.readByte());
            case DECIMAL:
                return parseBigDecimal(buf);
            case FLOAT:
                return BigDecimal.valueOf(buf.readFloatLE());
            case DOUBLE:
                return BigDecimal.valueOf(buf.readDoubleLE());
        }

        throw new IllegalStateException("Cannot decode type " + type + " as a BigDecimal");
    }

    private static BigDecimal parseBigDecimal(ByteBuf buf) {
        return new BigDecimal(buf.toString(StandardCharsets.US_ASCII));
    }

    private static final class BigDecimalMySqlParameter extends AbstractMySqlParameter {

        private final BigDecimal value;

        private BigDecimalMySqlParameter(BigDecimal value) {
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> CodecUtils.encodeAscii(allocator, value.toString()));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeBigDecimal(value));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.DECIMAL;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof BigDecimalMySqlParameter that) {
                return value.equals(that.value);
            }
            return false;

        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }
}
