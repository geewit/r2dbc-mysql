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

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * Codec for {@link OffsetDateTime}.
 */
final class OffsetDateTimeCodec implements Codec<OffsetDateTime> {

    static final OffsetDateTimeCodec INSTANCE = new OffsetDateTimeCodec();

    private OffsetDateTimeCodec() {
    }

    @Override
    public Class<? extends OffsetDateTime> getMainClass() {
        return OffsetDateTime.class;
    }

    @Override
    public OffsetDateTime decode(ByteBuf value,
                                 MySqlReadableMetadata metadata,
                                 Class<?> target,
                                 boolean binary,
                                 CodecContext context) {
        LocalDateTime origin = LocalDateTimeCodec.decodeOrigin(value, binary, context);

        if (origin == null) {
            return null;
        }

        ZoneId zone = context.isPreserveInstants() ? context.getTimeZone() : ZoneId.systemDefault();

        return OffsetDateTime.of(origin, zone instanceof ZoneOffset zoneOffset ? zoneOffset : zone.getRules()
                .getOffset(origin));
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new OffsetDateTimeMySqlParameter((OffsetDateTime) value, context);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof OffsetDateTime;
    }

    @Override
    public boolean canDecode(MySqlReadableMetadata metadata, Class<?> target) {
        return DateTimes.canDecodeDateTime(metadata.getType(), target, OffsetDateTime.class);
    }

    private static final class OffsetDateTimeMySqlParameter extends AbstractMySqlParameter {

        private final OffsetDateTime value;

        private final CodecContext context;

        private OffsetDateTimeMySqlParameter(OffsetDateTime value, CodecContext context) {
            this.value = value;
            this.context = context;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> LocalDateTimeCodec.encodeBinary(allocator, serverValue()));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> LocalDateTimeCodec.encodeText(writer, serverValue()));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.TIMESTAMP;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OffsetDateTimeMySqlParameter that = (OffsetDateTimeMySqlParameter) o;
            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        private LocalDateTime serverValue() {
            ZoneId zone = context.isPreserveInstants() ? context.getTimeZone() :
                    ZoneId.systemDefault().normalized();

            return zone instanceof ZoneOffset zoneOffset ?
                    value.withOffsetSameInstant(zoneOffset).toLocalDateTime() :
                    value.toZonedDateTime().withZoneSameInstant(zone).toLocalDateTime();
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }
}
