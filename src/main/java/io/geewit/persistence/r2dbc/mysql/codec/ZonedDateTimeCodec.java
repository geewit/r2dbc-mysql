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

import java.lang.reflect.ParameterizedType;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.chrono.ChronoZonedDateTime;

/**
 * Codec for {@link ZonedDateTime} and {@link ChronoZonedDateTime}.
 * <p>
 * For now, supports only A.D. calendar in {@link ChronoZonedDateTime}.
 */
final class ZonedDateTimeCodec implements ParameterizedCodec<ZonedDateTime> {

    static final ZonedDateTimeCodec INSTANCE = new ZonedDateTimeCodec();

    private ZonedDateTimeCodec() {
    }

    @Override
    public Class<? extends ZonedDateTime> getMainClass() {
        return ZonedDateTime.class;
    }

    @Override
    public ZonedDateTime decode(ByteBuf value,
                                MySqlReadableMetadata metadata,
                                Class<?> target, boolean binary,
                                CodecContext context) {
        return decode0(value, binary, context);
    }

    @Override
    public ChronoZonedDateTime<LocalDate> decode(ByteBuf value,
                                                 MySqlReadableMetadata metadata,
                                                 ParameterizedType target,
                                                 boolean binary,
                                                 CodecContext context) {
        return decode0(value, binary, context);
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new ZonedDateTimeMySqlParameter((ZonedDateTime) value, context);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof ZonedDateTime;
    }

    @Override
    public boolean canDecode(MySqlReadableMetadata metadata, ParameterizedType target) {
        return DateTimes.canDecodeChronology(metadata.getType(), target, ChronoZonedDateTime.class);
    }

    @Override
    public boolean canDecode(MySqlReadableMetadata metadata, Class<?> target) {
        return DateTimes.canDecodeDateTime(metadata.getType(), target, ZonedDateTime.class);
    }

    private static ZonedDateTime decode0(ByteBuf value,
                                         boolean binary,
                                         CodecContext context) {
        LocalDateTime origin = LocalDateTimeCodec.decodeOrigin(value, binary, context);

        if (origin == null) {
            return null;
        }

        return ZonedDateTime.of(origin, context.isPreserveInstants() ? context.getTimeZone() :
                ZoneId.systemDefault());
    }

    private static final class ZonedDateTimeMySqlParameter extends AbstractMySqlParameter {

        private final ZonedDateTime value;

        private final CodecContext context;

        private ZonedDateTimeMySqlParameter(ZonedDateTime value, CodecContext context) {
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
            if (o instanceof ZonedDateTimeMySqlParameter that) {
                return value.equals(that.value);
            }
            return false;

        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        private LocalDateTime serverValue() {
            ZoneId zoneId = context.isPreserveInstants() ? context.getTimeZone() :
                    ZoneId.systemDefault().normalized();

            return value.withZoneSameInstant(zoneId)
                    .toLocalDateTime();
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }
}
