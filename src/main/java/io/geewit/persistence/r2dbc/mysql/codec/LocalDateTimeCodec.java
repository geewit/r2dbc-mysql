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
import java.time.LocalTime;
import java.time.chrono.ChronoLocalDateTime;

/**
 * Codec for {@link LocalDateTime} and {@link ChronoLocalDateTime}.
 * <p>
 * For now, supports only A.D. calendar in {@link ChronoLocalDateTime}.
 */
final class LocalDateTimeCodec implements ParameterizedCodec<LocalDateTime> {

    static final LocalDateTimeCodec INSTANCE = new LocalDateTimeCodec();

    static final LocalDateTime ROUND = LocalDateTime.of(LocalDateCodec.ROUND, LocalTime.MIN);

    private LocalDateTimeCodec() {
    }

    @Override
    public Class<? extends LocalDateTime> getMainClass() {
        return LocalDateTime.class;
    }

    @Override
    public LocalDateTime decode(ByteBuf value,
                                MySqlReadableMetadata metadata,
                                Class<?> target,
                                boolean binary,
                                CodecContext context) {
        return decodeOrigin(value, binary, context);
    }

    @Override
    public ChronoLocalDateTime<LocalDate> decode(ByteBuf value, MySqlReadableMetadata metadata,
                                                 ParameterizedType target, boolean binary, CodecContext context) {
        return decodeOrigin(value, binary, context);
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new LocalDateTimeMySqlParameter((LocalDateTime) value);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof LocalDateTime;
    }

    @Override
    public boolean canDecode(MySqlReadableMetadata metadata, ParameterizedType target) {
        return DateTimes.canDecodeChronology(metadata.getType(), target, ChronoLocalDateTime.class);
    }

    @Override
    public boolean canDecode(MySqlReadableMetadata metadata, Class<?> target) {
        return DateTimes.canDecodeDateTime(metadata.getType(), target, LocalDateTime.class);
    }

    static LocalDateTime decodeOrigin(ByteBuf value,
                                      boolean binary,
                                      CodecContext context) {
        LocalDateTime dateTime = binary ? decodeBinary(value) : decodeText(value);
        return dateTime == null ? DateTimes.zeroDate(context.getZeroDateOption(), binary, ROUND) : dateTime;
    }

    static ByteBuf encodeBinary(ByteBufAllocator alloc, LocalDateTime value) {
        LocalTime time = value.toLocalTime();

        if (LocalTime.MIDNIGHT.equals(time)) {
            return LocalDateCodec.encodeDate(alloc, value.toLocalDate());
        }

        int nano = time.getNano();
        int bytes = nano > 0 ? DateTimes.MICRO_DATETIME_SIZE : DateTimes.DATETIME_SIZE;
        ByteBuf buf = alloc.buffer(Byte.BYTES + bytes);

        try {
            buf.writeByte(bytes)
                    .writeShortLE(value.getYear())
                    .writeByte(value.getMonthValue())
                    .writeByte(value.getDayOfMonth())
                    .writeByte(time.getHour())
                    .writeByte(time.getMinute())
                    .writeByte(time.getSecond());

            if (nano > 0) {
                return buf.writeIntLE(nano / DateTimes.NANOS_OF_MICRO);
            }

            return buf;
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }

    static void encodeText(ParameterWriter writer, LocalDateTime value) {
        LocalDateCodec.encodeDate(writer, value.toLocalDate());
        writer.append(' ');
        LocalTimeCodec.encodeTime(writer, value.toLocalTime());
    }

    private static LocalDateTime decodeText(ByteBuf buf) {
        LocalDate date = LocalDateCodec.readDateText(buf);

        if (date == null) {
            return null;
        }

        LocalTime time = LocalTimeCodec.readTimeText(buf);
        return LocalDateTime.of(date, time);
    }

    private static LocalDateTime decodeBinary(ByteBuf buf) {
        int bytes = buf.readableBytes();
        LocalDate date = LocalDateCodec.readDateBinary(buf, bytes);

        if (date == null) {
            return null;
        }

        if (bytes < DateTimes.DATETIME_SIZE) {
            return LocalDateTime.of(date, LocalTime.MIDNIGHT);
        }

        byte hour = buf.readByte();
        byte minute = buf.readByte();
        byte second = buf.readByte();

        if (bytes < DateTimes.MICRO_DATETIME_SIZE) {
            return LocalDateTime.of(date, LocalTime.of(hour, minute, second));
        }

        int nano = (int) (buf.readUnsignedIntLE() * DateTimes.NANOS_OF_MICRO);
        return LocalDateTime.of(date, LocalTime.of(hour, minute, second, nano));
    }

    private static final class LocalDateTimeMySqlParameter extends AbstractMySqlParameter {

        private final LocalDateTime value;

        private LocalDateTimeMySqlParameter(LocalDateTime value) {
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> encodeBinary(allocator, value));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> encodeText(writer, value));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.DATETIME;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof LocalDateTimeMySqlParameter that) {
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
