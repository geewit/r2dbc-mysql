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

import java.time.LocalTime;

import static io.geewit.persistence.r2dbc.mysql.codec.DateTimes.*;

/**
 * Codec for {@link LocalTime}.
 */
final class LocalTimeCodec extends AbstractClassedCodec<LocalTime> {

    static final LocalTimeCodec INSTANCE = new LocalTimeCodec();

    private static final long NANOS_OF_DAY = ((long) SECONDS_OF_DAY) * NANOS_OF_SECOND;

    private static final long NANOS_OF_HOUR = ((long) SECONDS_OF_HOUR) * NANOS_OF_SECOND;

    private static final long NANOS_OF_MINUTE = ((long) SECONDS_OF_MINUTE) * NANOS_OF_SECOND;

    private LocalTimeCodec() {
        super(LocalTime.class);
    }

    @Override
    public LocalTime decode(ByteBuf value,
                            MySqlReadableMetadata metadata,
                            Class<?> target,
                            boolean binary,
                            CodecContext context) {
        return decodeOrigin(binary, value);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof LocalTime;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new LocalTimeMySqlParameter((LocalTime) value);
    }

    @Override
    public boolean doCanDecode(MySqlReadableMetadata metadata) {
        return metadata.getType() == MySqlType.TIME;
    }

    static LocalTime decodeOrigin(boolean binary, ByteBuf value) {
        return binary ? decodeBinary(value) : readTimeText(value);
    }

    static LocalTime readTimeText(ByteBuf buf) {
        boolean isNegative = readNegative(buf);
        int hour = readIntInDigits(buf);
        int minute = readIntInDigits(buf);
        int second = readIntInDigits(buf);

        if (buf.isReadable()) {
            int nano = readMicroInDigits(buf) * NANOS_OF_MICRO;

            if (isNegative) {
                return negativeCircle(hour, minute, second, nano);
            }

            return LocalTime.of(hour % HOURS_OF_DAY, minute, second, nano);
        }

        if (isNegative) {
            return negativeCircle(hour, minute, second);
        }

        return LocalTime.of(hour % HOURS_OF_DAY, minute, second);
    }

    static boolean readNegative(ByteBuf buf) {
        return switch (buf.getByte(buf.readerIndex())) {
            case '-' -> {
                buf.skipBytes(1);
                yield true;
            }
            case '+' -> {
                buf.skipBytes(1);
                yield false;
            }
            default -> false;
        };
    }

    private static LocalTime decodeBinary(ByteBuf buf) {
        int bytes = buf.readableBytes();

        if (bytes < TIME_SIZE) {
            return LocalTime.MIDNIGHT;
        }

        boolean isNegative = buf.readBoolean();

        // Skip day part.
        buf.skipBytes(Integer.BYTES);

        byte hour = buf.readByte();
        byte minute = buf.readByte();
        byte second = buf.readByte();

        if (bytes < MICRO_TIME_SIZE) {
            if (isNegative) {
                return negativeCircle(hour, minute, second);
            }

            return LocalTime.of(hour % HOURS_OF_DAY, minute, second);
        }

        long nano = buf.readUnsignedIntLE() * NANOS_OF_MICRO;

        if (isNegative) {
            return negativeCircle(hour, minute, second, nano);
        }

        return LocalTime.of(hour % HOURS_OF_DAY, minute, second, (int) nano);
    }

    static ByteBuf encodeBinary(ByteBufAllocator alloc, LocalTime time) {
        if (LocalTime.MIDNIGHT.equals(time)) {
            // It is zero of var int, not terminal.
            return alloc.buffer(Byte.BYTES).writeByte(0);
        }

        int nanos = time.getNano();
        int size = nanos > 0 ? MICRO_TIME_SIZE : TIME_SIZE;

        ByteBuf buf = alloc.buffer(Byte.BYTES + size);

        try {
            buf.writeByte(size)
                    .writeBoolean(false)
                    .writeIntLE(0)
                    .writeByte(time.getHour())
                    .writeByte(time.getMinute())
                    .writeByte(time.getSecond());

            if (nanos > 0) {
                return buf.writeIntLE(nanos / NANOS_OF_MICRO);
            }

            return buf;
        } catch (Throwable e) {
            buf.release();
            throw e;
        }
    }

    static void encodeTime(ParameterWriter writer, LocalTime time) {
        int micros = time.getNano() / NANOS_OF_MICRO;
        DurationCodec.encodeTime(writer, false, time.getHour(), time.getMinute(), time.getSecond(), micros);
    }

    private static LocalTime negativeCircle(int hour, int minute, int second) {
        int total = -(hour * SECONDS_OF_HOUR + minute * SECONDS_OF_MINUTE + second);
        return LocalTime.ofSecondOfDay(((total % SECONDS_OF_DAY) + SECONDS_OF_DAY) % SECONDS_OF_DAY);
    }

    private static LocalTime negativeCircle(long hour, long minute, long second, long nano) {
        long total = -(hour * NANOS_OF_HOUR + minute * NANOS_OF_MINUTE +
                second * NANOS_OF_SECOND + nano);

        return LocalTime.ofNanoOfDay(((total % NANOS_OF_DAY) + NANOS_OF_DAY) % NANOS_OF_DAY);
    }

    private static final class LocalTimeMySqlParameter extends AbstractMySqlParameter {

        private final LocalTime value;

        private LocalTimeMySqlParameter(LocalTime value) {
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> encodeBinary(allocator, value));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> encodeTime(writer, value));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.TIME;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof LocalTimeMySqlParameter that) {
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
