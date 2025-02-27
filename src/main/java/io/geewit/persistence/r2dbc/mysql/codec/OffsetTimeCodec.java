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

import java.time.*;

/**
 * Codec for {@link OffsetTime}.
 */
final class OffsetTimeCodec extends AbstractClassedCodec<OffsetTime> {

    static final OffsetTimeCodec INSTANCE = new OffsetTimeCodec();

    private OffsetTimeCodec() {
        super(OffsetTime.class);
    }

    @Override
    public OffsetTime decode(ByteBuf value,
                             MySqlReadableMetadata metadata,
                             Class<?> target,
                             boolean binary,
                             CodecContext context) {
        // OffsetTime is not an instant value, so preserveInstants is not used here.
        LocalTime origin = LocalTimeCodec.decodeOrigin(binary, value);
        ZoneId zone = ZoneId.systemDefault().normalized();

        return OffsetTime.of(origin, zone instanceof ZoneOffset zoneOffset ? zoneOffset : zone.getRules()
                .getStandardOffset(Instant.EPOCH));
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new OffsetTimeMySqlParameter((OffsetTime) value);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof OffsetTime;
    }

    @Override
    public boolean doCanDecode(MySqlReadableMetadata metadata) {
        return metadata.getType() == MySqlType.TIME;
    }

    private static final class OffsetTimeMySqlParameter extends AbstractMySqlParameter {

        private final OffsetTime value;

        private OffsetTimeMySqlParameter(OffsetTime value) {
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> LocalTimeCodec.encodeBinary(allocator, serverValue()));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> LocalTimeCodec.encodeTime(writer, serverValue()));
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
            if (o instanceof OffsetTimeMySqlParameter that) {
                return value.equals(that.value);
            }
            return false;

        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        private LocalTime serverValue() {
            // OffsetTime is not an instant value, so preserveInstants is not used here.
            ZoneId zone = ZoneId.systemDefault().normalized();

            if (zone instanceof ZoneOffset zoneOffset) {
                return value.withOffsetSameInstant(zoneOffset).toLocalTime();
            }

            ZoneOffset offset = zone.getRules().getStandardOffset(Instant.EPOCH);

            return value.toLocalTime()
                    .plusSeconds(offset.getTotalSeconds() - value.getOffset().getTotalSeconds());
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }
}
