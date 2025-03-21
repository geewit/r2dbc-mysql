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
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;

/**
 * Codec for {@link String}.
 */
final class StringCodec extends AbstractClassedCodec<String> {

    static final StringCodec INSTANCE = new StringCodec();

    private StringCodec() {
        super(String.class);
    }

    @Override
    public String decode(ByteBuf value,
                         MySqlReadableMetadata metadata,
                         Class<?> target,
                         boolean binary,
                         CodecContext context) {
        if (!value.isReadable()) {
            return "";
        }

        return value.toString(metadata.getCharCollation(context).getCharset());
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof CharSequence;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new StringMySqlParameter((CharSequence) value, context);
    }

    @Override
    protected boolean doCanDecode(MySqlReadableMetadata metadata) {
        return metadata.getType().isString();
    }

    static ByteBuf encodeCharSequence(ByteBufAllocator allocator,
                                      CharSequence value,
                                      CodecContext context) {
        int length = value.length();

        if (length == 0) {
            // It is zero of var int, not terminal.
            return allocator.buffer(Byte.BYTES).writeByte(0);
        }

        Charset charset = context.getClientCollation().getCharset();
        ByteBuf content = allocator.buffer();

        try {
            VarIntUtils.reserveVarInt(content);

            return VarIntUtils.setReservedVarInt(content, content.writeCharSequence(value, charset));
        } catch (Throwable e) {
            content.release();
            throw e;
        }
    }

    private static final class StringMySqlParameter extends AbstractMySqlParameter {

        private final CharSequence value;

        private final CodecContext context;

        private StringMySqlParameter(CharSequence value, CodecContext context) {
            this.value = value;
            this.context = context;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> encodeCharSequence(allocator, value, context));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.append(value));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.VARCHAR;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof StringMySqlParameter that) {
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
