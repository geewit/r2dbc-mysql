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

package io.geewit.persistence.r2dbc.mysql.message.client;

import io.geewit.persistence.r2dbc.mysql.MySqlParameter;
import io.geewit.persistence.r2dbc.mysql.ParameterWriter;
import io.geewit.persistence.r2dbc.mysql.Query;
import io.geewit.persistence.r2dbc.mysql.internal.util.OperatorUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * A default implementation of {@link ParameterWriter}.
 * <p>
 * WARNING: It is not thread safe
 */
final class ParamWriter extends ParameterWriter {

    private static final char[] HEX_CHAR = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c',
        'd', 'e', 'f' };

    private static final Consumer<MySqlParameter> DISPOSE = MySqlParameter::dispose;

    private final StringBuilder builder;

    private final boolean noBackslashEscapes;

    private final Query query;

    private int index;

    private Mode mode;

    private ParamWriter(boolean noBackslashEscapes, Query query) {
        this.builder = newBuilder(query);
        this.noBackslashEscapes = noBackslashEscapes;
        this.query = query;
        this.index = 1;
        this.mode = 1 < query.getPartSize() ? Mode.AVAILABLE : Mode.FULL;
    }

    @Override
    public void writeNull() {
        startAvailable(Mode.NULL);

        builder.append("NULL");
    }

    @Override
    public void writeInt(int value) {
        startAvailable(Mode.NUMERIC);

        builder.append(value);
    }

    @Override
    public void writeLong(long value) {
        startAvailable(Mode.NUMERIC);

        builder.append(value);
    }

    @Override
    public void writeUnsignedLong(long value) {
        startAvailable(Mode.NUMERIC);

        builder.append(Long.toUnsignedString(value));
    }

    @Override
    public void writeBigInteger(BigInteger value) {
        requireNonNull(value, "value must not be null");

        startAvailable(Mode.NUMERIC);
        builder.append(value);
    }

    @Override
    public void writeFloat(float value) {
        startAvailable(Mode.NUMERIC);

        builder.append(value);
    }

    @Override
    public void writeDouble(double value) {
        startAvailable(Mode.NUMERIC);

        builder.append(value);
    }

    @Override
    public void writeBigDecimal(BigDecimal value) {
        requireNonNull(value, "value must not be null");

        startAvailable(Mode.NUMERIC);
        builder.append(value);
    }

    @Override
    public void writeBinary(boolean bit) {
        startAvailable(Mode.BINARY);

        builder.append(bit ? '1' : '0');
    }

    @Override
    public void startHex() {
        startAvailable(Mode.HEX);
    }

    @Override
    public void writeHex(ByteBuffer buffer) {
        requireNonNull(buffer, "buffer must not be null");

        startAvailable(Mode.HEX);

        int limit = buffer.limit();
        for (int i = buffer.position(); i < limit; ++i) {
            byte b = buffer.get(i);
            builder.append(HEX_CHAR[(b & 0xF0) >>> 4])
                .append(HEX_CHAR[b & 0xF]);
        }
    }

    @Override
    public void writeHex(byte[] bytes) {
        requireNonNull(bytes, "bytes must not be null");

        startAvailable(Mode.HEX);

        for (byte b : bytes) {
            builder.append(HEX_CHAR[(b & 0xF0) >>> 4])
                .append(HEX_CHAR[b & 0xF]);
        }
    }

    @Override
    public void writeHex(long bits) {
        startAvailable(Mode.HEX);

        builder.append(Long.toHexString(bits));
    }

    @Override
    public void startString() {
        startAvailable(Mode.STRING);
    }

    @Override
    public void write(int c) {
        startAvailable(Mode.STRING);

        escape((char) c);
    }

    @Override
    public ParameterWriter append(char c) {
        startAvailable(Mode.STRING);

        escape(c);
        return this;
    }

    @Override
    public ParameterWriter append(CharSequence csq) {
        CharSequence s = csq == null ? "null" : csq;

        return append0(s, 0, s.length());
    }

    @Override
    public ParameterWriter append(CharSequence csq, int start, int end) {
        CharSequence s = csq == null ? "null" : csq;

        if (start < 0 || start > s.length() || end < start || end > s.length()) {
            throw new IndexOutOfBoundsException("start: " + start + ", end: " + end + ", str length: " +
                s.length());
        }

        return append0(s, start, end);
    }

    @Override
    public void write(String str) {
        String s = str == null ? "null" : str;

        write0(s, 0, s.length());
    }

    @Override
    public void write(String str, int off, int len) {
        String s = str == null ? "null" : str;

        if (off < 0 || off > s.length() || len < 0 || off + len > s.length() || off + len < 0) {
            throw new IndexOutOfBoundsException("off: " + off + ", len: " + len + ", str length: " +
                s.length());
        }

        write0(s, off, len);
    }

    @Override
    public void write(char[] c) {
        if (c == null) {
            this.write((String) null);
            return;
        }

        write0(c, 0, c.length);
    }

    @Override
    public void write(char[] c, int off, int len) {
        if (c == null) {
            this.write((String) null, off, len);
            return;
        }

        if (off < 0 || off > c.length || len < 0 || off + len > c.length || off + len < 0) {
            throw new IndexOutOfBoundsException("off: " + off + ", len: " + len + ", chars length: " +
                c.length);
        }

        this.write0(c, off, len);
    }

    private String toSql() {
        if (this.mode != Mode.FULL) {
            throw new IllegalStateException("Unexpected completion, parameters are not filled");
        }

        return this.builder.toString();
    }

    private void startAvailable(Mode mode) {
        Mode current = this.mode;

        if (current == Mode.AVAILABLE) {
            this.mode = mode;
            mode.start(this.builder);
            return;
        } else if (current.canFollow(mode)) {
            return;
        }

        if (current == Mode.FULL) {
            throw new IllegalStateException("Unexpected write, parameters are filled-up");
        }

        throw new IllegalStateException("Unexpected write, mode is " + current + ", write with " + mode);
    }

    private void flushParameter(Void ignored) {
        Mode current = this.mode;

        switch (current) {
            case FULL:
                return;
            case AVAILABLE:
                // This parameter never be filled, filling with STRING mode by default.
                this.builder.append('\'').append('\'');
                break;
            default:
                current.end(this.builder);
                break;
        }

        query.partTo(builder, index++);
        this.mode = index < query.getPartSize() ? Mode.AVAILABLE : Mode.FULL;
    }

    private ParamWriter append0(CharSequence csq, int start, int end) {
        this.startAvailable(Mode.STRING);

        for (int i = start; i < end; ++i) {
            this.escape(csq.charAt(i));
        }

        return this;
    }

    private void write0(String s, int off, int len) {
        this.startAvailable(Mode.STRING);

        int end = len + off;
        for (int i = off; i < end; ++i) {
            this.escape(s.charAt(i));
        }
    }

    private void write0(char[] s, int off, int len) {
        this.startAvailable(Mode.STRING);

        int end = len + off;
        for (int i = off; i < end; ++i) {
            this.escape(s[i]);
        }
    }

    private void escape(char c) {
        if (c == '\'') {
            // MySQL will auto-combine consecutive strings, whatever backslash is used or not, e.g. '1''2' -> '1\'2'
            builder.append('\'').append('\'');
            return;
        } else if (noBackslashEscapes) {
            builder.append(c);
            return;
        }

        switch (c) {
            case '\\':
                builder.append('\\').append('\\');
                break;
            // Maybe useful in the future, keep '"' here.
            // case '"': buf.append('\\').append('"'); break;
            // SHIFT-JIS, WINDOWS-932, EUC-JP and eucJP-OPEN will encode '¥' (the sign of Japanese Yen
            // or Chinese Yuan) to '\' (ASCII 92). X-IBM949, X-IBM949C will encode '₩' (the sign of
            // Korean Won) to '\'. It is nothing because the driver is using UTF-8. See also CharCollation.
            // case '¥': do something; break;
            // case '₩': do something; break;
            case 0:
                // Should escape '\0' which is an end flag in C style string.
                builder.append('\\').append('0');
                break;
            case '\032':
                // It gives some problems on Win32.
                builder.append('\\').append('Z');
                break;
            case '\n':
                // Should be escaped for better logging.
                builder.append('\\').append('n');
                break;
            case '\r':
                // Should be escaped for better logging.
                builder.append('\\').append('r');
                break;
            default:
                builder.append(c);
                break;
        }
    }

    static Mono<String> publish(boolean noBackslashEscapes,
                                Query query,
                                Flux<MySqlParameter> values) {
        return Mono.defer(() -> {
            try (ParamWriter writer = new ParamWriter(noBackslashEscapes, query)) {
                return OperatorUtils.discardOnCancel(values)
                        .doOnDiscard(MySqlParameter.class, DISPOSE)
                        .concatMap(it -> it.publishText(writer).doOnSuccess(writer::flushParameter))
                        .then(Mono.fromCallable(writer::toSql));
            }
        });
    }

    private static StringBuilder newBuilder(Query query) {
        StringBuilder builder = new StringBuilder(Math.min(query.getFormattedSize(), 64));

        query.partTo(builder, 0);

        return builder;
    }

    private enum Mode {

        AVAILABLE,
        FULL,

        NULL {
            @Override
            boolean canFollow(Mode mode) {
                return false;
            }
        },

        NUMERIC {
            @Override
            boolean canFollow(Mode mode) {
                return false;
            }
        },

        BINARY {
            @Override
            void start(StringBuilder builder) {
                builder.append('b').append('\'');
            }

            @Override
            void end(StringBuilder builder) {
                builder.append('\'');
            }
        },

        HEX {
            @Override
            void start(StringBuilder builder) {
                builder.append('x').append('\'');
            }

            @Override
            void end(StringBuilder builder) {
                builder.append('\'');
            }
        },

        STRING {
            @Override
            boolean canFollow(Mode mode) {
                return this == mode || mode == Mode.NUMERIC;
            }

            @Override
            void start(StringBuilder builder) {
                builder.append('\'');
            }

            @Override
            void end(StringBuilder builder) {
                builder.append('\'');
            }
        };

        void start(StringBuilder builder) {
            // Do nothing
        }

        void end(StringBuilder builder) {
            // Do nothing
        }

        boolean canFollow(Mode mode) {
            return this == mode;
        }
    }
}
