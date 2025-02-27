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

import io.geewit.persistence.r2dbc.mysql.ConnectionContext;
import io.geewit.persistence.r2dbc.mysql.MySqlParameter;
import io.geewit.persistence.r2dbc.mysql.internal.util.OperatorUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * A message to execute a prepared statement once with parameter.
 */
public final class PreparedExecuteMessage implements ClientMessage, Disposable {

    /**
     * No cursor, just return entire result without fetch.
     */
    private static final byte NO_CURSOR = 0;

    /**
     * Read only cursor, it will be closed when prepared statement closing and resetting.
     */
    private static final byte READ_ONLY = 1;

    // Useless cursors for R2DBC client.
//    private static final byte FOR_UPDATE = 2;
//    private static final byte SCROLLABLE = 4;

    /**
     * The fixed size of execute message when it has no parameter.
     */
    private static final int NO_PARAM_SIZE = Byte.BYTES + Integer.BYTES + Byte.BYTES + Integer.BYTES;

    private static final int TIMES = 1;

    private static final byte EXECUTE_FLAG = 0x17;

    private final int statementId;

    /**
     * Immediate execute once instead of fetch.
     */
    private final boolean immediate;

    private final MySqlParameter[] values;

    public PreparedExecuteMessage(int statementId, boolean immediate, MySqlParameter[] values) {
        this.values = requireNonNull(values, "values must not be null");
        this.statementId = statementId;
        this.immediate = immediate;
    }

    @Override
    public void dispose() {
        for (MySqlParameter value : values) {
            value.dispose();
        }
        Arrays.fill(values, null);
    }

    @Override
    public String toString() {
        return "PreparedExecuteMessage{statementId=" + statementId + ", immediate=" + immediate + ", has " +
            values.length + " parameters}";
    }

    @Override
    public Flux<ByteBuf> encode(ByteBufAllocator allocator, ConnectionContext context) {
        requireNonNull(allocator, "allocator must not be null");
        requireNonNull(context, "context must not be null");

        return Flux.defer(() -> {
            int size = values.length;
            ByteBuf buf;

            if (size == 0) {
                buf = allocator.buffer(NO_PARAM_SIZE);
            } else {
                buf = allocator.buffer();
            }

            try {
                buf.writeByte(EXECUTE_FLAG)
                    .writeIntLE(statementId)
                    .writeByte(immediate ? NO_CURSOR : READ_ONLY)
                    .writeIntLE(TIMES);

                if (size == 0) {
                    return Flux.just(buf);
                }

                List<MySqlParameter> nonNull = new ArrayList<>(size);
                byte[] nullMap = fillNullBitmap(size, nonNull);

                // Fill null-bitmap.
                buf.writeBytes(nullMap);

                if (nonNull.isEmpty()) {
                    // No need rebound.
                    buf.writeBoolean(false);
                    return Flux.just(buf);
                }

                buf.writeBoolean(true);
                writeTypes(buf, size);

                Flux<ByteBuf> parameters = OperatorUtils.discardOnCancel(Flux.fromArray(values))
                    .doOnDiscard(MySqlParameter.class, MySqlParameter::dispose)
                    .concatMap(mySqlParameter -> mySqlParameter.publishBinary(allocator));

                return Flux.just(buf).concatWith(parameters);
            } catch (Throwable e) {
                buf.release();
                cancelParameters();
                return Flux.error(e);
            }
        });
    }

    private byte[] fillNullBitmap(int size, List<MySqlParameter> nonNull) {
        byte[] nullMap = new byte[ceilDiv8(size)];

        for (int i = 0; i < size; ++i) {
            MySqlParameter value = values[i];

            if (value.isNull()) {
                nullMap[i >> 3] |= (byte) (1 << (i & 7));
            } else {
                nonNull.add(value);
            }
        }

        return nullMap;
    }

    private void writeTypes(ByteBuf buf, int size) {
        for (int i = 0; i < size; ++i) {
            buf.writeShortLE(values[i].getType().getId());
        }
    }

    private void cancelParameters() {
        for (MySqlParameter value : values) {
            value.dispose();
        }
    }

    private static int ceilDiv8(int x) {
        int r = x >> 3;
        return (r << 3) == x ? r : r + 1;
    }
}
