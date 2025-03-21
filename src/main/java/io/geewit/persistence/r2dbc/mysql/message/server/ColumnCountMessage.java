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

package io.geewit.persistence.r2dbc.mysql.message.server;

import io.geewit.persistence.r2dbc.mysql.internal.util.VarIntUtils;
import io.netty.buffer.ByteBuf;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.require;

/**
 * A message that is start envelope for {@literal SELECT} query result, {@link #totalColumns}  how many
 * columns will be returned for the result.
 */
public final class ColumnCountMessage implements ServerMessage {

    private final int totalColumns;

    private ColumnCountMessage(int totalColumns) {
        require(totalColumns > 0, "totalColumns must be a positive integer");

        this.totalColumns = totalColumns;
    }

    public int getTotalColumns() {
        return totalColumns;
    }

    static ColumnCountMessage decode(ByteBuf buf) {
        // JVM does NOT support arrays longer than Integer.MAX_VALUE
        return new ColumnCountMessage(Math.toIntExact(VarIntUtils.readVarInt(buf)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ColumnCountMessage that) {
            return totalColumns == that.totalColumns;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return totalColumns;
    }

    @Override
    public String toString() {
        return "ColumnCountMessage{totalColumns=" + totalColumns + '}';
    }
}
