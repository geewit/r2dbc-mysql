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

import io.netty.buffer.ByteBuf;

/**
 * Authentication more data request, means continue send auth change response message if is exists.
 */
public final class AuthMoreDataMessage implements ServerMessage {

    private static final byte AUTH_SUCCEED = 3;

    private final boolean failed;

    private AuthMoreDataMessage(boolean failed) {
        this.failed = failed;
    }

    public boolean isFailed() {
        return failed;
    }

    static AuthMoreDataMessage decode(ByteBuf buf) {
        buf.skipBytes(1); // auth more data message header, 0x01

        return new AuthMoreDataMessage(buf.readByte() != AUTH_SUCCEED);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AuthMoreDataMessage that = (AuthMoreDataMessage) o;

        return failed == that.failed;
    }

    @Override
    public int hashCode() {
        return (failed ? 1 : 0);
    }

    @Override
    public String toString() {
        return "AuthMoreDataMessage{failed=" + failed + '}';
    }
}
