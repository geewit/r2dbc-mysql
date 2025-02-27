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

import io.geewit.persistence.r2dbc.mysql.Capability;
import io.geewit.persistence.r2dbc.mysql.authentication.MySqlAuthProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.util.Arrays;

import static io.geewit.persistence.r2dbc.mysql.constant.Packets.TERMINAL;
import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;
import static io.geewit.persistence.r2dbc.mysql.internal.util.InternalArrays.EMPTY_BYTES;

/**
 * MySQL Handshake Message for handshake protocol version 9.
 */
final class HandshakeV9Request implements HandshakeRequest {

    private static final Capability SERVER_CAPABILITY = Capability.of(0);

    private final HandshakeHeader header;

    private final byte[] salt;

    private HandshakeV9Request(HandshakeHeader header, byte[] salt) {
        this.header = requireNonNull(header, "header must not be null");
        this.salt = requireNonNull(salt, "salt must not be null");
    }

    @Override
    public HandshakeHeader getHeader() {
        return header;
    }

    @Override
    public Capability getServerCapability() {
        return SERVER_CAPABILITY;
    }

    @Override
    public String getAuthType() {
        return MySqlAuthProvider.MYSQL_OLD_PASSWORD;
    }

    @Override
    public byte[] getSalt() {
        return salt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof HandshakeV9Request that) {
            return header.equals(that.header) && Arrays.equals(salt, that.salt);
        }
        return false;

    }

    @Override
    public int hashCode() {
        int result = header.hashCode();
        return 31 * result + Arrays.hashCode(salt);
    }

    @Override
    public String toString() {
        return "HandshakeV9Request{header=" + header + ", salt=REDACTED}";
    }

    static HandshakeV9Request decode(ByteBuf buf, HandshakeHeader header) {
        int bytes = buf.readableBytes();

        if (bytes <= 0) {
            return new HandshakeV9Request(header, EMPTY_BYTES);
        }

        byte[] salt;

        if (buf.getByte(buf.writerIndex() - 1) == TERMINAL) {
            salt = ByteBufUtil.getBytes(buf, buf.readerIndex(), bytes - 1);
        } else {
            salt = ByteBufUtil.getBytes(buf);
        }

        return new HandshakeV9Request(header, salt);
    }
}
