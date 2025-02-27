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

import io.geewit.persistence.r2dbc.mysql.Capability;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.Map;

import static io.geewit.persistence.r2dbc.mysql.constant.Packets.TERMINAL;

/**
 * An abstraction of {@link SubsequenceClientMessage} considers handshake response.
 */
public interface HandshakeResponse extends SubsequenceClientMessage {

    /**
     * Construct an instance of {@link HandshakeResponse}, it is implemented by the protocol version that is
     * given by {@link Capability}.
     *
     * @param capability           the current {@link Capability}.
     * @param collationId          the {@code CharCollation} ID, or 0 if server does not return.
     * @param user                 the username for login.
     * @param authentication       the password authentication for login.
     * @param authType             the authentication plugin type.
     * @param database             the connecting database, may be empty.
     * @param attributes           the connecting attributes.
     * @param zstdCompressionLevel the Zstd compression level.
     * @return the instance implemented by the specified protocol version.
     */
    static HandshakeResponse from(Capability capability,
                                  int collationId,
                                  String user,
                                  byte[] authentication,
                                  String authType,
                                  String database,
                                  Map<String, String> attributes,
                                  int zstdCompressionLevel) {
        if (capability.isProtocol41()) {
            return new HandshakeResponse41(
                    capability,
                    collationId,
                    user,
                    authentication,
                    authType,
                    database,
                    attributes,
                    zstdCompressionLevel);
        }

        return new HandshakeResponse320(capability, user, authentication, database);
    }

    /**
     * Write a C-style string into a {@link ByteBuf}.
     *
     * @param buf     the {@link ByteBuf} for writing.
     * @param value   the string value.
     * @param charset the character set for encoding.
     */
    static void writeCString(ByteBuf buf, String value, Charset charset) {
        if (!value.isEmpty()) {
            buf.writeCharSequence(value, charset);
        }
        buf.writeByte(TERMINAL);
    }
}
