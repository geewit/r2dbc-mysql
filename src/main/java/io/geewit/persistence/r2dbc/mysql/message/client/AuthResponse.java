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

import io.netty.buffer.ByteBuf;

import java.util.Arrays;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * A message that contains only an authentication, used by full authentication or change authentication
 * response.
 */
public final class AuthResponse extends SizedClientMessage implements SubsequenceClientMessage {

    private final byte[] authentication;

    public AuthResponse(byte[] authentication) {
        this.authentication = requireNonNull(authentication, "authentication must not be null");
    }

    @Override
    protected int size() {
        return authentication.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AuthResponse that = (AuthResponse) o;

        return Arrays.equals(authentication, that.authentication);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(authentication);
    }

    @Override
    public String toString() {
        return "AuthResponse{authentication=REDACTED}";
    }

    @Override
    protected void writeTo(ByteBuf buf) {
        buf.writeBytes(authentication);
    }
}
