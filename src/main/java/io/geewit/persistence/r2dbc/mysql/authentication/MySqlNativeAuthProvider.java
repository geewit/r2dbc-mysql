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

package io.geewit.persistence.r2dbc.mysql.authentication;

import io.geewit.persistence.r2dbc.mysql.collation.CharCollation;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;
import static io.geewit.persistence.r2dbc.mysql.internal.util.InternalArrays.EMPTY_BYTES;

/**
 * An implementation of {@link MySqlAuthProvider} for type "mysql_native_password".
 */
final class MySqlNativeAuthProvider implements MySqlAuthProvider {

    static MySqlNativeAuthProvider getInstance() {
        return LazyHolder.INSTANCE;
    }

    private static final String ALGORITHM = "SHA-1";

    private static final boolean IS_LEFT_SALT = true;

    @Override
    public boolean isSslNecessary() {
        return false;
    }

    /**
     * SHA1(password) `all bytes xor` SHA1( salt + SHA1( SHA1(password) ) )
     * <p>
     * {@inheritDoc}
     */
    @Override
    public byte[] authentication(CharSequence password, byte[] salt, CharCollation collation) {
        if (password == null || password.isEmpty()) {
            return EMPTY_BYTES;
        }

        requireNonNull(salt, "salt must not be null when password exists");
        requireNonNull(collation, "collation must not be null when password exists");

        return AuthUtils.hash(ALGORITHM, IS_LEFT_SALT, password, salt, collation.getCharset());
    }

    @Override
    public MySqlAuthProvider next() {
        return this;
    }

    @Override
    public String getType() {
        return MYSQL_NATIVE_PASSWORD;
    }

    private MySqlNativeAuthProvider() {
    }

    private static class LazyHolder {
        private static final MySqlNativeAuthProvider INSTANCE = new MySqlNativeAuthProvider();
    }
}
