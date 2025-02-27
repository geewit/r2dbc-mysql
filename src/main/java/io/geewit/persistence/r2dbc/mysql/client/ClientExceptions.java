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

package io.geewit.persistence.r2dbc.mysql.client;

import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;

/**
 * A utility considers generic exceptions to {@link Client}.
 */
final class ClientExceptions {

    static R2dbcException exchangeClosed() {
        return new MySqlConnectionClosedException("Cannot exchange because the connection is closed");
    }

    static R2dbcException unexpectedClosed() {
        return new MySqlConnectionClosedException("Connection unexpectedly closed");
    }

    static R2dbcException expectedClosed() {
        return new MySqlConnectionClosedException("Connection closed");
    }

    static R2dbcException unsupportedProtocol(String type) {
        return new MySqlProtocolException("Unexpected protocol message: [" + type + ']');
    }

    static R2dbcException wrap(Throwable e) {
        if (e instanceof R2dbcException r2dbcException) {
            return r2dbcException;
        }

        return new MySqlConnectionException(e);
    }

    private ClientExceptions() { }
}

final class MySqlConnectionClosedException extends MySqlConnectionException {

    MySqlConnectionClosedException(String reason) {
        super(reason);
    }
}

final class MySqlProtocolException extends MySqlConnectionException {

    MySqlProtocolException(String reason) {
        super(reason);
    }
}

class MySqlConnectionException extends R2dbcNonTransientResourceException {

    MySqlConnectionException(String reason) {
        super(reason);
    }

    MySqlConnectionException(Throwable cause) {
        super(cause);
    }
}
