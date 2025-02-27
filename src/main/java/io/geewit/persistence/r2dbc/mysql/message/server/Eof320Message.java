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

/**
 * An EOF message for current context in protocol 3.20.
 * <p>
 * Note: It is also Old Change Authentication Request.
 */
final class Eof320Message implements EofMessage {

    static final int SIZE = Byte.BYTES;

    static final Eof320Message INSTANCE = new Eof320Message();

    @Override
    public boolean isDone() {
        // If server use protocol 3.20, it should not supports multi-results.
        return true;
    }

    @Override
    public String toString() {
        return "Eof320Message{}";
    }

    private Eof320Message() { }
}
