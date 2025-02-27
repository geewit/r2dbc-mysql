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

package io.geewit.persistence.r2dbc.mysql.codec.lob;

import io.geewit.persistence.r2dbc.mysql.collation.CharCollation;
import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Clob;

/**
 * An implementation of {@link Clob} for singleton {@link ByteBuf}.
 */
final class SingletonClob extends SingletonLob<CharSequence> implements Clob {

    private final CharCollation collation;

    SingletonClob(ByteBuf buf, CharCollation collation) {
        super(buf);

        this.collation = collation;
    }

    @Override
    protected CharSequence convert(ByteBuf buf) {
        if (!buf.isReadable()) {
            return "";
        }

        return buf.readCharSequence(buf.readableBytes(), collation.getCharset());
    }
}
