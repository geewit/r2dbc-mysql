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

package io.geewit.persistence.r2dbc.mysql.codec;

import io.geewit.persistence.r2dbc.mysql.api.MySqlReadableMetadata;
import io.netty.buffer.ByteBuf;

import java.lang.reflect.ParameterizedType;

/**
 * Special codec for decode values with parameterized types.
 * <p>
 * It also can encode and decode values without parameter.
 *
 * @param <T> the type without parameter that is handled by this codec.
 */
public interface ParameterizedCodec<T> extends Codec<T> {

    /**
     * Decodes a {@link ByteBuf} as specified {@link ParameterizedType}.
     *
     * @param value    the {@link ByteBuf}.
     * @param metadata the metadata of the column or the {@code OUT} parameter.
     * @param target   the specified {@link ParameterizedType}.
     * @param binary   if the value should be decoded by binary protocol.
     * @param context  the codec context.
     * @return the decoded result.
     */
    Object decode(ByteBuf value,
                  MySqlReadableMetadata metadata,
                  ParameterizedType target,
                  boolean binary,
                  CodecContext context);

    /**
     * Checks if the field value can be decoded as specified {@link ParameterizedType}.
     *
     * @param metadata the metadata of the column or the {@code OUT} parameter.
     * @param target   the specified {@link ParameterizedType}.
     * @return if it can decode.
     */
    boolean canDecode(MySqlReadableMetadata metadata, ParameterizedType target);
}
