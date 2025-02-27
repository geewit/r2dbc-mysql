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

package io.geewit.persistence.r2dbc.mysql.extension;

import io.geewit.persistence.r2dbc.mysql.codec.CodecRegistry;
import io.netty.buffer.ByteBufAllocator;

/**
 * Registrar interface that is used to register {@code Codec}s as extension to built-in codecs.
 * <p>
 * This will be registered before the connection is established.
 */
@FunctionalInterface
public interface CodecRegistrar extends Extension {

    /**
     * Register codec(s) into a {@link CodecRegistry} of the connection.
     *
     * @param allocator a {@link ByteBufAllocator} for allocate {@code ByteBuf}, non {@code null}.
     * @param registry  the {@link CodecRegistry}, it can register multiple codecs, non {@code null}.
     */
    void register(ByteBufAllocator allocator, CodecRegistry registry);
}
