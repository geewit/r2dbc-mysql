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

package io.geewit.persistence.r2dbc.mysql;

import io.geewit.persistence.r2dbc.mysql.constant.MySqlType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/**
 * A parameter value bound to an {@link Binding}.
 * <p>
 * TODO: add ScalarParameter for better performance.
 */
public interface MySqlParameter extends Disposable {

    /**
     * Note: the {@code null} is processed by built-in codecs.
     *
     * @return {@code true} if it is {@code null}. Codec extensions should always return {@code false}.
     */
    default boolean isNull() {
        return false;
    }

    /**
     * Binary protocol encoding. See MySQL protocol documentations, if don't want to support the binary
     * protocol, please receive an exception.
     * <p>
     * Note: not like the text protocol, it makes a sense for state-less.
     * <p>
     * Binary protocol maybe need to add a var-integer length before encoded content. So if makes it like
     * {@code Mono<Void> publishBinary (Xxx binaryWriter)}, and if supports multiple times writing like a
     * {@code OutputStream} or {@code Writer} for each parameter, this make a hell of a complex state system.
     * If we don't support multiple times writing, it will be hard to understand and maybe make a confuse to
     * user.
     *
     * @param allocator the buffer allocator.
     *
     * @return the encoded binary buffer(s).
     */
    Publisher<ByteBuf> publishBinary(ByteBufAllocator allocator);

    /**
     * Text protocol encoding.
     * <p>
     * Note: not like the binary protocol, it make a sense for copy-less.
     * <p>
     * If it seems like {@code Publisher<? extends CharSequence> publishText()}, then we need to always deep
     * copy results (with escaping) into the string buffer of the synthesized SQL statement.
     * <p>
     * WARNING: the {@code output} requires state synchronization after this function called, so if external
     * writer buffered the {@code writer}, please flush the external buffer before receiving the completion
     * signal.
     *
     * @param writer the text protocol writer, extended {@code Writer}, not thread-safety.
     * @return the encoding completion signal.
     */
    Mono<Void> publishText(ParameterWriter writer);

    /**
     * Gets the {@link MySqlType} of this parameter data.
     * <p>
     * If it does not want to support the binary protocol, just throw an exception please.
     *
     * @return the MySQL type.
     */
    MySqlType getType();

    /**
     * {@inheritDoc}
     */
    @Override
    default void dispose() { }
}
