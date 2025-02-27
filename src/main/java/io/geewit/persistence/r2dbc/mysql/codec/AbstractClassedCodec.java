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

/**
 * Codec for classed type when field bytes less or equals than {@link Integer#MAX_VALUE}.
 *
 * @param <T> the type of handling data.
 */
abstract class AbstractClassedCodec<T> implements Codec<T> {

    private final Class<? extends T> type;

    AbstractClassedCodec(Class<? extends T> type) {
        this.type = type;
    }

    @Override
    public final boolean canDecode(MySqlReadableMetadata metadata, Class<?> target) {
        return target.isAssignableFrom(this.type) && doCanDecode(metadata);
    }

    @Override
    public final Class<? extends T> getMainClass() {
        return this.type;
    }

    protected abstract boolean doCanDecode(MySqlReadableMetadata metadata);
}
