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

package io.geewit.persistence.r2dbc.mysql.cache;

import java.util.function.IntConsumer;

/**
 * A disabled {@link PrepareCache}.
 */
final class PrepareDisabledCache implements PrepareCache {

    @Override
    public Integer getIfPresent(String key) {
        return null;
    }

    @Override
    public boolean putIfAbsent(String key, int value, IntConsumer evict) {
        // Put always fails.
        return false;
    }
}
