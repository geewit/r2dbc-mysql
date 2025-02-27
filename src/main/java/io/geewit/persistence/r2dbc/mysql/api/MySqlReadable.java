/*
 * Copyright 2024 geewit.io projects
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

package io.geewit.persistence.r2dbc.mysql.api;

import io.r2dbc.spi.Readable;

/**
 * {@link Readable Readable data} for a row or a collection of {@code OUT} parameters that's against a MySQL
 * database.
 *
 * @see MySqlOutParameters
 * @see MySqlRow
 * @since 1.1.3
 */
public interface MySqlReadable extends Readable {
}
