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

import io.geewit.persistence.r2dbc.mysql.message.client.PreparedExecuteMessage;
import io.geewit.persistence.r2dbc.mysql.message.client.PreparedTextQueryMessage;

import java.util.Arrays;

/**
 * A collection of {@link MySqlParameter} for one bind invocation of a parameterized statement.
 *
 * @see ParameterizedStatementSupport
 */
final class Binding {

    private static final MySqlParameter[] EMPTY_VALUES = { };

    private final MySqlParameter[] values;

    Binding(int length) {
        this.values = length == 0 ? EMPTY_VALUES : new MySqlParameter[length];
    }

    /**
     * Add a {@link MySqlParameter} to the binding.
     *
     * @param index the index of the {@link MySqlParameter}
     * @param value the {@link MySqlParameter} from {@link PrepareParameterizedStatement}
     */
    void add(int index, MySqlParameter value) {
        if (index < 0 || index >= this.values.length) {
            throw new IndexOutOfBoundsException("Index: " + index + ", length: " + this.values.length);
        }

        this.values[index] = value;
    }

    /**
     * Converts bindings to a request message. If not need execute immediate, it will return an open cursor
     * message.
     *
     * @param statementId prepared statement identifier.
     * @param immediate   if it should be executed immediate, otherwise return an open cursor message
     * @return an execute message or open cursor message
     */
    PreparedExecuteMessage toExecuteMessage(int statementId, boolean immediate) {
        if (values.length == 0) {
            QueryLogger.log(statementId, EMPTY_VALUES);

            return new PreparedExecuteMessage(statementId, immediate, EMPTY_VALUES);
        }

        if (values[0] == null) {
            throw new IllegalStateException("Parameters has been used");
        }

        MySqlParameter[] values = drainValues();

        QueryLogger.log(statementId, values);

        return new PreparedExecuteMessage(statementId, immediate, values);
    }

    PreparedTextQueryMessage toTextMessage(Query query,
                                           String returning) {
        MySqlParameter[] values = drainValues();

        QueryLogger.log(query, returning, values);

        return new PreparedTextQueryMessage(query, returning, values);
    }

    /**
     * Clear/release binding values.
     */
    void clear() {
        int size = this.values.length;
        for (int i = 0; i < size; ++i) {
            MySqlParameter value = this.values[i];
            this.values[i] = null;

            if (value != null) {
                value.dispose();
            }
        }
    }

    int findUnbind() {
        int size = this.values.length;

        for (int i = 0; i < size; ++i) {
            if (this.values[i] == null) {
                return i;
            }
        }

        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof Binding binding) {
            return Arrays.equals(this.values, binding.values);
        }
        return false;

    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public String toString() {
        return String.format("Binding{values=%s}", Arrays.toString(values));
    }

    private MySqlParameter[] drainValues() {
        MySqlParameter[] results = new MySqlParameter[this.values.length];

        System.arraycopy(this.values, 0, results, 0, this.values.length);
        Arrays.fill(this.values, null);

        return results;
    }
}
