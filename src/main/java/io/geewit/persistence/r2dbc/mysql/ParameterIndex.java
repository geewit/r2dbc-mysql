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


/**
 * A data class considers indexes of a named parameter. Most case of the relation between parameter name and
 * index is one-to-one.
 */
final class ParameterIndex {

    private static final int INIT_CAPACITY = 4;

    private final int first;

    private int[] values;

    private int size = 1;

    ParameterIndex(int first) {
        this.first = first;
    }

    void push(int value) {
        if (values == null) {
            int[] data = new int[INIT_CAPACITY];

            data[0] = first;
            data[1] = value;

            this.values = data;
            this.size = 2;
        } else {
            int i = this.size++;

            if (i >= values.length) {
                int[] data = new int[values.length << 1];
                System.arraycopy(values, 0, data, 0, values.length);
                this.values = data;
            }

            this.values[i] = value;
        }
    }

    void bind(Binding binding, MySqlParameter value) {
        if (values == null) {
            binding.add(first, value);
        } else {
            for (int i = 0; i < size; ++i) {
                binding.add(values[i], value);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ParameterIndex that) {
            if (size != that.size) {
                return false;
            }

            if (values == null) {
                return that.values == null && first == that.first;
            }

            if (that.values == null) {
                return false;
            }

            for (int i = 0; i < size; ++i) {
                if (values[i] != that.values[i]) {
                    return false;
                }
            }

            return true;
        } else {
            return false;
        }

    }

    @Override
    public int hashCode() {
        if (values == null) {
            return first;
        }

        int result = 1;

        for (int i = 0; i < size; ++i) {
            result = 31 * result + values[i];
        }

        return result;
    }

    @Override
    public String toString() {
        if (values == null) {
            return Integer.toString(first);
        }

        StringBuilder builder = new StringBuilder()
            .append('[')
            .append(values[0]);

        for (int i = 1; i < size; ++i) {
            builder.append(", ")
                .append(values[i]);
        }

        return builder.append(']').toString();
    }
}
