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

import io.geewit.persistence.r2dbc.mysql.api.MySqlResult;
import io.geewit.persistence.r2dbc.mysql.api.MySqlStatement;
import io.geewit.persistence.r2dbc.mysql.client.Client;
import io.geewit.persistence.r2dbc.mysql.codec.Codecs;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.require;
import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * Base class considers parameterized {@link MySqlStatement} with parameter markers.
 * <p>
 * MySQL uses indexed parameters which are marked by {@literal ?} without naming. Implementations should use
 * {@link Query} to supports named parameters.
 */
abstract class ParameterizedStatementSupport extends MySqlStatementSupport {

    protected final Codecs codecs;

    protected final Query query;

    private final Bindings bindings;

    private final AtomicBoolean executed = new AtomicBoolean();

    ParameterizedStatementSupport(Client client, Codecs codecs, Query query) {
        super(client);

        requireNonNull(query, "query must not be null");
        require(query.getParameters() > 0, "parameters must be a positive integer");

        this.codecs = requireNonNull(codecs, "codecs must not be null");
        this.query = query;
        this.bindings = new Bindings(query.getParameters());
    }

    @Override
    public final MySqlStatement add() {
        assertNotExecuted();

        this.bindings.validatedFinish();
        return this;
    }

    @Override
    public final MySqlStatement bind(int index, Object value) {
        requireNonNull(value, "value must not be null");

        addBinding(index, codecs.encode(value, client.getContext()));
        return this;
    }

    @Override
    public final MySqlStatement bind(String name, Object value) {
        requireNonNull(name, "name must not be null");
        requireNonNull(value, "value must not be null");

        addBinding(getIndexes(name), codecs.encode(value, client.getContext()));
        return this;
    }

    @Override
    public final MySqlStatement bindNull(int index, Class<?> type) {
        // Useless, but should be checked in here, for programming robustness
        requireNonNull(type, "type must not be null");

        addBinding(index, codecs.encodeNull());
        return this;
    }

    @Override
    public final MySqlStatement bindNull(String name, Class<?> type) {
        requireNonNull(name, "name must not be null");
        // Useless, but should be checked in here, for programming robustness
        requireNonNull(type, "type must not be null");

        addBinding(getIndexes(name), codecs.encodeNull());
        return this;
    }

    @Override
    public final Flux<? extends MySqlResult> execute() {
        if (bindings.bindings.isEmpty()) {
            throw new IllegalStateException("No parameters bound for current statement");
        }
        bindings.validatedFinish();

        return Flux.defer(() -> {
            if (!executed.compareAndSet(false, true)) {
                return Flux.error(new IllegalStateException("Parameterized statement was already executed"));
            }

            return execute(bindings.bindings);
        });
    }

    protected abstract Flux<? extends MySqlResult> execute(List<Binding> bindings);

    /**
     * Get parameter index(es) by parameter name.
     *
     * @param name the parameter name
     * @return the {@link ParameterIndex} including an index or multi-indexes
     * @throws IllegalArgumentException if parameter {@code name} not found
     */
    private ParameterIndex getIndexes(String name) {
        ParameterIndex index = query.getNamedIndexes().get(name);

        if (index == null) {
            throw new NoSuchElementException("No such parameter with name: " + name);
        }

        return index;
    }

    private void addBinding(int index, MySqlParameter value) {
        assertNotExecuted();

        this.bindings.getCurrent().add(index, value);
    }

    private void addBinding(ParameterIndex indexes, MySqlParameter value) {
        assertNotExecuted();

        indexes.bind(this.bindings.getCurrent(), value);
    }

    private void assertNotExecuted() {
        if (this.executed.get()) {
            throw new IllegalStateException("Statement was already executed");
        }
    }

    private static final class Bindings implements Iterable<Binding> {

        private final List<Binding> bindings = new ArrayList<>();

        private final int paramCount;

        private Binding current;

        private Bindings(int paramCount) {
            this.paramCount = paramCount;
        }

        @Override
        public Iterator<Binding> iterator() {
            return bindings.iterator();
        }

        @Override
        public void forEach(Consumer<? super Binding> action) {
            bindings.forEach(action);
        }

        @Override
        public Spliterator<Binding> spliterator() {
            return bindings.spliterator();
        }

        private void validatedFinish() {
            Binding current = this.current;

            if (current == null) {
                throw new IllegalStateException("Not all parameter values are provided yet.");
            }

            int unbind = current.findUnbind();

            if (unbind >= 0) {
                String message = String.format("Parameter %d has no binding", unbind);
                throw new IllegalStateException(message);
            }

            this.current = null;
        }

        private Binding getCurrent() {
            Binding current = this.current;

            if (current == null) {
                current = new Binding(this.paramCount);
                this.current = current;
                this.bindings.add(current);
            }

            return current;
        }
    }
}
