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

import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.TransactionDefinition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link TransactionDefinition} for MySQL transactions.
 * <p>
 * Note: The lock wait timeout is only available in InnoDB, and only supports seconds, which must be between 1
 * and 1073741824.
 *
 * @since 0.9.0
 * @deprecated since 1.1.3, use {@link io.geewit.persistence.r2dbc.mysql.api.MySqlTransactionDefinition} instead.
 */
@Deprecated
public final class MySqlTransactionDefinition implements TransactionDefinition {

    /**
     * Use {@code WITH CONSISTENT SNAPSHOT} property.
     * <p>
     * The option starts a consistent read for storage engines such as InnoDB and XtraDB that can do so, the
     * same as if a {@code START TRANSACTION} followed by a {@code SELECT ...} from any InnoDB table was
     * issued.
     */
    public static final Option<Boolean> WITH_CONSISTENT_SNAPSHOT =
        io.geewit.persistence.r2dbc.mysql.api.MySqlTransactionDefinition.WITH_CONSISTENT_SNAPSHOT;

    /**
     * Use {@code WITH CONSISTENT [engine] SNAPSHOT} for Facebook/MySQL or similar property. Only available
     * when {@link #WITH_CONSISTENT_SNAPSHOT} is set to {@code true}.
     * <p>
     * Note: This is an extended syntax based on specific distributions. Please check whether the server
     * supports this property before using it.
     */
    public static final Option<?> CONSISTENT_SNAPSHOT_ENGINE =
        io.geewit.persistence.r2dbc.mysql.api.MySqlTransactionDefinition.CONSISTENT_SNAPSHOT_ENGINE;

    /**
     * Use {@code WITH CONSISTENT SNAPSHOT FROM SESSION [session_id]} for Percona/MySQL or similar property.
     * Only available when {@link #WITH_CONSISTENT_SNAPSHOT} is set to {@code true}.
     * <p>
     * The {@code session_id} is received by {@code SHOW COLUMNS FROM performance_schema.processlist}, it
     * should be an unsigned 64-bit integer. Use {@code SHOW PROCESSLIST} to find session identifier of the
     * process list.
     * <p>
     * Note: This is an extended syntax based on specific distributions. Please check whether the server
     * supports this property before using it.
     */
    public static final Option<Long> CONSISTENT_SNAPSHOT_FROM_SESSION =
        io.geewit.persistence.r2dbc.mysql.api.MySqlTransactionDefinition.CONSISTENT_SNAPSHOT_FROM_SESSION;

    private static final MySqlTransactionDefinition EMPTY =
        new MySqlTransactionDefinition(Collections.emptyMap());

    private final Map<Option<?>, Object> options;

    private MySqlTransactionDefinition(Map<Option<?>, Object> options) {
        this.options = options;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getAttribute(Option<T> option) {
        return (T) this.options.get(option);
    }

    /**
     * Returns a builder to mutate options of this definition by creating a new instance and returning either
     * mutated values or old values.
     *
     * @return the builder with old values.
     */
    public Builder mutate() {
        return new Builder(new HashMap<>(this.options));
    }

    /**
     * Defines an empty transaction. i.e. the regular transaction.
     *
     * @return the empty transaction definition.
     */
    public static MySqlTransactionDefinition empty() {
        return EMPTY;
    }

    /**
     * Creates a builder without any value.
     *
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder(new HashMap<>());
    }

    /**
     * A builder considers to create {@link TransactionDefinition}.
     */
    public static final class Builder {

        private final Map<Option<?>, Object> options;

        /**
         * Builds a transaction definition with current values.
         *
         * @return the transaction definition.
         */
        public MySqlTransactionDefinition build() {
            return switch (this.options.size()) {
                case 0 -> EMPTY;
                case 1 -> {
                    Map.Entry<Option<?>, Object> entry = this.options.entrySet().iterator().next();

                    yield new MySqlTransactionDefinition(Collections.singletonMap(entry.getKey(),
                            entry.getValue()));
                }
                default -> new MySqlTransactionDefinition(new HashMap<>(this.options));
            };
        }

        /**
         * Changes the {@link #ISOLATION_LEVEL} option.
         *
         * @param isolationLevel the level which change to, or {@code null} to remove old value.
         * @return this builder.
         */
        public Builder isolationLevel(IsolationLevel isolationLevel) {
            return option(ISOLATION_LEVEL, isolationLevel);
        }

        /**
         * Changes the {@link #LOCK_WAIT_TIMEOUT} option.
         *
         * @param lockWaitTimeout the timeout which change to, or {@code null} to remove old value.
         * @return this builder.
         */
        public Builder lockWaitTimeout(Duration lockWaitTimeout) {
            return option(LOCK_WAIT_TIMEOUT, lockWaitTimeout);
        }

        /**
         * Changes the {@link #READ_ONLY} option.
         *
         * @param readOnly if enable read only, or {@code null} to remove old value.
         * @return this builder.
         */
        public Builder readOnly(Boolean readOnly) {
            return option(READ_ONLY, readOnly);
        }

        /**
         * Changes the {@link #WITH_CONSISTENT_SNAPSHOT} option.  Notice that this phrase can only be used
         * with the {@code REPEATABLE READ} isolation level.
         *
         * @param withConsistentSnapshot if enable consistent snapshot, or {@code null} to remove old value.
         * @return this builder.
         */
        public Builder withConsistentSnapshot(Boolean withConsistentSnapshot) {
            return option(WITH_CONSISTENT_SNAPSHOT, withConsistentSnapshot);
        }

        /**
         * Changes the {@link #CONSISTENT_SNAPSHOT_ENGINE} option.
         *
         * @param snapshotEngine the engine which change to, or {@code null} to remove old value.
         * @return this builder.
         */
        public Builder consistentSnapshotEngine(ConsistentSnapshotEngine snapshotEngine) {
            return option(CONSISTENT_SNAPSHOT_ENGINE, snapshotEngine == null ? null : snapshotEngine.asSql());
        }

        /**
         * Changes the {@link #CONSISTENT_SNAPSHOT_FROM_SESSION} option.
         *
         * @param sessionId the session id which change to, or {@code null} to remove old value.
         * @return this builder.
         */
        public Builder consistentSnapshotFromSession(Long sessionId) {
            return option(CONSISTENT_SNAPSHOT_FROM_SESSION, sessionId);
        }

        private Builder option(Option<?> key, Object value) {
            if (value == null) {
                this.options.remove(key);
            } else {
                this.options.put(key, value);
            }

            return this;
        }

        private Builder(Map<Option<?>, Object> options) {
            this.options = options;
        }
    }
}
