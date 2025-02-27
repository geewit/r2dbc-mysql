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

import io.geewit.persistence.r2dbc.mysql.api.MySqlConnection;
import io.geewit.persistence.r2dbc.mysql.cache.Caches;
import io.geewit.persistence.r2dbc.mysql.cache.QueryCache;
import io.geewit.persistence.r2dbc.mysql.client.Client;
import io.geewit.persistence.r2dbc.mysql.internal.util.StringUtils;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link ConnectionFactory} for creating connections to a MySQL database.
 */
public final class MySqlConnectionFactory implements ConnectionFactory {

    private final Mono<? extends MySqlConnection> client;

    private MySqlConnectionFactory(Mono<? extends MySqlConnection> client) {
        this.client = client;
    }

    @Override
    public Mono<? extends MySqlConnection> create() {
        return client;
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return MySqlConnectionFactoryMetadata.INSTANCE;
    }

    /**
     * Creates a {@link MySqlConnectionFactory} with a {@link MySqlConnectionConfiguration}.
     *
     * @param configuration the {@link MySqlConnectionConfiguration}.
     * @return configured {@link MySqlConnectionFactory}.
     */
    public static MySqlConnectionFactory from(MySqlConnectionConfiguration configuration) {
        requireNonNull(configuration, "configuration must not be null");

        LazyQueryCache queryCache = new LazyQueryCache(configuration.getQueryCacheSize());

        return new MySqlConnectionFactory(Mono.defer(() -> {
            MySqlSslConfiguration ssl;
            SocketAddress address;

            // 根据配置判断是否采用 SSL（或者开启 TCP 模式），同时在 Windows 下只能使用 TCP 连接
            if (configuration.isHost()) {
                ssl = configuration.getSsl();
            } else {
                ssl = MySqlSslConfiguration.disabled();
            }
            // 统一采用 InetSocketAddress 来创建地址，兼容 Windows 环境
            address = InetSocketAddress.createUnresolved(configuration.getDomain(), configuration.getPort());

            String user = configuration.getUser();
            CharSequence password = configuration.getPassword();
            Publisher<String> passwordPublisher = configuration.getPasswordPublisher();

            if (Objects.nonNull(passwordPublisher)) {
                return Mono.from(passwordPublisher).flatMap(token -> getMySqlConnection(
                        configuration, ssl,
                        queryCache,
                        address,
                        user,
                        token
                ));
            }

            return getMySqlConnection(
                    configuration, ssl,
                    queryCache,
                    address,
                    user,
                    password
            );
        }));
    }

    /**
     * Gets an initialized {@link MySqlConnection} from authentication credential and configurations.
     * (后续代码保持不变)
     */
    private static Mono<MySqlConnection> getMySqlConnection(
            final MySqlConnectionConfiguration configuration,
            final MySqlSslConfiguration ssl,
            final LazyQueryCache queryCache,
            final SocketAddress address,
            final String user,
            final CharSequence password
    ) {
        return Mono.fromSupplier(() -> {
            ZoneId connectionTimeZone = retrieveZoneId(configuration.getConnectionTimeZone());
            return new ConnectionContext(
                    configuration.getZeroDateOption(),
                    configuration.getLoadLocalInfilePath(),
                    configuration.getLocalInfileBufferSize(),
                    configuration.isPreserveInstants(),
                    connectionTimeZone
            );
        }).flatMap(context -> Client.connect(
                ssl,
                address,
                configuration.isTcpKeepAlive(),
                configuration.isTcpNoDelay(),
                context,
                configuration.getConnectTimeout(),
                configuration.getLoopResources(),
                configuration.getResolver(),
                configuration.isMetrics()
        )).flatMap(client -> {
            // Lazy init database after handshake/login
            boolean deferDatabase = configuration.isCreateDatabaseIfNotExist();
            String database = configuration.getDatabase();
            String loginDb = deferDatabase ? "" : database;
            String sessionDb = deferDatabase ? database : "";

            return InitFlow.initHandshake(
                    client,
                    ssl.getSslMode(),
                    loginDb,
                    user,
                    password,
                    configuration.getCompressionAlgorithms(),
                    configuration.getZstdCompressionLevel()
            ).then(InitFlow.initSession(
                    client,
                    sessionDb,
                    configuration.getPrepareCacheSize(),
                    configuration.getSessionVariables(),
                    configuration.isForceConnectionTimeZoneToSession(),
                    configuration.getLockWaitTimeout(),
                    configuration.getStatementTimeout(),
                    configuration.getExtensions()
            )).map(codecs -> new MySqlSimpleConnection(
                    client,
                    codecs,
                    queryCache.get(),
                    configuration.getPreferPrepareStatement()
            )).onErrorResume(e -> client.forceClose().then(Mono.error(e)));
        });
    }

    private static ZoneId retrieveZoneId(String timeZone) {
        if ("LOCAL".equalsIgnoreCase(timeZone)) {
            return ZoneId.systemDefault().normalized();
        } else if ("SERVER".equalsIgnoreCase(timeZone)) {
            return null;
        }
        return StringUtils.parseZoneId(timeZone);
    }

    private static final class LazyQueryCache implements Supplier<QueryCache> {

        private final int capacity;
        private final ReentrantLock lock = new ReentrantLock();
        private volatile QueryCache cache;

        private LazyQueryCache(int capacity) {
            this.capacity = capacity;
        }

        @Override
        public QueryCache get() {
            QueryCache cache = this.cache;
            if (cache == null) {
                lock.lock();
                try {
                    if ((cache = this.cache) == null) {
                        this.cache = cache = Caches.createQueryCache(capacity);
                    }
                    return cache;
                } finally {
                    lock.unlock();
                }
            }
            return cache;
        }
    }
}
