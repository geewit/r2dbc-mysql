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
import io.geewit.persistence.r2dbc.mysql.internal.util.StringUtils;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.List;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.require;

/**
 * An implementations of {@link SimpleStatementSupport} based on MySQL prepare query.
 */
final class PrepareSimpleStatement extends SimpleStatementSupport {

    private static final List<Binding> BINDINGS = Collections.singletonList(new Binding(0));

    private int fetchSize = 0;

    PrepareSimpleStatement(Client client, Codecs codecs, String sql) {
        super(client, codecs, sql);
    }

    @Override
    public Flux<MySqlResult> execute() {
        return Flux.defer(() -> QueryFlow.execute(client,
                StringUtils.extendReturning(sql, returningIdentifiers()), BINDINGS, fetchSize))
            .map(messages -> MySqlSegmentResult.toResult(true, client, codecs, syntheticKeyName(), messages));
    }

    @Override
    public MySqlStatement fetchSize(int rows) {
        require(rows >= 0, "Fetch size must be greater or equal to zero");

        this.fetchSize = rows;
        return this;
    }
}
