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

package io.geewit.persistence.r2dbc.mysql.collation;

import java.nio.charset.Charset;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link CharsetTarget} that has cached charset.
 */
final class CachedCharsetTarget extends AbstractCharsetTarget {

    private final Charset charset;

    CachedCharsetTarget(int byteSize, Charset charset) {
        super(byteSize);

        this.charset = requireNonNull(charset, "charset must not be null");
    }

    @Override
    public Charset getCharset() {
        return charset;
    }

    @Override
    public boolean isCached() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CachedCharsetTarget that)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        return charset.equals(that.charset);
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        return 31 * hash + charset.hashCode();
    }

    @Override
    public String toString() {
        return "CachedCharsetTarget{charset=" + charset + ", byteSize=" + byteSize + '}';
    }
}
