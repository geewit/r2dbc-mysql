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
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.Objects;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * Mixin {@link CharsetTarget} for select the optimal {@link Charset} in multiple {@link CharsetTarget}s.
 */
final class MixCharsetTarget extends AbstractCharsetTarget {

    private final Charset fallbackCharset;

    private final CharsetTarget[] targets;

    MixCharsetTarget(int byteSize, CharsetTarget... targets) {
        this(byteSize, null, targets);
    }

    MixCharsetTarget(int byteSize, Charset fallbackCharset, CharsetTarget... targets) {
        super(maxByteSize(requireNonNull(targets, "targets must not be null"), byteSize));

        this.fallbackCharset = fallbackCharset;
        this.targets = targets;
    }

    @Override
    public Charset getCharset() {
        return fallbackCharset == null ? getCharsetFallible() : getCharsetNonFail(fallbackCharset);
    }

    @Override
    public boolean isCached() {
        return false;
    }

    private Charset getCharsetFallible() {
        IllegalArgumentException err = null;

        for (CharsetTarget target : this.targets) {
            try {
                return target.getCharset();
            } catch (IllegalArgumentException e) {
                // UnsupportedCharsetException is subclass of IllegalArgumentException
                if (err != null) {
                    e.addSuppressed(err);
                }

                err = e;
            }
        }

        if (err == null) {
            throw new UnsupportedCharsetException("Charset target not found in MixCharsetTarget");
        }

        throw err;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MixCharsetTarget that)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        return Objects.equals(fallbackCharset, that.fallbackCharset) && Arrays.equals(targets, that.targets);
    }

    @Override
    public int hashCode() {
        int hash = 31 * super.hashCode() + (fallbackCharset != null ? fallbackCharset.hashCode() : 0);
        return 31 * hash + Arrays.hashCode(targets);
    }

    @Override
    public String toString() {
        return String.format("MixCharsetTarget{fallbackCharset=%s, targets=%s, byteSize=%d}", fallbackCharset,
            Arrays.toString(targets), byteSize);
    }

    private Charset getCharsetNonFail(Charset fallback) {
        for (CharsetTarget target : this.targets) {
            try {
                return target.getCharset();
            } catch (IllegalArgumentException ignored) {
                // UnsupportedCharsetException is subclass of IllegalArgumentException
                // Charset not support, just ignore
            }
        }

        return fallback;
    }

    private static int maxByteSize(CharsetTarget[] targets, int defaultByteSize) {
        int result = defaultByteSize;

        for (CharsetTarget target : targets) {
            int byteSize = target.getByteSize();
            if (byteSize > result) {
                result = byteSize;
            }
        }

        return result;
    }
}
