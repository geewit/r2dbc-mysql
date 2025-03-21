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

package io.geewit.persistence.r2dbc.mysql.message;

import io.geewit.persistence.r2dbc.mysql.internal.util.InternalArrays;
import io.geewit.persistence.r2dbc.mysql.internal.util.NettyBufferUtils;
import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

import java.util.List;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link FieldValue} considers large field value which bytes width/size is greater than
 * {@link Integer#MAX_VALUE}, it is used by the MySQL server returns LOB types (i.e. BLOB, CLOB), e.g.
 * LONGTEXT length can be unsigned int32.
 *
 * @see FieldValue
 */
public final class LargeFieldValue extends AbstractReferenceCounted implements FieldValue {

    private final List<ByteBuf> buffers;

    public LargeFieldValue(List<ByteBuf> buffers) {
        this.buffers = requireNonNull(buffers, "buffers must not be null");
    }

    public List<ByteBuf> getBufferSlices() {
        int size = this.buffers.size();
        ByteBuf[] buffers = new ByteBuf[size];

        for (int i = 0; i < size; ++i) {
            buffers[i] = this.buffers.get(i).slice();
        }

        return InternalArrays.asImmutableList(buffers);
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        if (this.buffers.isEmpty()) {
            return this;
        }

        for (ByteBuf buf : this.buffers) {
            buf.touch(hint);
        }

        return this;
    }

    @Override
    protected void deallocate() {
        if (this.buffers.isEmpty()) {
            return;
        }

        NettyBufferUtils.releaseAll(this.buffers);
    }
}
