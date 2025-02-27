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

package io.geewit.persistence.r2dbc.mysql.message.server;

import io.geewit.persistence.r2dbc.mysql.internal.util.VarIntUtils;
import io.geewit.persistence.r2dbc.mysql.message.FieldValue;
import io.geewit.persistence.r2dbc.mysql.message.NormalFieldValue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.require;

/**
 * An implementation of {@link FieldReader} considers field data, which byte width/size is less than or equal
 * to {@link Integer#MAX_VALUE}.
 */
final class NormalFieldReader implements FieldReader {

    private final ByteBuf buf;

    NormalFieldReader(ByteBuf buf) {
        this.buf = buf;
    }

    @Override
    public short getUnsignedByte() {
        return buf.getUnsignedByte(buf.readerIndex());
    }

    @Override
    public void skipOneByte() {
        buf.skipBytes(Byte.BYTES);
    }

    @Override
    public byte[] readSizeFixedBytes(int length) {
        require(length > 0, "length must be a positive integer");

        return ByteBufUtil.getBytes(buf.readSlice(length));
    }

    @Override
    public FieldValue readSizeFixedField(int length) {
        require(length > 0, "length must be a positive integer");

        return new NormalFieldValue(buf.readRetainedSlice(length));
    }

    @Override
    public FieldValue readVarIntSizedField() {
        return new NormalFieldValue(readVarIntSizedRetained(buf));
    }

    @Override
    public int refCnt() {
        return buf.refCnt();
    }

    @Override
    public NormalFieldReader retain() {
        buf.retain();
        return this;
    }

    @Override
    public NormalFieldReader retain(int increment) {
        buf.retain(increment);
        return this;
    }

    @Override
    public NormalFieldReader touch() {
        buf.touch();
        return this;
    }

    @Override
    public NormalFieldReader touch(Object hint) {
        buf.touch(hint);
        return this;
    }

    @Override
    public boolean release() {
        return buf.release();
    }

    @Override
    public boolean release(int decrement) {
        return buf.release(decrement);
    }

    private static ByteBuf readVarIntSizedRetained(ByteBuf buf) {
        // Normal field will NEVER be greater than Integer.MAX_VALUE.
        int size = (int) VarIntUtils.readVarInt(buf);
        if (size == 0) {
            // Use EmptyByteBuf, new buffer no need to be retained.
            return buf.alloc().buffer(0, 0);
        }

        return buf.readRetainedSlice(size);
    }
}
