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

import io.geewit.persistence.r2dbc.mysql.ConnectionContext;
import io.geewit.persistence.r2dbc.mysql.constant.Packets;
import io.geewit.persistence.r2dbc.mysql.internal.util.NettyBufferUtils;
import io.geewit.persistence.r2dbc.mysql.internal.util.VarIntUtils;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;

import java.util.ArrayList;
import java.util.List;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * Generic message decoder logic.
 */
public final class ServerMessageDecoder {

    private static final short OK = 0;

    private static final short AUTH_MORE_DATA = 1;

    private static final short HANDSHAKE_V9 = 9;

    private static final short HANDSHAKE_V10 = 10;

    private static final short EOF = 0xFE;

    private static final short ERROR = 0xFF;

    /**
     * Note: it can be a column count message, so the packet size should be checked.
     */
    private static final short LOCAL_INFILE = 0xFB;

    private final List<ByteBuf> parts = new ArrayList<>();

    /**
     * Decode a server-side message from {@link #parts} and current envelope.
     *
     * @param payload       the payload of the current packet.
     * @param context       the connection context.
     * @param decodeContext the decode context.
     * @return the server-side message, or {@code null} if {@code envelope} is not last packet.
     */
    public ServerMessage decode(ByteBuf payload, ConnectionContext context, DecodeContext decodeContext) {
        requireNonNull(payload, "payload must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(decodeContext, "decodeContext must not be null");

        parts.add(payload);

        if (payload.readableBytes() == Packets.MAX_PAYLOAD_SIZE) {
            // Not last packet.
            return null;
        }

        return decodeMessage(parts, context, decodeContext);
    }

    /**
     * Dispose the underlying resource.
     */
    public void dispose() {
        if (parts.isEmpty()) {
            return;
        }

        NettyBufferUtils.releaseAll(parts);
        parts.clear();
    }

    private static ServerMessage decodeMessage(List<ByteBuf> buffers,
                                               ConnectionContext context,
                                               DecodeContext decodeContext) {
        switch (decodeContext) {
            case ResultDecodeContext resultDecodeContext -> {
                return decodeResult(buffers, context, resultDecodeContext);
            }
            case FetchDecodeContext _ -> {
                return decodeFetch(buffers, context);
            }
            case null, default -> {
            }
        }

        ByteBuf combined = NettyBufferUtils.composite(buffers);

        try {
            switch (decodeContext) {
                case CommandDecodeContext _ -> {
                    return decodeCommandMessage(combined, context);
                }
                case PreparedMetadataDecodeContext preparedMetadataDecodeContext -> {
                    return decodePreparedMetadata(combined, context, preparedMetadataDecodeContext);
                }
                case PrepareQueryDecodeContext _ -> {
                    return decodePrepareQuery(combined);
                }
                case LoginDecodeContext _ -> {
                    return decodeLogin(combined, context);
                }
                case null, default -> {
                }
            }
        } finally {
            combined.release();
        }

        throw new IllegalStateException("unknown decode context type: " + (decodeContext != null ? decodeContext.getClass() : null));
    }

    private static ServerMessage decodePreparedMetadata(ByteBuf buf,
                                                        ConnectionContext context,
                                                        PreparedMetadataDecodeContext decodeContext) {
        short header = buf.getUnsignedByte(buf.readerIndex());

        if (header == ERROR) {
            // 0xFF is not header of var integer,
            // not header of text result null (0xFB) and
            // not header of column metadata (0x03 + "def")
            return ErrorMessage.decode(buf);
        }

        if (decodeContext.isInMetadata()) {
            return decodeInMetadata(buf, header, context, decodeContext);
        }

        throw new R2dbcNonTransientResourceException("Unknown message header 0x" +
                Integer.toHexString(header) + " and readable bytes is " + buf.readableBytes() +
                " on prepared metadata phase");
    }

    private static ServerMessage decodeFetch(List<ByteBuf> buffers,
                                             ConnectionContext context) {
        ByteBuf firstBuf = buffers.getFirst();
        short header = firstBuf.getUnsignedByte(firstBuf.readerIndex());
        ErrorMessage error = decodeCheckError(buffers, header);

        if (error != null) {
            return error;
        }

        return decodeRow(buffers, firstBuf, header, context, "fetch");
    }

    private static ServerMessage decodeResult(List<ByteBuf> buffers,
                                              ConnectionContext context,
                                              ResultDecodeContext decodeContext) {
        ByteBuf firstBuf = buffers.getFirst();
        short header = firstBuf.getUnsignedByte(firstBuf.readerIndex());
        ErrorMessage error = decodeCheckError(buffers, header);

        if (error != null) {
            return error;
        }

        if (decodeContext.isInMetadata()) {
            ByteBuf combined = NettyBufferUtils.composite(buffers);
            try {
                return decodeInMetadata(combined, header, context, decodeContext);
            } finally {
                combined.release();
            }
            // Should not have other messages when metadata reading.
        }

        return decodeRow(buffers, firstBuf, header, context, "result");
    }

    private static ServerMessage decodePrepareQuery(ByteBuf buf) {
        short header = buf.getUnsignedByte(buf.readerIndex());
        switch (header) {
            case ERROR:
                return ErrorMessage.decode(buf);
            case OK:
                if (PreparedOkMessage.isLooksLike(buf)) {
                    return PreparedOkMessage.decode(buf);
                }
                break;
        }

        throw new R2dbcNonTransientResourceException("Unknown message header 0x" +
                Integer.toHexString(header) + " and readable bytes is " + buf.readableBytes() +
                " on prepare query phase");
    }

    private static ServerMessage decodeCommandMessage(ByteBuf buf, ConnectionContext context) {
        short header = buf.getUnsignedByte(buf.readerIndex());
        switch (header) {
            case ERROR -> {
                return ErrorMessage.decode(buf);
            }
            case OK -> {
                if (OkMessage.isValidSize(buf.readableBytes())) {
                    return OkMessage.decode(false, buf, context);
                }
            }
            case EOF -> {
                int byteSize = buf.readableBytes();

                // Maybe OK, maybe column count (unsupported EOF on command phase)
                if (OkMessage.isValidSize(byteSize)) {
                    // MySQL has hard limited of 4096 columns per-table,
                    // so if readable bytes is greater than 7, it means if it is column count,
                    // column count is already greater than (1 << 24) - 1 = 16777215, it is impossible.
                    // So it must be OK message, not be column count.
                    return OkMessage.decode(false, buf, context);
                } else if (EofMessage.isValidSize(byteSize)) {
                    return EofMessage.decode(buf);
                }
            }
            case LOCAL_INFILE -> {
                if (buf.readableBytes() > 1) {
                    return LocalInfileRequest.decode(buf, context);
                }
            }
            default -> {
            }
        }
        if (VarIntUtils.checkNextVarInt(buf) == 0) {
            // EOF message must be 5-bytes, it will never be looks like a var integer.
            // It looks like has only a var integer, should be column count.
            return ColumnCountMessage.decode(buf);
        }

        throw new R2dbcNonTransientResourceException("Unknown message header 0x" +
                Integer.toHexString(header) + " and readable bytes is " + buf.readableBytes() +
                " on command phase");
    }

    private static ServerMessage decodeLogin(ByteBuf buf, ConnectionContext context) {
        short header = buf.getUnsignedByte(buf.readerIndex());
        switch (header) {
            case OK -> {
                if (OkMessage.isValidSize(buf.readableBytes())) {
                    return OkMessage.decode(false, buf, context);
                }
            }
            case AUTH_MORE_DATA -> {
                return AuthMoreDataMessage.decode(buf); // Auth more data
            }
            case HANDSHAKE_V9, HANDSHAKE_V10 -> {
                return HandshakeRequest.decode(buf); // Handshake V9 (not supported) or V10
            }
            case ERROR -> {
                return ErrorMessage.decode(buf); // Error
            }
            case EOF -> {
                if (EofMessage.isValidSize(buf.readableBytes())) {
                    return EofMessage.decode(buf);
                }
                return ChangeAuthMessage.decode(buf);
            }
        }
        throw new R2dbcPermissionDeniedException("Unknown message header 0x" +
                Integer.toHexString(header) + " and readable bytes is " + buf.readableBytes() +
                " on connection phase");
    }

    private static boolean isRow(List<ByteBuf> buffers, ByteBuf firstBuf, short header) {
        switch (header) {
            case RowMessage.NULL_VALUE -> {
                // NULL_VALUE (0xFB) is not header of var integer and not header of OK (0x0 or 0xFE)
                return true;
                // NULL_VALUE (0xFB) is not header of var integer and not header of OK (0x0 or 0xFE)
            }
            case EOF -> {
                // 0xFE means it maybe EOF, or var int (64-bits) header in text row.
                if (buffers.size() > 1) {
                    // Multi-buffers, must be big data row message.
                    return true;
                }

                // Not EOF or OK.
                int size = firstBuf.readableBytes();
                return !EofMessage.isValidSize(size) && !OkMessage.isValidSize(size);
            }
            default -> {
                // If header is 0, SHOULD NOT be OK message.
                // Because MySQL sends OK messages always starting with 0xFE in SELECT statement result.
                // Now, it is not OK message, not be error message, it must be row.
                return true;
            }
        }
    }

    private static ErrorMessage decodeCheckError(List<ByteBuf> buffers, short header) {
        if (ERROR == header) {
            // 0xFF is not header of var integer,
            // not header of text result null (0xFB) and
            // not header of column metadata (0x03 + "def")
            ByteBuf combined = NettyBufferUtils.composite(buffers);
            try {
                return ErrorMessage.decode(combined);
            } finally {
                combined.release();
            }
        }

        return null;
    }

    private static ServerMessage decodeRow(List<ByteBuf> buffers,
                                           ByteBuf firstBuf,
                                           short header,
                                           ConnectionContext context,
                                           String phase) {
        if (isRow(buffers, firstBuf, header)) {
            // FieldReader will clear the buffers.
            return new RowMessage(FieldReader.of(buffers));
        } else if (header == EOF) {
            int byteSize = firstBuf.readableBytes();

            if (OkMessage.isValidSize(byteSize)) {
                ByteBuf combined = NettyBufferUtils.composite(buffers);

                try {
                    return OkMessage.decode(true, combined, context);
                } finally {
                    combined.release();
                }
            } else if (EofMessage.isValidSize(byteSize)) {
                ByteBuf combined = NettyBufferUtils.composite(buffers);

                try {
                    return EofMessage.decode(combined);
                } finally {
                    combined.release();
                }
            }
        }

        long totalBytes = 0;
        try {
            for (ByteBuf buffer : buffers) {
                if (buffer != null) {
                    totalBytes += buffer.readableBytes();
                    ReferenceCountUtil.safeRelease(buffer);
                }
            }
        } finally {
            buffers.clear();
        }

        throw new R2dbcNonTransientResourceException("Unknown message header 0x" +
                Integer.toHexString(header) + " and readable bytes is " + totalBytes + " on " + phase + " phase");
    }

    private static SyntheticMetadataMessage decodeInMetadata(ByteBuf buf,
                                                             short header,
                                                             ConnectionContext context,
                                                             MetadataDecodeContext decodeContext) {
        ServerMessage message;
        if (EOF == header && EofMessage.isValidSize(buf.readableBytes())) {
            message = EofMessage.decode(buf);
        } else {
            message = DefinitionMetadataMessage.decode(buf, context);
        }

        if (message instanceof ServerStatusMessage serverStatusMessage) {
            context.setServerStatuses(serverStatusMessage.getServerStatuses());
        }

        return decodeContext.putPart(message);
    }
}
