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

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * Base class considers {@link DefinitionMetadataMessage} for {@link DecodeContext} implementations.
 */
abstract class MetadataDecodeContext implements DecodeContext {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MetadataDecodeContext.class);

    private final boolean eofDeprecated;

    MetadataDecodeContext(boolean eofDeprecated) {
        this.eofDeprecated = eofDeprecated;
    }

    abstract boolean isInMetadata();

    final SyntheticMetadataMessage putPart(ServerMessage message) {
        if (message instanceof DefinitionMetadataMessage) {
            // Index of metadata after put, see `putMetadata`.
            int index = putMetadata((DefinitionMetadataMessage) message);

            if (eofDeprecated) {
                // If EOF has deprecated, has no EOF for complete signal, should check complete always.
                SyntheticMetadataMessage bundle = checkComplete(index, null);

                if (bundle != null) {
                    logger.debug("Respond a metadata bundle by filled-up");
                }

                return bundle;
            }

            // Should not check complete, EOF message will be complete signal.
            return null;
        } else if (message instanceof EofMessage eofMessage) {
            if (eofDeprecated) {
                throw new IllegalStateException("Unexpected " + message +
                    " because server has deprecated EOF");
            }

            // Current columns index is also last index of metadata after put, see `putMetadata`.
            int currentIndex = currentIndex();
            SyntheticMetadataMessage bundle = checkComplete(currentIndex, eofMessage);

            if (bundle == null) {
                if (logger.isErrorEnabled()) {
                    logger.error("Unexpected {} when metadata unfilled, fill index: {}, checkpoint(s): {}",
                        message, currentIndex, loggingPoints());
                }
            } else {
                logger.debug("Respond a metadata bundle by {}", message);
            }

            return bundle;
        }

        throw new IllegalStateException("Unknown message type " + message.getClass().getSimpleName() +
            " when reading metadata");
    }

    protected abstract SyntheticMetadataMessage checkComplete(int index, EofMessage eof);

    /**
     * Put a column metadata message into this context.
     *
     * @param metadata the column metadata message.
     * @return current index after putting the metadata.
     */
    protected abstract int putMetadata(DefinitionMetadataMessage metadata);

    /**
     * Get the current index, for {@link #checkComplete(int, EofMessage)} when receive a EOF message.
     *
     * @return the current index.
     */
    protected abstract int currentIndex();

    /**
     * Get checkpoints for logging.
     *
     * @return serializable object, like {@link String} or {@link Integer}.
     */
    protected abstract Object loggingPoints();
}
