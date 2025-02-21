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

// 移除已弃用的导入
import io.geewit.persistence.r2dbc.mysql.extension.CodecRegistrar;
import io.geewit.persistence.r2dbc.mysql.extension.Extension;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Consumer;

final class Extensions {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(Extensions.class);

    private static final Extension[] EMPTY = { };

    private final Extension[] extensions;

    private Extensions(List<Extension> extensions) {
        this.extensions = toArray(extensions);
    }

    /**
     * Apply {@link Consumer consumer} for each {@link Extension} of the requested type {@code type} until all
     * {@link Extension extensions} have been processed or the action throws an exception. Actions are
     * performed in the order of iteration, if that order is specified. Exceptions thrown by the action are
     * relayed to the caller.
     *
     * @param <T>      extension type
     * @param consumer the {@link Consumer} to notify for each instance of {@code type}
     */
    @SuppressWarnings("unchecked")
    <T extends CodecRegistrar> void forEach(Consumer<? super T> consumer) {
        for (Extension extension : this.extensions) {
            if (extension instanceof CodecRegistrar) {
                // 直接强制转换对象而非Class类型，并通过@SuppressWarnings消除警告
                consumer.accept((T) extension);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Extensions that = (Extensions) o;
        return Arrays.equals(extensions, that.extensions);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(extensions);
    }

    @Override
    public String toString() {
        // Extensions[AbcExtension{...}, DefExtension{...}, ...]
        return "Extensions" + Arrays.toString(extensions);
    }

    /**
     * Creates a new {@link Extensions} object.
     *
     * @param extensions the extensions to hold.
     * @param autodetect use autodetect or not.
     * @return a new {@link Extensions} object.
     */
    static Extensions from(List<Extension> extensions, boolean autodetect) {
        if (autodetect) {
            return new Extensions(autodetect(new ArrayList<>(extensions)));
        }

        return new Extensions(extensions);
    }

    /**
     * Autodetect extensions using {@link ServiceLoader} mechanism.
     */
    private static List<Extension> autodetect(List<Extension> discovered) {
        logger.debug("Discovering Extensions using ServiceLoader");

        // 直接使用ServiceLoader加载，移除PrivilegedAction
        ServiceLoader<Extension> extensions = ServiceLoader.load(
                Extension.class, Extensions.class.getClassLoader()
        );

        for (Extension extension : extensions) {
            logger.debug("Registering extension {}", extension.getClass().getName());
            discovered.add(extension);
        }

        return discovered;
    }

    private static Extension[] toArray(List<Extension> extensions) {
        if (extensions.isEmpty()) {
            return EMPTY;
        }
        return extensions.toArray(new Extension[0]);
    }
}
