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

import io.geewit.persistence.r2dbc.mysql.constant.SslMode;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.HostnameVerifier;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

import static io.geewit.persistence.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;
import static io.geewit.persistence.r2dbc.mysql.internal.util.InternalArrays.EMPTY_STRINGS;

/**
 * A configuration of MySQL SSL connection.
 */
public final class MySqlSslConfiguration {

    private static final MySqlSslConfiguration DISABLED = new MySqlSslConfiguration(SslMode.DISABLED,
            EMPTY_STRINGS, null, null, null, null, null, null);

    private final SslMode sslMode;

    private final String[] tlsVersion;

    private final HostnameVerifier sslHostnameVerifier;

    private final String sslCa;

    private final String sslKey;

    private final CharSequence sslKeyPassword;

    private final String sslCert;

    private final Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer;

    private MySqlSslConfiguration(SslMode sslMode,
                                  String[] tlsVersion,
                                  HostnameVerifier sslHostnameVerifier,
                                  String sslCa,
                                  String sslKey,
                                  CharSequence sslKeyPassword,
                                  String sslCert,
                                  Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer) {
        this.sslMode = sslMode;
        this.tlsVersion = tlsVersion;
        this.sslHostnameVerifier = sslHostnameVerifier;
        this.sslCa = sslCa;
        this.sslKey = sslKey;
        this.sslKeyPassword = sslKeyPassword;
        this.sslCert = sslCert;
        this.sslContextBuilderCustomizer = sslContextBuilderCustomizer;
    }

    public SslMode getSslMode() {
        return sslMode;
    }

    public String[] getTlsVersion() {
        return tlsVersion;
    }

    public HostnameVerifier getSslHostnameVerifier() {
        return sslHostnameVerifier;
    }

    public String getSslCa() {
        return sslCa;
    }

    public String getSslKey() {
        return sslKey;
    }

    public CharSequence getSslKeyPassword() {
        return sslKeyPassword;
    }

    public String getSslCert() {
        return sslCert;
    }

    /**
     * Customizes a {@link SslContextBuilder} that customizer was specified by configuration, or do nothing if
     * the customizer was not set.
     *
     * @param builder the {@link SslContextBuilder}.
     * @return the {@code builder}.
     */
    public SslContextBuilder customizeSslContext(SslContextBuilder builder) {
        if (sslContextBuilderCustomizer == null) {
            return builder;
        }

        return sslContextBuilderCustomizer.apply(builder);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof MySqlSslConfiguration that) {
            return sslMode == that.sslMode &&
                    Arrays.equals(tlsVersion, that.tlsVersion) &&
                    Objects.equals(sslHostnameVerifier, that.sslHostnameVerifier) &&
                    Objects.equals(sslCa, that.sslCa) &&
                    Objects.equals(sslKey, that.sslKey) &&
                    Objects.equals(sslKeyPassword, that.sslKeyPassword) &&
                    Objects.equals(sslCert, that.sslCert) &&
                    Objects.equals(sslContextBuilderCustomizer, that.sslContextBuilderCustomizer);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = Objects.hash(sslMode, sslHostnameVerifier, sslCa, sslKey, sslKeyPassword, sslCert,
                sslContextBuilderCustomizer);
        return 31 * hash + Arrays.hashCode(tlsVersion);
    }

    @Override
    public String toString() {
        if (sslMode == SslMode.DISABLED) {
            return "MySqlSslConfiguration{sslMode=DISABLED}";
        }

        return "MySqlSslConfiguration{sslMode=" + sslMode + ", tlsVersion=" + Arrays.toString(tlsVersion) +
                ", sslHostnameVerifier=" + sslHostnameVerifier + ", sslCa='" + sslCa + "', sslKey='" + sslKey +
                "', sslKeyPassword=REDACTED, sslCert='" + sslCert + "', sslContextBuilderCustomizer=" +
                sslContextBuilderCustomizer + '}';
    }

    static MySqlSslConfiguration disabled() {
        return DISABLED;
    }

    static MySqlSslConfiguration create(SslMode sslMode,
                                        String[] tlsVersion,
                                        HostnameVerifier sslHostnameVerifier,
                                        String sslCa,
                                        String sslKey,
                                        CharSequence sslKeyPassword,
                                        String sslCert,
                                        Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer) {
        requireNonNull(sslMode, "sslMode must not be null");

        if (sslMode == SslMode.DISABLED) {
            return DISABLED;
        }

        requireNonNull(tlsVersion, "tlsVersion must not be null");

        return new MySqlSslConfiguration(sslMode, tlsVersion, sslHostnameVerifier, sslCa, sslKey,
                sslKeyPassword, sslCert, sslContextBuilderCustomizer);
    }
}
