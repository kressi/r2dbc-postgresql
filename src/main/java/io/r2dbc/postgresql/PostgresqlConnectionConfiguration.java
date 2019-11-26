/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql;


import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.r2dbc.postgresql.client.DefaultHostnameVerifier;
import io.r2dbc.postgresql.client.MultipleHostsConfiguration;
import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.postgresql.client.SingleHostConfiguration;
import io.r2dbc.postgresql.codec.Codec;
import io.r2dbc.postgresql.extension.CodecRegistrar;
import io.r2dbc.postgresql.extension.Extension;
import io.r2dbc.postgresql.util.Assert;
import reactor.netty.tcp.SslProvider;
import reactor.util.annotation.Nullable;

import javax.net.ssl.HostnameVerifier;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Supplier;

import static reactor.netty.tcp.SslProvider.DefaultConfigurationType.TCP;

/**
 * Connection configuration information for connecting to a PostgreSQL database.
 */
public final class PostgresqlConnectionConfiguration {

    /**
     * Default PostgreSQL port.
     */
    public static final int DEFAULT_PORT = 5432;

    private final String applicationName;

    private final boolean autodetectExtensions;

    private final Duration connectTimeout;

    private final String database;

    private final List<Extension> extensions;

    private final boolean forceBinary;

    private final Map<String, String> options;

    private final CharSequence password;

    private final String schema;


    private final String username;

    private final SSLConfig sslConfig;

    private final MultipleHostsConfiguration multipleHostsConfiguration;

    private final SingleHostConfiguration singleHostConfiguration;

    private PostgresqlConnectionConfiguration(String applicationName, boolean autodetectExtensions,
                                              @Nullable Duration connectTimeout, @Nullable String database, List<Extension> extensions, boolean forceBinary,
                                              @Nullable Map<String, String> options, @Nullable CharSequence password, @Nullable String schema, String username,
                                              SSLConfig sslConfig,
                                              @Nullable SingleHostConfiguration singleHostConfiguration,
                                              @Nullable MultipleHostsConfiguration multipleHostsConfiguration) {
        this.applicationName = Assert.requireNonNull(applicationName, "applicationName must not be null");
        this.autodetectExtensions = autodetectExtensions;
        this.connectTimeout = connectTimeout;
        this.extensions = Assert.requireNonNull(extensions, "extensions must not be null");
        this.database = database;
        this.forceBinary = forceBinary;
        this.options = options;
        this.password = password;
        this.schema = schema;
        this.username = Assert.requireNonNull(username, "username must not be null");
        this.sslConfig = sslConfig;
        this.singleHostConfiguration = singleHostConfiguration;
        this.multipleHostsConfiguration = multipleHostsConfiguration;
    }

    /**
     * Returns a new {@link Builder}.
     *
     * @return a new {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    private static String repeat(int length, String character) {

        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < length; i++) {
            builder.append(character);
        }

        return builder.toString();
    }

    @Nullable
    public MultipleHostsConfiguration getMultipleHostsConfiguration() {
        return multipleHostsConfiguration;
    }

    @Nullable
    public SingleHostConfiguration getSingleHostConfiguration() {
        return singleHostConfiguration;
    }

    @Override
    public String toString() {
        return "PostgresqlConnectionConfiguration{" +
            "applicationName='" + this.applicationName + '\'' +
            ", singleHostConfiguration='" + this.singleHostConfiguration + '\'' +
            ", multipleHostsConfiguration='" + this.multipleHostsConfiguration + '\'' +
            ", autodetectExtensions='" + this.autodetectExtensions + '\'' +
            ", connectTimeout=" + this.connectTimeout +
            ", database='" + this.database + '\'' +
            ", extensions=" + this.extensions +
            ", forceBinary='" + this.forceBinary + '\'' +
            ", options='" + this.options + '\'' +
            ", password='" + repeat(this.password != null ? this.password.length() : 0, "*") + '\'' +
            ", schema='" + this.schema + '\'' +
            ", username='" + this.username + '\'' +
            '}';
    }

    String getApplicationName() {
        return this.applicationName;
    }

    @Nullable
    Duration getConnectTimeout() {
        return this.connectTimeout;
    }

    @Nullable
    String getDatabase() {
        return this.database;
    }

    List<Extension> getExtensions() {
        return this.extensions;
    }

    @Nullable
    Map<String, String> getOptions() {
        return this.options;
    }

    @Nullable
    CharSequence getPassword() {
        return this.password;
    }

    @Nullable
    String getSchema() {
        return this.schema;
    }


    String getUsername() {
        return this.username;
    }

    boolean isAutodetectExtensions() {
        return this.autodetectExtensions;
    }

    boolean isForceBinary() {
        return this.forceBinary;
    }

    SSLConfig getSslConfig() {
        return this.sslConfig;
    }

    /**
     * A builder for {@link PostgresqlConnectionConfiguration} instances.
     * <p>
     * <i>This class is not threadsafe</i>
     */
    public static final class Builder {

        private String applicationName = "r2dbc-postgresql";

        private boolean autodetectExtensions = true;

        @Nullable
        private Duration connectTimeout;

        @Nullable
        private String database;

        private List<Extension> extensions = new ArrayList<>();

        private boolean forceBinary = false;

        @Nullable
        private MultipleHostsConfiguration multipleHostsConfiguration;

        @Nullable
        private SingleHostConfiguration singleHostConfiguration;

        private Map<String, String> options;

        @Nullable
        private CharSequence password;

        @Nullable
        private String schema;

        @Nullable
        private String sslCert = null;

        private HostnameVerifier sslHostnameVerifier = DefaultHostnameVerifier.INSTANCE;

        @Nullable
        private String sslKey = null;

        private SSLMode sslMode = SSLMode.DISABLE;

        @Nullable
        private CharSequence sslPassword = null;

        @Nullable
        private String sslRootCert = null;

        private Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer = Function.identity();

        @Nullable
        private String username;

        private Builder() {
        }

        /**
         * Configure the application name.  Defaults to {@code postgresql-r2dbc}.
         *
         * @param applicationName the application name
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code applicationName} is {@code null}
         */
        public Builder applicationName(String applicationName) {
            this.applicationName = Assert.requireNonNull(applicationName, "applicationName must not be null");
            return this;
        }

        /**
         * Configures whether to use {@link ServiceLoader} to discover and register extensions. Defaults to true.
         *
         * @param autodetectExtensions to discover and register extensions
         * @return this {@link Builder}
         */
        public Builder autodetectExtensions(boolean autodetectExtensions) {
            this.autodetectExtensions = autodetectExtensions;
            return this;
        }

        /**
         * Returns a configured {@link PostgresqlConnectionConfiguration}.
         *
         * @return a configured {@link PostgresqlConnectionConfiguration}
         */
        public PostgresqlConnectionConfiguration build() {

            if (this.singleHostConfiguration != null && this.singleHostConfiguration.getHost() == null && this.singleHostConfiguration.getSocket() == null) {
                throw new IllegalArgumentException("host or socket must not be null");
            }

            if (this.singleHostConfiguration != null && this.singleHostConfiguration.getHost() != null && this.singleHostConfiguration.getSocket() != null) {
                throw new IllegalArgumentException("Connection must be configured for either host/port or socket usage but not both");
            }

            if (this.username == null) {
                throw new IllegalArgumentException("username must not be null");
            }

            return new PostgresqlConnectionConfiguration(this.applicationName, this.autodetectExtensions, this.connectTimeout, this.database, this.extensions, this.forceBinary,
                this.options, this.password, this.schema, this.username, this.createSslConfig(), this.singleHostConfiguration, this.multipleHostsConfiguration);
        }

        /**
         * Configures the connection timeout. Default unconfigured.
         *
         * @param connectTimeout the connection timeout
         * @return this {@link Builder}
         */
        public Builder connectTimeout(@Nullable Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * Registers a {@link CodecRegistrar} that can contribute extension {@link Codec}s.
         *
         * @param codecRegistrar registrar to contribute codecs
         * @return this {@link Builder}
         */
        public Builder codecRegistrar(CodecRegistrar codecRegistrar) {
            return extendWith(codecRegistrar);
        }

        /**
         * Configure the database.
         *
         * @param database the database
         * @return this {@link Builder}
         */
        public Builder database(@Nullable String database) {
            this.database = database;
            return this;
        }

        /**
         * Enable SSL usage. This flag is also known as Use Encryption in other drivers.
         *
         * @return this {@link Builder}
         */
        public Builder enableSsl() {
            return sslMode(SSLMode.VERIFY_FULL);
        }

        /**
         * Registers a {@link Extension} to extend driver functionality.
         *
         * @param extension extension to extend driver functionality
         * @return this {@link Builder}
         */
        public Builder extendWith(Extension extension) {
            this.extensions.add(Assert.requireNonNull(extension, "extension must not be null"));
            return this;
        }

        /**
         * Force binary results (<a href="https://wiki.postgresql.org/wiki/JDBC-BinaryTransfer">Binary Transfer</a>). Defaults to false.
         *
         * @param forceBinary whether to force binary transfer
         * @return this {@link Builder}
         */
        public Builder forceBinary(boolean forceBinary) {
            this.forceBinary = forceBinary;
            return this;
        }

        /**
         * Configure the host.
         *
         * @param host the host
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code host} is {@code null}
         */
        public Builder host(String host) {
            Assert.requireNonNull(host, "host must not be null");
            if (this.singleHostConfiguration == null) {
                this.singleHostConfiguration = new SingleHostConfiguration(host, DEFAULT_PORT, null);
            } else {
                this.singleHostConfiguration = new SingleHostConfiguration(host, this.singleHostConfiguration.getPort(), this.singleHostConfiguration.getSocket());
            }
            return this;
        }

        /**
         * Configure connection initialization parameters.
         * <p>
         * These parameters are applied once after creating a new connection. This is useful for setting up client-specific
         * <a href="https://www.postgresql.org/docs/current/runtime-config-client.html#RUNTIME-CONFIG-CLIENT-FORMAT">runtime parameters</a>
         * like statement timeouts, time zones etc.
         *
         * @param options the options
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code options} or any key or value of {@code options} is {@code null}
         */
        public Builder options(Map<String, String> options) {
            Assert.requireNonNull(options, "options map must not be null");

            options.forEach((k, v) -> {
                Assert.requireNonNull(k, "option keys must not be null");
                Assert.requireNonNull(v, "option values must not be null");
            });

            this.options = options;
            return this;
        }

        /**
         * Configure the password.
         *
         * @param password the password
         * @return this {@link Builder}
         */
        public Builder password(@Nullable CharSequence password) {
            this.password = password;
            return this;
        }

        public Builder multipleHostsConfiguration(MultipleHostsConfiguration multipleHostsConfiguration) {
            this.multipleHostsConfiguration = Assert.requireNonNull(multipleHostsConfiguration, "multipleHostsConfiguration must not be null");
            return this;
        }

        /**
         * Configure the schema.
         *
         * @param schema the schema
         * @return this {@link Builder}
         */
        public Builder schema(@Nullable String schema) {
            this.schema = schema;
            return this;
        }

        /**
         * Configure the port.  Defaults to {@code 5432}.
         *
         * @param port the port
         * @return this {@link Builder}
         */
        public Builder port(int port) {
            if (this.singleHostConfiguration == null) {
                this.singleHostConfiguration = new SingleHostConfiguration(null, port, null);
            } else {
                this.singleHostConfiguration = new SingleHostConfiguration(this.singleHostConfiguration.getHost(), port, this.singleHostConfiguration.getSocket());
            }
            return this;
        }

        /**
         * Configure a {@link SslContextBuilder} customizer. The customizer gets applied on each SSL connection attempt to allow for just-in-time configuration updates. The {@link Function} gets
         * called with the prepared {@link SslContextBuilder} that has all configuration options applied. The customizer may return the same builder or return a new builder instance to be used to
         * build the SSL context.
         *
         * @param sslContextBuilderCustomizer customizer function
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code sslContextBuilderCustomizer} is {@code null}
         */
        public Builder sslContextBuilderCustomizer(Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer) {
            this.sslContextBuilderCustomizer = Assert.requireNonNull(sslContextBuilderCustomizer, "sslContextBuilderCustomizer must not be null");
            return this;
        }

        /**
         * Configure ssl cert for client certificate authentication.
         *
         * @param sslCert an X.509 certificate chain file in PEM format
         * @return this {@link Builder}
         */
        public Builder sslCert(String sslCert) {
            this.sslCert = Assert.requireFileExistsOrNull(sslCert, "sslCert must not be null and must exist");
            return this;
        }

        /**
         * Configure ssl HostnameVerifier.
         *
         * @param sslHostnameVerifier {@link javax.net.ssl.HostnameVerifier}
         * @return this {@link Builder}
         */
        public Builder sslHostnameVerifier(HostnameVerifier sslHostnameVerifier) {
            this.sslHostnameVerifier = Assert.requireNonNull(sslHostnameVerifier, "sslHostnameVerifier must not be null");
            return this;
        }

        /**
         * Configure ssl key for client certificate authentication.
         *
         * @param sslKey a PKCS#8 private key file in PEM format
         * @return this {@link Builder}
         */
        public Builder sslKey(String sslKey) {
            this.sslKey = Assert.requireFileExistsOrNull(sslKey, "sslKey must not be null and must exist");
            return this;
        }

        /**
         * Configure ssl mode.
         *
         * @param sslMode the SSL mode to use.
         * @return this {@link Builder}
         */
        public Builder sslMode(SSLMode sslMode) {
            this.sslMode = Assert.requireNonNull(sslMode, "sslMode must be not be null");
            return this;
        }

        /**
         * Configure ssl password.
         *
         * @param sslPassword the password of the sslKey, or null if it's not password-protected
         * @return this {@link Builder}
         */
        public Builder sslPassword(@Nullable CharSequence sslPassword) {
            this.sslPassword = sslPassword;
            return this;
        }

        /**
         * Configure ssl root cert for server certificate validation.
         *
         * @param sslRootCert an X.509 certificate chain file in PEM format
         * @return this {@link Builder}
         */
        public Builder sslRootCert(String sslRootCert) {
            this.sslRootCert = Assert.requireFileExistsOrNull(sslRootCert, "sslRootCert must not be null and must exist");
            return this;
        }

        /**
         * Configure the username.
         *
         * @param username the username
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code username} is {@code null}
         */
        public Builder username(String username) {
            this.username = Assert.requireNonNull(username, "username must not be null");
            return this;
        }

        /**
         * Configure the unix domain socket to connect to.
         *
         * @param socket the socket path
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code socket} is {@code null}
         */
        public Builder socket(String socket) {
            Assert.requireNonNull(socket, "host must not be null");

            if (this.singleHostConfiguration == null) {
                this.singleHostConfiguration = new SingleHostConfiguration(null, DEFAULT_PORT, socket);
            } else {
                this.singleHostConfiguration = new SingleHostConfiguration(this.singleHostConfiguration.getHost(), this.singleHostConfiguration.getPort(), socket);
            }

            sslMode(SSLMode.DISABLE);
            return this;
        }

        @Override
        public String toString() {
            return "Builder{" +
                "applicationName='" + this.applicationName + '\'' +
                ", singleHostConfiguration='" + this.singleHostConfiguration + '\'' +
                ", multipleHostsConfiguration='" + this.multipleHostsConfiguration + '\'' +
                ", autodetectExtensions='" + this.autodetectExtensions + '\'' +
                ", connectTimeout='" + this.connectTimeout + '\'' +
                ", database='" + this.database + '\'' +
                ", extensions='" + this.extensions + '\'' +
                ", forceBinary='" + this.forceBinary + '\'' +
                ", parameters='" + this.options + '\'' +
                ", password='" + repeat(this.password != null ? this.password.length() : 0, "*") + '\'' +
                ", schema='" + this.schema + '\'' +
                ", username='" + this.username + '\'' +
                ", sslContextBuilderCustomizer='" + this.sslContextBuilderCustomizer + '\'' +
                ", sslMode='" + this.sslMode + '\'' +
                ", sslRootCert='" + this.sslRootCert + '\'' +
                ", sslCert='" + this.sslCert + '\'' +
                ", sslKey='" + this.sslKey + '\'' +
                ", sslHostnameVerifier='" + this.sslHostnameVerifier + '\'' +
                '}';
        }

        private SSLConfig createSslConfig() {
            if (this.singleHostConfiguration != null && this.singleHostConfiguration.getSocket() != null || this.sslMode == SSLMode.DISABLE) {
                return SSLConfig.disabled();
            }

            HostnameVerifier hostnameVerifier = this.sslHostnameVerifier;
            return new SSLConfig(this.sslMode, createSslProvider(), hostnameVerifier);
        }

        private Supplier<SslProvider> createSslProvider() {
            SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
            if (this.sslMode.verifyCertificate()) {
                if (this.sslRootCert != null) {
                    sslContextBuilder.trustManager(new File(this.sslRootCert));
                }
            } else {
                sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            }

            String sslKey = this.sslKey;
            String sslCert = this.sslCert;

            // Emulate Libpq behavior
            // Determining the default file location
            String pathsep = System.getProperty("file.separator");
            String defaultdir;
            if (System.getProperty("os.name").toLowerCase().contains("windows")) { // It is Windows
                defaultdir = System.getenv("APPDATA") + pathsep + "postgresql" + pathsep;
            } else {
                defaultdir = System.getProperty("user.home") + pathsep + ".postgresql" + pathsep;
            }

            if (sslCert == null) {
                String pathname = defaultdir + "postgresql.crt";
                if (new File(pathname).exists()) {
                    sslCert = pathname;
                }
            }

            if (sslKey == null) {
                String pathname = defaultdir + "postgresql.pk8";
                if (new File(pathname).exists()) {
                    sslKey = pathname;
                }
            }

            if (sslKey != null && sslCert != null) {
                String sslPassword = this.sslPassword == null ? null : this.sslPassword.toString();
                sslContextBuilder.keyManager(new File(sslCert), new File(sslKey), sslPassword);
            }


            return () -> SslProvider.builder()
                .sslContext(this.sslContextBuilderCustomizer.apply(sslContextBuilder))
                .defaultConfiguration(TCP)
                .build();
        }
    }
}
