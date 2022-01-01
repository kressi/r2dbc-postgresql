package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionSettings;
import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.net.SocketAddress;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class SslFallbackConnectionStrategy implements ConnectionStrategy {

    private final ConnectionStrategy connectionStrategy;
    private final PostgresqlConnectionConfiguration configuration;
    private final Map<String, String> options;

    SslFallbackConnectionStrategy(ConnectionStrategy connectionStrategy, PostgresqlConnectionConfiguration configuration, @Nullable Map<String, String> options) {
        this.connectionStrategy = connectionStrategy;
        this.configuration = configuration;
        this.options = options;
    }

    @Override
    public Mono<Client> connect() {
        SSLConfig sslConfig = this.configuration.getSslConfig();
        Predicate<Throwable> isAuthSpecificationError = e -> e instanceof ExceptionFactory.PostgresqlAuthenticationFailure;
        return this.connectionStrategy.connect()
            .onErrorResume(isAuthSpecificationError.and(e -> sslConfig.getSslMode() == SSLMode.ALLOW), fallback(SSLMode.REQUIRE))
            .onErrorResume(isAuthSpecificationError.and(e -> sslConfig.getSslMode() == SSLMode.PREFER), fallback(SSLMode.DISABLE));
    }

    private Function<Throwable, Mono<Client>> fallback(SSLMode sslMode) {
        ConnectionSettings connectionSettings = this.configuration.getConnectionSettings();
        SSLConfig sslConfig = this.configuration.getSslConfig();
        return e -> this.connectionStrategy.withConnectionSettings(connectionSettings.mutate(builder -> builder.sslConfig(sslConfig.mutateMode(sslMode))))
            .connect()
            .onErrorResume(sslAuthError -> {
                e.addSuppressed(sslAuthError);
                return Mono.error(e);
            });
    }

    @Override
    public ConnectionStrategy withConnectionSettings(ConnectionSettings connectionSettings) {
        return new SslFallbackConnectionStrategy(this.connectionStrategy.withConnectionSettings(connectionSettings), this.configuration, this.options);
    }

    @Override
    public ConnectionStrategy withAddress(SocketAddress address) {
        return new SslFallbackConnectionStrategy(this.connectionStrategy.withAddress(address), this.configuration, this.options);
    }

    @Override
    public ConnectionStrategy withOptions(Map<String, String> options) {
        return new SslFallbackConnectionStrategy(this.connectionStrategy.withOptions(options), this.configuration, options);
    }

}
