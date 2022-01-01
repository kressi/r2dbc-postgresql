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

    private final PostgresqlConnectionConfiguration configuration;

    private final ConnectionStrategy connectionStrategy;

    private final Map<String, String> options;

    SslFallbackConnectionStrategy(PostgresqlConnectionConfiguration configuration, ConnectionStrategy connectionStrategy, @Nullable Map<String, String> options) {
        this.configuration = configuration;
        this.connectionStrategy = connectionStrategy;
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
    public ConnectionStrategy withAddress(SocketAddress address) {
        return new SslFallbackConnectionStrategy(this.configuration, this.connectionStrategy.withAddress(address), this.options);
    }

    @Override
    public ConnectionStrategy withConnectionSettings(ConnectionSettings connectionSettings) {
        return new SslFallbackConnectionStrategy(this.configuration, this.connectionStrategy.withConnectionSettings(connectionSettings), this.options);
    }

    @Override
    public ConnectionStrategy withOptions(Map<String, String> options) {
        return new SslFallbackConnectionStrategy(this.configuration, this.connectionStrategy.withOptions(options), options);
    }

}
