package io.r2dbc.postgresql;

import io.r2dbc.postgresql.authentication.AuthenticationHandler;
import io.r2dbc.postgresql.authentication.PasswordAuthenticationHandler;
import io.r2dbc.postgresql.authentication.SASLAuthenticationHandler;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionSettings;
import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.postgresql.client.StartupMessageFlow;
import io.r2dbc.postgresql.message.backend.AuthenticationMessage;
import io.r2dbc.postgresql.util.Assert;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.net.SocketAddress;
import java.util.Map;
import java.util.function.Predicate;

public abstract class ClientFactoryBase implements ClientFactory {

    private final ClientSupplier clientSupplier;

    private final PostgresqlConnectionConfiguration configuration;

    protected ClientFactoryBase(PostgresqlConnectionConfiguration configuration, ClientSupplier clientSupplier) {
        this.configuration = configuration;
        this.clientSupplier = clientSupplier;
    }

    protected PostgresqlConnectionConfiguration getConfiguration() {
        return this.configuration;
    }

    protected AuthenticationHandler getAuthenticationHandler(AuthenticationMessage message) {
        if (PasswordAuthenticationHandler.supports(message)) {
            CharSequence password = Assert.requireNonNull(this.configuration.getPassword(), "Password must not be null");
            return new PasswordAuthenticationHandler(password, this.configuration.getUsername());
        } else if (SASLAuthenticationHandler.supports(message)) {
            CharSequence password = Assert.requireNonNull(this.configuration.getPassword(), "Password must not be null");
            return new SASLAuthenticationHandler(password, this.configuration.getUsername());
        } else {
            throw new IllegalStateException(String.format("Unable to provide AuthenticationHandler capable of handling %s", message));
        }
    }

    protected Mono<? extends Client> tryConnectToEndpoint(SocketAddress endpoint, @Nullable Map<String, String> options) {
        SSLConfig sslConfig = this.configuration.getSslConfig();
        ConnectionSettings connectionSettings = this.configuration.getConnectionSettings();
        Predicate<Throwable> isAuthSpecificationError = e -> e instanceof ExceptionFactory.PostgresqlAuthenticationFailure;
        return this.tryConnectWithConfig(endpoint, connectionSettings, options)
            .onErrorResume(
                isAuthSpecificationError.and(e -> sslConfig.getSslMode() == SSLMode.ALLOW),
                e -> this.tryConnectWithConfig(endpoint, connectionSettings.mutate(builder -> builder.sslConfig(sslConfig.mutateMode(SSLMode.REQUIRE))), options)
                    .onErrorResume(sslAuthError -> {
                        e.addSuppressed(sslAuthError);
                        return Mono.error(e);
                    })
            )
            .onErrorResume(
                isAuthSpecificationError.and(e -> sslConfig.getSslMode() == SSLMode.PREFER),
                e -> this.tryConnectWithConfig(endpoint, connectionSettings.mutate(builder -> builder.sslConfig(sslConfig.mutateMode(SSLMode.DISABLE))), options)
                    .onErrorResume(sslAuthError -> {
                        e.addSuppressed(sslAuthError);
                        return Mono.error(e);
                    })
            );
    }

    protected Mono<Client> tryConnectWithConfig(SocketAddress endpoint, ConnectionSettings settings, @Nullable Map<String, String> options) {
        return this.clientSupplier.connect(endpoint, settings)
            .delayUntil(client -> StartupMessageFlow
                .exchange(this.configuration.getApplicationName(), this::getAuthenticationHandler, client, this.configuration.getDatabase(), this.configuration.getUsername(), options)
                .handle(ExceptionFactory.INSTANCE::handleErrorResponse));
    }

}
