package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionSettings;
import reactor.core.publisher.Mono;

import java.net.SocketAddress;
import java.util.Map;
import java.util.function.Function;

public interface ConnectionStrategy {

    default ConnectionStrategy andThen(boolean guard, Function<ConnectionStrategy, ConnectionStrategy> nextStrategyProvider) {
        return guard ? nextStrategyProvider.apply(this) : this;
    }

    Mono<Client> connect();
    ConnectionStrategy withConnectionSettings(ConnectionSettings connectionSettings);
    ConnectionStrategy withAddress(SocketAddress address);
    ConnectionStrategy withOptions(Map<String, String> options);
}
