package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionSettings;
import io.r2dbc.postgresql.client.MultiHostConfiguration;
import io.r2dbc.postgresql.codec.DefaultCodecs;
import io.r2dbc.spi.IsolationLevel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.net.SocketAddress;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static io.r2dbc.postgresql.TargetServerType.ANY;
import static io.r2dbc.postgresql.TargetServerType.MASTER;
import static io.r2dbc.postgresql.TargetServerType.PREFER_SECONDARY;
import static io.r2dbc.postgresql.TargetServerType.SECONDARY;

public class MultiHostConnectionStrategy implements ConnectionStrategy {

    private final List<SocketAddress> addresses;

    private final PostgresqlConnectionConfiguration configuration;

    private final ConnectionStrategy connectionStrategy;

    private final MultiHostConfiguration multiHostConfiguration;

    private final Map<SocketAddress, MultiHostConnectionStrategy.HostSpecStatus> statusMap;

    MultiHostConnectionStrategy(List<SocketAddress> addresses, PostgresqlConnectionConfiguration configuration, ConnectionStrategy connectionStrategy) {
        this.addresses = addresses;
        this.configuration = configuration;
        this.connectionStrategy = connectionStrategy;
        this.multiHostConfiguration = this.configuration.getMultiHostConfiguration();
        this.statusMap = new ConcurrentHashMap<>();
    }

    public Mono<Client> tryConnect(TargetServerType targetServerType) {
        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        return this.getCandidates(targetServerType).concatMap(candidate -> this.tryConnectToCandidate(targetServerType, candidate)
                .onErrorResume(e -> {
                    if (!exceptionRef.compareAndSet(null, e)) {
                        exceptionRef.get().addSuppressed(e);
                    }
                    this.statusMap.put(candidate, MultiHostConnectionStrategy.HostSpecStatus.fail(candidate));
                    return Mono.empty();
                }))
            .next()
            .switchIfEmpty(Mono.defer(() -> exceptionRef.get() != null
                ? Mono.error(exceptionRef.get())
                : Mono.empty()));
    }

    private static MultiHostConnectionStrategy.HostSpecStatus evaluateStatus(SocketAddress candidate, @Nullable MultiHostConnectionStrategy.HostSpecStatus oldStatus) {
        return oldStatus == null || oldStatus.hostStatus == MultiHostConnectionStrategy.HostStatus.CONNECT_FAIL
            ? MultiHostConnectionStrategy.HostSpecStatus.ok(candidate)
            : oldStatus;
    }

    private static Mono<Boolean> isPrimaryServer(Client client, PostgresqlConnectionConfiguration configuration) {
        PostgresqlConnection connection = new PostgresqlConnection(client, new DefaultCodecs(client.getByteBufAllocator()), DefaultPortalNameSupplier.INSTANCE,
            StatementCache.fromPreparedStatementCacheQueries(client, configuration.getPreparedStatementCacheQueries()), IsolationLevel.READ_UNCOMMITTED, configuration);
        return connection.createStatement("show transaction_read_only")
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, String.class)))
            .map(s -> s.equalsIgnoreCase("off"))
            .next();
    }

    private Flux<SocketAddress> getCandidates(TargetServerType targetServerType) {
        return Flux.create(sink -> {
            Predicate<Long> needsRecheck = updated -> System.currentTimeMillis() > updated + this.multiHostConfiguration.getHostRecheckTime().toMillis();
            List<SocketAddress> addresses = new ArrayList<>(this.addresses);
            if (this.multiHostConfiguration.isLoadBalanceHosts()) {
                Collections.shuffle(addresses);
            }
            int counter = 0;
            for (SocketAddress address : addresses) {
                MultiHostConnectionStrategy.HostSpecStatus currentStatus = this.statusMap.get(address);
                if (currentStatus == null || needsRecheck.test(currentStatus.updated)) {
                    sink.next(address);
                    counter++;
                } else if (targetServerType.allowStatus(currentStatus.hostStatus)) {
                    sink.next(address);
                    counter++;
                }
            }
            if (counter == 0) {
                // if no candidate match the requirement or all of them are in unavailable status try all the hosts
                addresses = new ArrayList<>(this.addresses);
                if (this.multiHostConfiguration.isLoadBalanceHosts()) {
                    Collections.shuffle(addresses);
                }
                for (SocketAddress address : addresses) {
                    sink.next(address);
                }
            }
            sink.complete();
        });
    }

    private Mono<Client> tryConnectToCandidate(TargetServerType targetServerType, SocketAddress candidate) {
        return Mono.create(sink -> this.connectionStrategy.withAddress(candidate).connect().subscribe(client -> {
            this.statusMap.compute(candidate, (a, oldStatus) -> MultiHostConnectionStrategy.evaluateStatus(candidate, oldStatus));
            if (targetServerType == ANY) {
                sink.success(client);
                return;
            }
            MultiHostConnectionStrategy.isPrimaryServer(client, this.configuration).subscribe(
                isPrimary -> {
                    if (isPrimary) {
                        this.statusMap.put(candidate, MultiHostConnectionStrategy.HostSpecStatus.primary(candidate));
                    } else {
                        this.statusMap.put(candidate, MultiHostConnectionStrategy.HostSpecStatus.standby(candidate));
                    }
                    if (isPrimary && targetServerType == MASTER) {
                        sink.success(client);
                    } else if (!isPrimary && (targetServerType == SECONDARY || targetServerType == PREFER_SECONDARY)) {
                        sink.success(client);
                    } else {
                        client.close().subscribe(v -> sink.success(), sink::error, sink::success, sink.currentContext());
                    }
                },
                sink::error, () -> {}, sink.currentContext());
        }, sink::error, () -> {}, sink.currentContext()));
    }

    @Override
    public Mono<Client> connect() {
        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        TargetServerType targetServerType = this.multiHostConfiguration.getTargetServerType();
        return this.tryConnect(targetServerType)
            .onErrorResume(e -> {
                if (!exceptionRef.compareAndSet(null, e)) {
                    exceptionRef.get().addSuppressed(e);
                }
                return Mono.empty();
            })
            .switchIfEmpty(Mono.defer(() -> targetServerType == PREFER_SECONDARY
                ? this.tryConnect(MASTER)
                : Mono.empty()))
            .switchIfEmpty(Mono.error(() -> {
                Throwable error = exceptionRef.get();
                if (error == null) {
                    return new PostgresqlConnectionFactory.PostgresConnectionException(String.format("No server matches target type %s", targetServerType.getValue()), null);
                } else {
                    return error;
                }
            }));
    }

    @Override
    public ConnectionStrategy withAddress(SocketAddress address) {
        throw new InvalidParameterException("single address of HaClusterConnectionStrategy does not exist");
    }

    @Override
    public ConnectionStrategy withConnectionSettings(ConnectionSettings connectionSettings) {
        throw new InvalidParameterException("connectionSettings of HaClusterConnectionStrategy are not intended to be changed");
    }

    @Override
    public ConnectionStrategy withOptions(Map<String, String> options) {
        return new MultiHostConnectionStrategy(this.addresses, this.configuration, this.connectionStrategy.withOptions(options));
    }

    enum HostStatus {
        CONNECT_FAIL,
        CONNECT_OK,
        PRIMARY,
        STANDBY
    }

    private static class HostSpecStatus {

        public final SocketAddress address;

        public final HostStatus hostStatus;

        public final long updated;

        private HostSpecStatus(SocketAddress address, HostStatus hostStatus) {
            this.address = address;
            this.hostStatus = hostStatus;
            this.updated = System.currentTimeMillis();
        }

        public static HostSpecStatus fail(SocketAddress host) {
            return new HostSpecStatus(host, HostStatus.CONNECT_FAIL);
        }

        public static HostSpecStatus ok(SocketAddress host) {
            return new HostSpecStatus(host, HostStatus.CONNECT_OK);
        }

        public static HostSpecStatus primary(SocketAddress host) {
            return new HostSpecStatus(host, HostStatus.PRIMARY);
        }

        public static HostSpecStatus standby(SocketAddress host) {
            return new HostSpecStatus(host, HostStatus.STANDBY);
        }
    }
}
