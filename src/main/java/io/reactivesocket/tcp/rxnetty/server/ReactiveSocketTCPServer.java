package io.reactivesocket.tcp.rxnetty.server;

import io.netty.buffer.ByteBuf;
import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.LeaseGovernor;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.tcp.rxnetty.TcpDuplexConnection;
import io.reactivex.netty.channel.Connection;
import io.reactivex.netty.protocol.tcp.server.ConnectionHandler;
import rx.Observable;

import java.util.concurrent.ConcurrentHashMap;

public class ReactiveSocketTCPServer implements ConnectionHandler<ByteBuf, ByteBuf> {
    private final ConcurrentHashMap<TcpDuplexConnection, ReactiveSocket> reactiveSockets;

    private final ConnectionSetupHandler setupHandler;

    private final LeaseGovernor leaseGovernor;

    private ReactiveSocketTCPServer(ConnectionSetupHandler setupHandler, LeaseGovernor leaseGovernor) {
        reactiveSockets = new ConcurrentHashMap<>();
        this.setupHandler  = setupHandler;
        this.leaseGovernor = leaseGovernor;
    }

    public static ReactiveSocketTCPServer create(ConnectionSetupHandler setupHandler, LeaseGovernor leaseGovernor) {
        return new ReactiveSocketTCPServer(setupHandler, leaseGovernor);
    }

    public static ReactiveSocketTCPServer create(ConnectionSetupHandler setupHandler) {
        return new ReactiveSocketTCPServer(setupHandler, LeaseGovernor.UNLIMITED_LEASE_GOVERNOR);
    }

    @Override
    public Observable<Void> handle(Connection<ByteBuf, ByteBuf> connection) {
        return Observable
            .create(subscriber -> {
                TcpDuplexConnection tcpDuplexConnection = TcpDuplexConnection.createTcpConnection(connection);

                ReactiveSocket reactiveSocket  = ReactiveSocket
                    .fromServerConnection(
                        tcpDuplexConnection,
                        setupHandler,
                        leaseGovernor,
                        t -> t.printStackTrace());

                reactiveSocket
                    .start(new Completable() {
                        @Override
                        public void success() {

                        }

                        @Override
                        public void error(Throwable e) {
                            subscriber.onError(e);
                        }
                    });

                reactiveSockets.putIfAbsent(tcpDuplexConnection, reactiveSocket);

                connection
                    .closeListener()
                    .doOnCompleted(() -> {
                       reactiveSockets
                           .remove(tcpDuplexConnection);

                        try {
                            reactiveSocket.close();
                        } catch (Exception e) {
                            subscriber.onError(e);
                        }
                    });
            });
    }
}
