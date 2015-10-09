/**
 * Copyright 2015 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.tcp.rxnetty;

import io.netty.buffer.ByteBuf;
import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.exceptions.SetupException;
import io.reactivesocket.tcp.rxnetty.server.ReactiveSocketTCPServer;
import io.reactivex.netty.channel.Connection;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import io.reactivex.netty.protocol.tcp.server.TcpServer;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;

import java.util.concurrent.CountDownLatch;

public class ReactiveSocketTCPTest {

    @Test(timeout = 5_00000000)
    public void test100() throws Exception {
        testN(100);
    }

    @Test(timeout = 5_000)
    public void test10_000() throws Exception {
        testN(10_000);
    }

    @Test(timeout = 5_000)
    public void test100_000() throws Exception {
        testN(100_000);
    }

    public void testN(int n) throws Exception {
        TcpServer<ByteBuf, ByteBuf> tcpServer = TcpServer.newServer(12345);
        tcpServer.start(ReactiveSocketTCPServer.create(new ConnectionSetupHandler() {
            @Override
            public RequestHandler apply(ConnectionSetupPayload setupPayload) throws SetupException {
                return new RequestHandler() {
                    @Override
                    public Publisher<Payload> handleRequestResponse(Payload payload) {
                        Payload resp = TestUtil.utf8EncodedPayload("pong", "pong meta data");
                        return new Publisher<Payload>() {
                            @Override
                            public void subscribe(Subscriber<? super Payload> s) {
                                s.onNext(resp);
                                s.onComplete();
                            }
                        };
                    }

                    @Override
                    public Publisher<Payload> handleRequestStream(Payload payload) {
                        return null;
                    }

                    @Override
                    public Publisher<Payload> handleSubscription(Payload payload) {
                        return null;
                    }

                    @Override
                    public Publisher<Void> handleFireAndForget(Payload payload) {
                        return null;
                    }

                    @Override
                    public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> payloads) {
                        return null;
                    }

                    @Override
                    public Publisher<Void> handleMetadataPush(Payload payload) {
                        return null;
                    }
                };

            }
        }));

        TcpClient<ByteBuf, ByteBuf> localhost = TcpClient.newClient("localhost", 12345);
        Connection<ByteBuf, ByteBuf> connection = localhost.createConnectionRequest().toBlocking().last();
        TcpDuplexConnection duplexConnection = TcpDuplexConnection.createTcpConnection(connection);
        ReactiveSocket client = ReactiveSocket.fromClientConnection(duplexConnection, ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.NO_FLAGS));
        client.startAndWait();

        TestSubscriber testSubscriber = new TestSubscriber();

        CountDownLatch latch = new CountDownLatch(n);

        Observable
            .range(0, n)
            .flatMap(i -> {
                Publisher<Payload> payloadPublisher = client.requestResponse(TestUtil.utf8EncodedPayload("ping => " + i, "ping metadata"));
                return RxReactiveStreams.toObservable(payloadPublisher).doOnNext(f -> latch.countDown());
            })
            .subscribe();

        latch.await();

        //testSubscriber.awaitTerminalEvent(3, TimeUnit.SECONDS);
        //testSubscriber.assertValueCount(1);
        //testSubscriber.assertCompleted();

    }
}
