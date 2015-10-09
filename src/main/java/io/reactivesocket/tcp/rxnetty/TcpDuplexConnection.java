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
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Disposable;
import io.reactivesocket.rx.Observable;
import io.reactivesocket.rx.Observer;
import io.reactivex.netty.channel.Connection;
import org.reactivestreams.Publisher;
import rx.RxReactiveStreams;
import rx.Subscriber;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CopyOnWriteArrayList;

public class TcpDuplexConnection implements DuplexConnection {
    protected static ThreadLocal<MutableDirectByteBuf> mutableDirectByteBufs = ThreadLocal.withInitial(() -> new MutableDirectByteBuf(Unpooled.buffer()));

    private Connection<ByteBuf, ByteBuf> connection;

    private CopyOnWriteArrayList<Observer<Frame>> observers;

    private TcpDuplexConnection(Connection<ByteBuf, ByteBuf> connection) {
        this.connection = connection;
        this.observers = new CopyOnWriteArrayList<>();

        connection
            .getInput()
            .unsafeSubscribe(new Subscriber<ByteBuf>() {
                @Override
                public void onCompleted() {
                    observers
                        .forEach(Observer::onComplete);
                }

                @Override
                public void onError(Throwable e) {
                    observers
                        .forEach(observer ->
                            observer.onError(e));
                }

                @Override
                public void onNext(ByteBuf byteBuf) {
                    observers
                        .forEach(observer -> {
                            MutableDirectByteBuf buffer = mutableDirectByteBufs.get();
                            buffer.wrap(byteBuf);
                            Frame frame = Frame.from(buffer, 0, buffer.capacity());
                            observer.onNext(frame);
                        });
                }
            });

    }

    public static TcpDuplexConnection createTcpConnection(Connection<ByteBuf, ByteBuf> connection) {
       return new TcpDuplexConnection(connection);
    }

    @Override
    public Observable<Frame> getInput() {
        Observable<Frame> observable = new Observable<Frame>() {
            @Override
            public void subscribe(Observer<Frame> o) {
                observers.add(o);

                o.onSubscribe(new Disposable() {
                    @Override
                    public void dispose() {
                        observers.removeIf(s -> s == o);
                    }
                });
            }
        };

        return observable;
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        rx.Observable<ByteBuf> byteBufObservable = RxReactiveStreams
            .toObservable(o)
            .map(frame -> {
                ByteBuffer byteBuffer = frame.getByteBuffer();
                ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer(byteBuffer.capacity());
                byteBuf.writeBytes(byteBuffer);

                return byteBuf;
            });

        connection
            .writeAndFlushOnEach(byteBufObservable)
            .doOnCompleted(callback::success)
            .doOnError(callback::error)
            .subscribe();
    }

    @Override
    public void close() throws IOException {
        connection.close().subscribe(new Subscriber<Void>() {
            @Override
            public void onCompleted() {
            }

            @Override
            public void onError(Throwable e) {
            }

            @Override
            public void onNext(Void aVoid) {
            }
        });

    }
}
