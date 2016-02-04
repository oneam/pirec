/**
 * Copyright 2015 Sam Leitch
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package redis.clients.pirec.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;

import redis.clients.pirec.codec.Decoder.DecoderException;
import redis.clients.pirec.codec.RedisDecoder;
import redis.clients.pirec.codec.RedisEncoder;
import redis.clients.pirec.codec.RedisEncoder.RedisEncodeException;
import redis.clients.pirec.codec.object.RedisObject;

public class RedisClient {

    final RedisEncoder encoder = new RedisEncoder();
    final RedisDecoder decoder = new RedisDecoder();

    final SocketAdapter socket;
    final ByteBuffer readBuffer = ByteBuffer.allocate(1048576);
    final ByteBuffer writeBuffer = ByteBuffer.allocate(1048576);

    final ConcurrentLinkedDeque<RedisObject> requestQueue = new ConcurrentLinkedDeque<>();
    final ConcurrentLinkedDeque<CompletableFuture<RedisObject>> responseQueue = new ConcurrentLinkedDeque<>();
    final Object sendSync = new Object();

    boolean writing = false;
    boolean reading = false;
    boolean connected = false;

    public RedisClient() {
        this(new SocketChannelAdapter());
    }

    public RedisClient(SocketAdapter socketAdapter) {
        this.socket = socketAdapter;
    }

    public int numActiveRequests() {
        synchronized (requestQueue) {
            return responseQueue.size();
        }
    }

    public CompletableFuture<Void> connect(InetSocketAddress remote) {
        CompletableFuture<Void> connectFuture = socket.connect(remote).whenComplete((r, e) -> {
            if (e == null) {
                connected = true;
            } else {
                onFailed(e);
            }
        });

        return connectFuture;
    }

    public void disconnect() throws Exception {
        if (connected) socket.close();
    }

    void onFailed(Throwable e) {
        if (e instanceof AsynchronousCloseException) {
            onClosed();
            return;
        }

        if (!connected) return;

        synchronized (sendSync) {
            connected = false;
            completeOutstandingRequestsExceptionally(e);
        }
    }

    void onClosed() {
        if (!connected) return;

        synchronized (sendSync) {
            connected = false;
            completeOutstandingRequestsExceptionally(new IOException("Redis client not connected"));
        }
    }

    void completeOutstandingRequestsExceptionally(Throwable e) {
        CompletableFuture<RedisObject> responseFuture = responseQueue.poll();
        while (responseFuture != null) {
            responseFuture.completeExceptionally(e);
            responseFuture = responseQueue.poll();
        }
    }

    void startRead() {
        synchronized (sendSync) {
            if (responseQueue.isEmpty()) {
                reading = false;
                return;
            }
        }

        socket.read(readBuffer).whenComplete((bytesRead, e) -> {
            if (e != null) {
                onFailed(e);
                return;
            }

            if (bytesRead <= 0) {
                onClosed();
                return;
            }

            try {
                readBuffer.flip();
                processResponses();
                readBuffer.compact();
                startRead();
            } catch (DecoderException | NoSuchElementException e2) {
                onFailed(e2);
            }
        });
    }

    void processResponses() throws DecoderException {
        RedisObject nextMessage = decoder.decode(readBuffer);
        while (nextMessage != null) {
            CompletableFuture<RedisObject> responseFuture;
            responseFuture = responseQueue.remove();
            responseFuture.complete(nextMessage);
            nextMessage = decoder.decode(readBuffer);
        }
    }

    void startWrite() {
        try {
            processRequests(); // Process as many as you can before locking

            synchronized (sendSync) {
                processRequests(); // Ensure request queue is empty after locking

                writeBuffer.flip();
                if (!writeBuffer.hasRemaining()) {
                    writeBuffer.compact();
                    writing = false;
                    return;
                }

                if (!reading) {
                    reading = true;
                    CompletableFuture.runAsync(this::startRead);
                }
            }

            socket.write(writeBuffer).whenComplete((bytesWritten, e) -> {
                if (e != null) {
                    onFailed(e);
                    return;
                }

                if (bytesWritten <= 0) {
                    return;
                }

                writeBuffer.compact();
                startWrite();
            });
        } catch (InterruptedException e) {
            onFailed(e);
        }
    }

    void processRequests() throws InterruptedException {
        RedisObject request = requestQueue.poll();

        try {
            while (request != null) {
                int bytesWritten = encoder.encode(request, writeBuffer);
                if (bytesWritten <= 0) {
                    requestQueue.addFirst(request);
                    break;
                }

                request = requestQueue.poll();
            }
        } catch (RedisEncodeException e) {
            onFailed(e);
        }
    }

    public CompletableFuture<RedisObject> sendRequest(RedisObject request) {
        CompletableFuture<RedisObject> responseFuture = new CompletableFuture<RedisObject>();

        synchronized (sendSync) {
            if (!connected) {
                responseFuture.completeExceptionally(new IOException("Redis client not connected"));
                return responseFuture;
            }

            responseQueue.add(responseFuture);
            requestQueue.add(request);

            if (!writing) {
                writing = true;
                CompletableFuture.runAsync(this::startWrite);
            }
        }

        return responseFuture;
    }
}
