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
import java.lang.reflect.Array;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

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

    final Queue<CompletableFuture<RedisObject>> responseQueue = new ArrayDeque<>();
    final Queue<byte[][]> requestQueue = new ArrayDeque<byte[][]>();
    boolean readyToSend = true;

    boolean connected = false;

    public RedisClient() {
        this(new AsynchronousSocketChannelAdapter());
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

        connectFuture.whenCompleteAsync((r, e) -> {
            if (e == null) {
                startRead();
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

        synchronized (responseQueue) {
            if (!connected) return;

            connected = false;
            CompletableFuture<RedisObject> responseFuture = responseQueue.poll();
            while (responseFuture != null) {
                responseFuture.completeExceptionally(e);
                responseFuture = responseQueue.poll();
            }
        }
    }

    void onClosed() {
        if (!connected) return;
        connected = false;
    }

    void startRead() {
        socket.read(readBuffer).whenCompleteAsync((bytesRead, e) -> {
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
            synchronized (responseQueue) {
                responseFuture = responseQueue.remove();
            }
            responseFuture.complete(nextMessage);
            decoder.reset();
            nextMessage = decoder.decode(readBuffer);
        }
    }

    void startWrite() {
        try {
            synchronized (requestQueue) {
                processRequests();
                writeBuffer.flip();

                if (!writeBuffer.hasRemaining()) {
                    writeBuffer.compact();
                    readyToSend = true;
                    return;
                }
            }

            socket.write(writeBuffer).whenCompleteAsync((bytesWritten, e) -> {
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
        byte[][] request = requestQueue.peek();

        while (request != null) {
            int bytesWritten = addToBuffer(request, writeBuffer);
            if (bytesWritten <= 0) {
                break;
            }

            requestQueue.remove();
            request = requestQueue.peek();
        }
    }

    int addToBuffer(byte[][] request, ByteBuffer buffer) {
        int totalSize = Arrays.stream(request).mapToInt(Array::getLength).sum();
        if (buffer.remaining() < totalSize) {
            return 0;
        }

        for (byte[] bytes : request) {
            buffer.put(bytes);
        }

        return totalSize;
    }

    public CompletableFuture<RedisObject> sendRequest(RedisObject request) {
        if (!connected) {
            return NioFutures.completedExceptionally(new IOException("Redis client not connected"));
        }

        CompletableFuture<RedisObject> responseFuture = new CompletableFuture<RedisObject>();

        try {
            byte[][] requestBytes = encoder.encode(request);

            synchronized (requestQueue) {
                synchronized (responseQueue) {
                    responseQueue.add(responseFuture);
                }
                requestQueue.add(requestBytes);

                if (readyToSend) {
                    readyToSend = false;
                    CompletableFuture.runAsync(this::startWrite);
                }
            }
        } catch (RedisEncodeException e) {
            responseFuture.completeExceptionally(e);
        }

        return responseFuture;
    }
}
