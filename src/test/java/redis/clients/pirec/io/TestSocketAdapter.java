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

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

import redis.clients.pirec.codec.Decoder.DecoderException;
import redis.clients.pirec.codec.RedisDecoder;
import redis.clients.pirec.codec.RedisEncoder;
import redis.clients.pirec.codec.RedisEncoder.RedisEncodeException;
import redis.clients.pirec.codec.object.RedisObject;

public class TestSocketAdapter implements SocketAdapter {

    final Map<RedisObject, RedisObject> responses = new HashMap<>();
    final RedisDecoder decoder = new RedisDecoder();
    final RedisEncoder encoder = new RedisEncoder();
    int responsesPerPacket = 1;
    int responsesPending = 0;

    Object bufferSync = new Object();
    ByteBuffer readBuffer;
    ByteBuffer writeBuffer;
    CompletableFuture<Integer> readFuture;
    CompletableFuture<Integer> writeFuture;

    public void put(RedisObject request, RedisObject response) {
        responses.put(request, response);
    }

    public void close() {
        synchronized (bufferSync) {
            CompletableFuture<Integer> readFuture = this.readFuture;
            this.readFuture = null;
            this.readBuffer = null;
            if (readFuture != null) readFuture.complete(0);

            CompletableFuture<Integer> writeFuture = this.writeFuture;
            this.writeFuture = null;
            this.writeBuffer = null;
            if (writeFuture != null) writeFuture.complete(0);
        }
    }

    @Override
    public CompletableFuture<Void> connect(SocketAddress remote) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Integer> write(ByteBuffer src) {
        CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
        synchronized (bufferSync) {
            this.writeFuture = writeFuture;
            this.writeBuffer = src;
        }
        ForkJoinPool.commonPool().execute(this::readWrite);
        return writeFuture;
    }

    void readWrite() {
        ByteBuffer writeBuffer;
        ByteBuffer readBuffer;
        synchronized (bufferSync) {
            writeBuffer = this.writeBuffer;
            readBuffer = this.readBuffer;
            if (readBuffer == null) return;
            if (writeBuffer == null) return;

            try {
                // Read requests and find responses
                int bytesWritten = writeBuffer.remaining();
                int bytesRead = 0;
                RedisObject request = decoder.decode(writeBuffer);
                int requestsProcessed = 0;
                while (request != null) {
                    decoder.reset();
                    ++requestsProcessed;
                    RedisObject response = responses.get(request);
                    if (response == null) {
                        response = RedisObject.error("Response not found");
                    }
                    bytesRead += encoder.encode(response, readBuffer);
                    request = decoder.decode(writeBuffer);
                }

                if (requestsProcessed > 1) System.out.format("Requests processed %d\n", requestsProcessed);
                if (bytesRead > 0) completeRead(bytesRead);
                completeWrite(bytesWritten);
            } catch (DecoderException | RedisEncodeException e) {
                e.printStackTrace();
                completeWrite(e);
            }
        }
    }

    void completeWrite(int numBytes) {
        CompletableFuture<Integer> writeFuture;
        synchronized (bufferSync) {
            writeFuture = this.writeFuture;
            this.writeFuture = null;
            this.writeBuffer = null;
        }
        writeFuture.complete(numBytes);
    }

    void completeWrite(Throwable ex) {
        CompletableFuture<Integer> writeFuture;
        synchronized (bufferSync) {
            writeFuture = this.writeFuture;
            this.writeFuture = null;
            this.writeBuffer = null;
        }
        writeFuture.completeExceptionally(ex);
    }

    void completeRead(int numBytes) {
        CompletableFuture<Integer> readFuture;
        synchronized (bufferSync) {
            readFuture = this.readFuture;
            this.readFuture = null;
            this.readBuffer = null;
        }
        readFuture.complete(numBytes);
    }

    void completeRead(Throwable ex) {
        CompletableFuture<Integer> readFuture;
        synchronized (bufferSync) {
            readFuture = this.readFuture;
            this.readFuture = null;
            this.readBuffer = null;
        }
        readFuture.completeExceptionally(ex);
    }

    @Override
    public CompletableFuture<Integer> read(ByteBuffer dst) {
        CompletableFuture<Integer> readFuture = new CompletableFuture<>();
        synchronized (bufferSync) {
            this.readFuture = readFuture;
            this.readBuffer = dst;
        }
        ForkJoinPool.commonPool().execute(this::readWrite);
        return readFuture;
    }
}
