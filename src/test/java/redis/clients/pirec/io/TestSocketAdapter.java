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

    Object readSync = new Object();
    ByteBuffer readBuffer;
    CompletableFuture<Integer> readFuture;

    public void put(RedisObject request, RedisObject response) {
        responses.put(request, response);
    }

    public void setNumResponsesPerPacket(int responsesPerPacket) {

    }

    public void close() {
        synchronized (readSync) {
            CompletableFuture<Integer> readFuture = this.readFuture;
            this.readFuture = null;
            this.readBuffer = null;
            if (readFuture != null) readFuture.complete(0);
        }
    }

    @Override
    public CompletableFuture<Void> connect(SocketAddress remote) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Integer> write(ByteBuffer src) {
        try {
            Thread.sleep(10); // Simulate some network lag

            // Read requests and find responses
            int bytesWritten = src.remaining();
            int bytesRead = 0;
            RedisObject request = decoder.decode(src);
            int requestsProcessed = 0;
            while (request != null) {
                decoder.reset();
                ++requestsProcessed;
                RedisObject response = responses.get(request);
                if (response == null) {
                    response = RedisObject.error("Response not found");
                }
                bytesRead += encoder.encode(response, readBuffer);
                request = decoder.decode(src);
            }

            if (requestsProcessed > 1) System.out.format("Requests processed %d\n", requestsProcessed);

            // Complete response futures
            if (bytesRead > 0) {
                synchronized (readSync) {
                    CompletableFuture<Integer> readFuture = this.readFuture;
                    this.readFuture = null;
                    this.readBuffer = null;
                    if (readFuture != null) readFuture.complete(bytesRead);
                }
            }
            return CompletableFuture.completedFuture(bytesWritten);
        } catch (DecoderException | RedisEncodeException | InterruptedException e) {
            e.printStackTrace();
            return NioFutures.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Integer> read(ByteBuffer dst) {
        synchronized (readSync) {
            readFuture = new CompletableFuture<>();
            readBuffer = dst;
        }
        return readFuture;
    }
}
