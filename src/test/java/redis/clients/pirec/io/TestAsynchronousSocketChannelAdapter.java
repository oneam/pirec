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
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

public class TestAsynchronousSocketChannelAdapter {

    @Test
    public void test_adapter() throws Exception {
        AsynchronousServerSocketChannel server = AsynchronousServerSocketChannel.open();
        server.bind(null);
        InetSocketAddress local = (InetSocketAddress) server.getLocalAddress();
        int port = local.getPort();

        CompletableFuture<AsynchronousSocketChannel> accept = NioFutures.get(server::accept);
        accept.thenCompose(s -> {
            ByteBuffer buffer = ByteBuffer.allocate(65536);
            CompletableFuture<Integer> read = NioFutures.apply(s::read, buffer);
            return read.<Void> thenCompose(r -> {
                buffer.flip();
                CompletableFuture<Integer> write = NioFutures.apply(s::write, buffer);
                return write.thenAccept(w -> {
                    closeQuietly(s);
                });
            });
        });

        Random r = new Random();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        r.nextBytes(buffer.array());

        AsynchronousSocketChannelAdapter adapter = new AsynchronousSocketChannelAdapter();
        adapter.connect(new InetSocketAddress(port)).get(2, TimeUnit.SECONDS);
        adapter.write(buffer).get(2, TimeUnit.SECONDS);
        buffer.clear();
        adapter.read(buffer).get(2, TimeUnit.SECONDS);
        adapter.close();
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void test_adapter_fails() throws Exception {
        AsynchronousSocketChannelAdapter adapter = new AsynchronousSocketChannelAdapter();
        adapter.connect(new InetSocketAddress(0)).get(2, TimeUnit.SECONDS);
        adapter.close();
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void test_adapter_write_not_connected() throws Exception {
        AsynchronousSocketChannelAdapter adapter = new AsynchronousSocketChannelAdapter();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        adapter.write(buffer).get();
        adapter.close();
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void test_adapter_read_not_connected() throws Exception {
        AsynchronousSocketChannelAdapter adapter = new AsynchronousSocketChannelAdapter();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        adapter.read(buffer).get();
        adapter.close();
    }

    void closeQuietly(AsynchronousSocketChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
