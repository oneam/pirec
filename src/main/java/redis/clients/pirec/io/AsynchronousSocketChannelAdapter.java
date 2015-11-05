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
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.CompletableFuture;

public class AsynchronousSocketChannelAdapter implements SocketAdapter {

    AsynchronousSocketChannel socket;

    @Override
    public CompletableFuture<Void> connect(SocketAddress remote) {
        try {
            socket = AsynchronousSocketChannel.open();
        } catch (IOException e) {
            return NioFutures.completedExceptionally(e);
        }
        return NioFutures.apply(socket::connect, remote);
    }

    @Override
    public CompletableFuture<Integer> write(ByteBuffer src) {
        if (socket == null) return NioFutures.completedExceptionally(new IOException("Socket not connected"));
        return NioFutures.apply(socket::write, src);
    }

    @Override
    public CompletableFuture<Integer> read(ByteBuffer dst) {
        if (socket == null) return NioFutures.completedExceptionally(new IOException("Socket not connected"));
        return NioFutures.apply(socket::read, dst);
    }

    @Override
    public void close() throws Exception {
        socket.close();
    }
}
