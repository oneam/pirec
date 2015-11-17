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
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import redis.clients.pirec.codec.Decoder.DecoderException;
import redis.clients.pirec.codec.RedisDecoder;
import redis.clients.pirec.codec.RedisEncoder;
import redis.clients.pirec.codec.RedisEncoder.RedisEncodeException;
import redis.clients.pirec.codec.object.RedisObject;

public class TestServer {

    final ConcurrentHashMap<RedisObject, byte[][]> responses = new ConcurrentHashMap<>();
    final RedisDecoder decoder = new RedisDecoder();
    final RedisEncoder encoder = new RedisEncoder();

    ServerSocketChannel serverSocket;
    Thread acceptThread;

    public InetSocketAddress start() throws IOException {
        serverSocket = ServerSocketChannel.open();
        serverSocket.bind(null);
        acceptThread = new Thread(this::acceptLoop);
        acceptThread.start();

        return (InetSocketAddress) serverSocket.getLocalAddress();
    }

    private void acceptLoop() {
        try {
            while (true) {
                SocketChannel clientSocket;
                clientSocket = serverSocket.accept();
                Thread clientThread = new Thread(() -> readLoop(clientSocket));
                clientThread.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readLoop(SocketChannel socket) {
        ByteBuffer readBuffer = ByteBuffer.allocate(65536);
        ByteBuffer writeBuffer = ByteBuffer.allocate(65536);

        try {
            while (true) {
                int bytesRead = socket.read(readBuffer);
                if (bytesRead == 0) return;
                readBuffer.flip();
                RedisObject request = decoder.decode(readBuffer);
                while (request != null) {
                    decoder.reset();
                    byte[][] response = responses.get(request);
                    if (response == null) {
                        response = encoder.encode(RedisObject.error("Response not found"));
                    }
                    int responseBytes = Arrays.stream(response).mapToInt(Array::getLength).sum();
                    if (responseBytes > writeBuffer.remaining()) {
                        writeBuffer.flip();
                        socket.write(writeBuffer);
                        writeBuffer.clear();
                    } else {
                        request = decoder.decode(readBuffer);
                    }
                }
                writeBuffer.flip();
                if (writeBuffer.hasRemaining()) {
                    socket.write(writeBuffer);
                    writeBuffer.clear();
                }
                readBuffer.compact();
            }
        } catch (IOException | DecoderException | RedisEncodeException e) {
            e.printStackTrace();
        }
    }

    public void put(RedisObject request, RedisObject response) {
        try {
            responses.put(request, encoder.encode(response));
        } catch (RedisEncodeException e) {
            throw new RuntimeException(e);
        }
    }

    public void put(RedisObject request, byte[][] response) {
        responses.put(request, response);
    }
}
