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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import redis.clients.pirec.codec.object.RedisObject;

public class TestClient {

    @Test
    public void test_client_sync_request_response() throws Exception {
        int numRequests = 10;
        RedisObject request = RedisObject.array(RedisObject.bulkString("PING"));
        RedisObject response = RedisObject.simple("PONG");
        TestServer server = new TestServer();
        server.put(request, response);
        InetSocketAddress serverAddress = server.start();

        RedisClient client = new RedisClient();
        client.connect(serverAddress).get();

        for (int i = 0; i < numRequests; ++i) {
            CompletableFuture<RedisObject> responseFuture = client.sendRequest(request);
            RedisObject actual = responseFuture.get(2, TimeUnit.SECONDS);
            Assert.assertEquals(actual, response, "response");
        }

        client.disconnect();
    }

    @Test
    public void test_client_async_request_response() throws Exception {
        int numRequests = 10000;
        RedisObject request = RedisObject.array(RedisObject.bulkString("PING"));
        RedisObject response = RedisObject.simple("PONG");
        TestServer server = new TestServer();
        server.put(request, response);
        InetSocketAddress serverAddress = server.start();

        RedisClient client = new RedisClient();
        client.connect(serverAddress).get();

        ArrayList<CompletableFuture<RedisObject>> responseFutures = new ArrayList<CompletableFuture<RedisObject>>(
                numRequests);
        for (int i = 0; i < numRequests; ++i) {
            CompletableFuture<RedisObject> responseFuture = client.sendRequest(request);
            responseFutures.add(responseFuture);
        }

        client.numActiveRequests();

        for (CompletableFuture<RedisObject> responseFuture : responseFutures) {
            RedisObject actual = responseFuture.get(2, TimeUnit.SECONDS);
            Assert.assertEquals(actual, response, "response");
        }

        client.disconnect();
    }

    @Test
    public void test_client_parallel_request_response() throws Exception {
        int numRequests = 1000;
        RedisObject request = RedisObject.array(RedisObject.bulkString("PING"));
        RedisObject response = RedisObject.simple("PONG");
        TestServer server = new TestServer();
        server.put(request, response);
        InetSocketAddress serverAddress = server.start();

        RedisClient client = new RedisClient();
        client.connect(serverAddress).get();

        CountDownLatch done = new CountDownLatch(numRequests);

        for (int i = 0; i < numRequests; ++i) {
            CompletableFuture.runAsync(() -> {
                CompletableFuture<RedisObject> responseFuture = client.sendRequest(request);
                responseFuture.whenComplete((actual, e) -> {
                    Assert.assertNull(e, "Response exception");
                    Assert.assertEquals(actual, response, "response");
                    done.countDown();
                });
            });
        }

        boolean success = done.await(10, TimeUnit.SECONDS);
        Assert.assertTrue(success, "success");
    }

    @Test
    public void test_client_error() throws Exception {
        RedisObject request = RedisObject.array(RedisObject.bulkString("PING"));
        RedisObject response = RedisObject.error("Response not found");
        TestServer server = new TestServer();
        InetSocketAddress serverAddress = server.start();

        RedisClient client = new RedisClient();
        client.connect(serverAddress).get();

        CompletableFuture<RedisObject> responseFuture = client.sendRequest(request);
        RedisObject actual = responseFuture.get(2, TimeUnit.SECONDS);
        Assert.assertEquals(actual, response, "response");

        client.disconnect();
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void test_client_not_connected() throws Exception {
        RedisObject request = RedisObject.array(RedisObject.bulkString("PING"));

        RedisClient client = new RedisClient();

        CompletableFuture<RedisObject> responseFuture = client.sendRequest(request);
        responseFuture.get(2, TimeUnit.SECONDS);
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void test_client_bad_response() throws Exception {
        RedisObject request = RedisObject.array(RedisObject.bulkString("PING"));
        RedisObject response = new InvalidObject();
        TestServer server = new TestServer();
        server.put(request, response);
        InetSocketAddress serverAddress = server.start();

        RedisClient client = new RedisClient();
        client.connect(serverAddress).get();

        CompletableFuture<RedisObject> responseFuture = client.sendRequest(request);
        RedisObject actual = responseFuture.get(2, TimeUnit.SECONDS);
        Assert.assertEquals(actual, response, "response");

        client.disconnect();
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void test_client_bad_request() throws Exception {
        RedisObject request = new InvalidObject();
        TestServer server = new TestServer();
        InetSocketAddress serverAddress = server.start();

        RedisClient client = new RedisClient();
        client.connect(serverAddress).get();

        CompletableFuture<RedisObject> responseFuture = client.sendRequest(request);
        responseFuture.get(2, TimeUnit.SECONDS);
    }

    class InvalidObject extends RedisObject {
    }
}
