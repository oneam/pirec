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
package redis.clients.pirec.codec;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import redis.clients.pirec.codec.RedisEncoder.RedisEncodeException;
import redis.clients.pirec.codec.object.RedisObject;

public class TestEncoder {

    @Test(dataProvider = "data_redis_encoder")
    public void test_redis_encoder(String desc, byte[] bytes, RedisObject object) throws RedisEncodeException {
        RedisEncoder encoder = new RedisEncoder();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int bytesWritten = encoder.encode(object, buffer);

        Assert.assertEquals(bytesWritten, bytes.length, "bytes written");

        byte[] actual = Arrays.copyOf(buffer.array(), bytesWritten);
        Assert.assertEquals(actual, bytes, "output");
    }

    @Test(dataProvider = "data_redis_encoder")
    public void test_redis_encoder_byte_array(String desc, byte[] bytes, RedisObject object)
            throws RedisEncodeException {
        RedisEncoder encoder = new RedisEncoder();
        byte[][] encodedBytes = encoder.encode(object);

        int totalSize = Arrays.stream(encodedBytes).mapToInt(Array::getLength).sum();
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        for (byte[] chunk : encodedBytes) {
            buffer.put(chunk);
        }
        byte[] actual = buffer.array();

        Assert.assertEquals(actual, bytes, "combined output");
    }

    @Test(dataProvider = "data_redis_encoder")
    public void test_redis_encoder_buffer_not_big_enough(String desc, byte[] bytes, RedisObject object)
            throws RedisEncodeException {
        RedisEncoder encoder = new RedisEncoder();
        ByteBuffer buffer = ByteBuffer.allocate(4);
        int bytesWritten = encoder.encode(object, buffer);

        Assert.assertEquals(bytesWritten, 0, "bytes written");
        Assert.assertEquals(buffer.remaining(), 4, "byte buffer size remaining");
    }

    @DataProvider(name = "data_redis_encoder")
    Object[][] data_redis_encoder() {
        List<Object[]> tests = new ArrayList<>();

        {
            byte[] bytes = "+TEST\r\n".getBytes(StandardCharsets.UTF_8);
            RedisObject object = RedisObject.simple("TEST");
            tests.add(new Object[] { "Simple string", bytes, object });
        }

        {
            byte[] bytes = "-Error\r\n".getBytes(StandardCharsets.UTF_8);
            RedisObject object = RedisObject.error("Error");
            tests.add(new Object[] { "Error", bytes, object });
        }

        {
            byte[] bytes = ":1000\r\n".getBytes(StandardCharsets.UTF_8);
            RedisObject object = RedisObject.integer(1000);
            tests.add(new Object[] { "Number", bytes, object });
        }

        {
            byte[] bytes = "$4\r\nTEST\r\n".getBytes(StandardCharsets.UTF_8);
            RedisObject object = RedisObject.bulkString("TEST".getBytes(StandardCharsets.UTF_8));
            tests.add(new Object[] { "Bulk string", bytes, object });
        }

        {
            byte[] bytes = "$-1\r\n".getBytes(StandardCharsets.UTF_8);
            RedisObject object = RedisObject.nullBulkString();
            tests.add(new Object[] { "Null", bytes, object });
        }

        {
            byte[] bytes = "*-1\r\n".getBytes(StandardCharsets.UTF_8);
            RedisObject object = RedisObject.nullArray();
            tests.add(new Object[] { "Null array", bytes, object });
        }

        {
            byte[] bytes = "*6\r\n+TEST\r\n-Error\r\n:1000\r\n$4\r\nTEST\r\n$-1\r\n*-1\r\n"
                    .getBytes(StandardCharsets.UTF_8);
            RedisObject simple = RedisObject.simple("TEST");
            RedisObject error = RedisObject.error("Error");
            RedisObject integer = RedisObject.integer(1000);
            RedisObject bulkString = RedisObject.bulkString("TEST".getBytes(StandardCharsets.UTF_8));
            RedisObject nullBulkString = RedisObject.nullBulkString();
            RedisObject nullArray = RedisObject.nullArray();
            RedisObject object = RedisObject.array(simple, error, integer, bulkString, nullBulkString, nullArray);
            tests.add(new Object[] { "Array", bytes, object });
        }

        return tests.toArray(new Object[0][]);
    }

    @Test(expectedExceptions = RedisEncoder.RedisEncodeException.class)
    public void test_redis_encoder_fails() throws RedisEncodeException {
        RedisEncoder encoder = new RedisEncoder();
        RedisObject object = new InvalidRedisObject();
        encoder.encode(object);
    }

    /**
     * Not a helpful unit test on its own. Mostly here for informational purposes.
     */
    @Test
    public void test_redis_encoder_performance() throws RedisEncodeException {
        RedisObject simple = RedisObject.simple("TEST");
        RedisObject error = RedisObject.error("Error");
        RedisObject integer = RedisObject.integer(1000);
        RedisObject bulkString = RedisObject.bulkString("TEST".getBytes(StandardCharsets.UTF_8));
        RedisObject nullObject = RedisObject.nullBulkString();
        RedisObject object = RedisObject.array(simple, error, integer, bulkString, nullObject);

        ByteBuffer buffer = ByteBuffer.allocate(1048576);
        RedisEncoder encoder = new RedisEncoder();

        long count = 0;
        long start = System.nanoTime();
        while (encoder.encode(object, buffer) > 0) {
            count += 6; // 6 RedisObjects (5 + array)
        }
        long end = System.nanoTime();

        Duration duration = Duration.ofNanos(end - start);
        long rate = count * 1000000000L / duration.toNanos();
        System.out.format("Encoded %d objects in %s. %d/s\n", count, duration, rate);
    }

    class InvalidRedisObject extends RedisObject {

    }
}
