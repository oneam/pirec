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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import redis.clients.pirec.codec.Decoder.DecoderException;
import redis.clients.pirec.codec.object.RedisObject;

public class TestDecoder {

    @Test(dataProvider = "data_redis_decoder")
    public void test_redis_decoder(String description, byte[] input, RedisObject object) throws DecoderException {
        // Test all portions of input (from 0 bytes to the full message)
        for (int i = 0; i <= input.length; ++i) {
            RedisDecoder decoder = new RedisDecoder();

            byte[] bytes = Arrays.copyOfRange(input, 0, i);

            // Result should be null for all variations except complete message
            RedisObject expected = i == input.length ? object : null;

            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            RedisObject actual = decoder.decode(buffer);
            Assert.assertEquals(actual, expected, "actual");
        }
    }

    @DataProvider(name = "data_redis_decoder")
    Object[][] data_redis_decoder() {
        List<Object[]> tests = new ArrayList<>();

        {
            byte[] bytes = "+TEST\r\n".getBytes(StandardCharsets.UTF_8);
            RedisObject expected = RedisObject.simple("TEST");
            tests.add(new Object[] { "Simple string", bytes, expected });
        }

        {
            byte[] bytes = "-Error\r\n".getBytes(StandardCharsets.UTF_8);
            RedisObject expected = RedisObject.error("Error");
            tests.add(new Object[] { "Error", bytes, expected });
        }

        {
            byte[] bytes = ":1000\r\n".getBytes(StandardCharsets.UTF_8);
            RedisObject expected = RedisObject.integer(1000);
            tests.add(new Object[] { "Number", bytes, expected });
        }

        {
            byte[] bytes = "$4\r\nTEST\r\n".getBytes(StandardCharsets.UTF_8);
            RedisObject expected = RedisObject.bulkString("TEST".getBytes(StandardCharsets.UTF_8));
            tests.add(new Object[] { "Bulk string", bytes, expected });
        }

        {
            byte[] bytes = "$-1\r\n".getBytes(StandardCharsets.UTF_8);
            RedisObject expected = RedisObject.nullBulkString();
            tests.add(new Object[] { "Null Bulk string", bytes, expected });
        }

        {
            byte[] bytes = "*-1\r\n".getBytes(StandardCharsets.UTF_8);
            RedisObject expected = RedisObject.nullArray();
            tests.add(new Object[] { "Null Array", bytes, expected });
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
            RedisObject expected = RedisObject.array(simple, error, integer, bulkString, nullBulkString, nullArray);
            tests.add(new Object[] { "Array", bytes, expected });
        }

        return tests.toArray(new Object[0][]);
    }

    @Test(dataProvider = "data_redis_decoder_fail", expectedExceptions = {
            DecoderException.class,
            NumberFormatException.class })
    public void test_redis_decoder_fail(String description, byte[] input) throws DecoderException {
        Decoder<RedisObject> parser = RedisDecoder.newDecoder();
        ByteBuffer buffer = ByteBuffer.wrap(input);
        RedisObject actual = parser.decode(buffer);
        Assert.assertEquals(actual, null);
    }

    @DataProvider(name = "data_redis_decoder_fail")
    Object[][] data_redis_decoder_fail() {
        List<Object[]> tests = new ArrayList<>();

        {
            byte[] bytes = "=Nothing\r\n".getBytes(StandardCharsets.UTF_8);
            tests.add(new Object[] { "Invalid start byte", bytes });
        }

        {
            byte[] bytes = ":\r\n".getBytes(StandardCharsets.UTF_8);
            tests.add(new Object[] { "Empty integer", bytes });
        }

        {
            byte[] bytes = ":bad integer\r\n".getBytes(StandardCharsets.UTF_8);
            tests.add(new Object[] { "Bad integer", bytes });
        }

        {
            byte[] bytes = "$3\r\nTEST\r\n".getBytes(StandardCharsets.UTF_8);
            tests.add(new Object[] { "Bulk string too long", bytes });
        }

        {
            byte[] bytes = "$5\r\nTEST\r\n$".getBytes(StandardCharsets.UTF_8);
            tests.add(new Object[] { "Bulk string too short", bytes });
        }

        {
            byte[] bytes = "$bad\r\nTEST\r\n".getBytes(StandardCharsets.UTF_8);
            tests.add(new Object[] { "Bulk string bad integer", bytes });
        }

        {
            byte[] bytes = "*bad\r\n+TEST\r\n-Error\r\n:1000\r\n$4\r\nTEST\r\n$-1\r\n".getBytes(StandardCharsets.UTF_8);
            tests.add(new Object[] { "Array bad integer", bytes });
        }

        {
            StringBuilder builder = new StringBuilder();
            builder.append("+");
            for (int i = 0; i < 4096; ++i) {
                builder.append("long");
            }
            builder.append("\r\n");

            byte[] bytes = builder.toString().getBytes(StandardCharsets.UTF_8);
            tests.add(new Object[] { "Delimited too long", bytes });
        }

        return tests.toArray(new Object[0][]);
    }

    /**
     * Not a helpful unit test on its own. Mostly here for informational purposes.
     */
    @Test
    public void test_redis_decoder_performance() throws DecoderException {
        byte[] bytes = "*5\r\n+TEST\r\n-Error\r\n:1000\r\n$4\r\nTEST\r\n$-1\r\n".getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1048576);
        while (buffer.remaining() >= bytes.length) {
            buffer.put(bytes);
        }
        buffer.flip();

        RedisDecoder decoder = new RedisDecoder();

        long count = 0;
        long start = System.nanoTime();
        while (buffer.hasRemaining()) {
            RedisObject object = decoder.decode(buffer);
            Assert.assertNotNull(object, "Decoded object");
            count += 6; // 6 RedisObjects (5 + array)
        }
        long end = System.nanoTime();

        Duration duration = Duration.ofNanos(end - start);
        long rate = count * 1000000000L / duration.toNanos();
        System.out.format("Decoded %d objects in %s. %d/s\n", count, duration, rate);
    }
}
