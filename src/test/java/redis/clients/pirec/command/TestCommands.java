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
package redis.clients.pirec.command;

import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import org.testng.Assert;
import org.testng.annotations.Test;

import redis.clients.pirec.codec.object.RedisObject;
import redis.clients.pirec.command.RedisCommands;

public class TestCommands {
    static final String COMMAND = "command";
    static final String STRING_PARAM = "stringParam";
    static final byte[] BYTES_PARAM = "bytesParam".getBytes(StandardCharsets.UTF_8);

    @Test
    public void test_createRequest_simple() {
        RedisObject expected = RedisObject.array(RedisObject.bulkString(COMMAND));
        RedisObject actual = RedisCommands.createRequest(COMMAND);
        Assert.assertEquals(actual, expected);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void test_createRequest_simple_null() {
        RedisCommands.createRequest(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void test_createRequest_simple_empty() {
        RedisCommands.createRequest("");
    }

    @Test
    public void test_createRequest_string() {
        RedisObject expected = RedisObject.array(
                RedisObject.bulkString(COMMAND),
                RedisObject.bulkString(STRING_PARAM));
        RedisObject actual = RedisCommands.createRequest(COMMAND, STRING_PARAM);
        Assert.assertEquals(actual, expected);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void test_createRequest_string_null() {
        RedisCommands.createRequest(null, STRING_PARAM);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void test_createRequest_string_null_param() {
        RedisCommands.createRequest(COMMAND, (String) null);
    }

    @Test
    public void test_createRequest_bytes() {
        RedisObject expected = RedisObject.array(
                RedisObject.bulkString(COMMAND),
                RedisObject.bulkString(BYTES_PARAM));
        RedisObject actual = RedisCommands.createRequest(COMMAND, BYTES_PARAM);
        Assert.assertEquals(actual, expected);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void test_createRequest_bytes_null() {
        RedisCommands.createRequest(null, BYTES_PARAM);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void test_createRequest_bytes_null_param() {
        RedisCommands.createRequest(COMMAND, (byte[]) null);
    }

    @Test
    public void test_bulkStringResponseAsBytes() {
        RedisObject response = RedisObject.bulkString(BYTES_PARAM);
        byte[] actual = RedisCommands.bulkStringResponseAsBytes(response);
        Assert.assertEquals(actual, BYTES_PARAM);
    }

    @Test
    public void test_bulkStringResponseAsBytes_null_response() {
        RedisObject response = RedisObject.nullBulkString();
        String actual = RedisCommands.bulkStringResponseAsString(response);
        Assert.assertNull(actual);
    }

    @Test(expectedExceptions = UncheckedIOException.class)
    public void test_bulkStringResponseAsBytes_error() {
        RedisObject response = RedisObject.error("error");
        RedisCommands.bulkStringResponseAsBytes(response);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void test_bulkStringResponseAsBytes_null() {
        RedisCommands.bulkStringResponseAsBytes(null);
    }

    @Test(expectedExceptions = UncheckedIOException.class)
    public void test_bulkStringResponseAsBytes_wrongType() {
        RedisObject response = RedisObject.integer(-1);
        RedisCommands.bulkStringResponseAsBytes(response);
    }

    @Test
    public void test_bulkStringResponseAsString() {
        RedisObject response = RedisObject.bulkString(STRING_PARAM);
        String actual = RedisCommands.bulkStringResponseAsString(response);
        Assert.assertEquals(actual, STRING_PARAM);
    }

    @Test
    public void test_bulkStringResponseAsString_null_response() {
        RedisObject response = RedisObject.nullBulkString();
        String actual = RedisCommands.bulkStringResponseAsString(response);
        Assert.assertNull(actual);
    }

    @Test(expectedExceptions = UncheckedIOException.class)
    public void test_bulkStringResponseAsString_error() {
        RedisObject response = RedisObject.error("error");
        RedisCommands.bulkStringResponseAsString(response);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void test_bulkStringResponseAsString_null() {
        RedisCommands.bulkStringResponseAsString(null);
    }

    @Test(expectedExceptions = UncheckedIOException.class)
    public void test_bulkStringResponseAsString_wrongType() {
        RedisObject response = RedisObject.integer(-1);
        RedisCommands.bulkStringResponseAsString(response);
    }

    @Test
    public void test_simpleResponse() {
        RedisObject response = RedisObject.simple(STRING_PARAM);
        String actual = RedisCommands.simpleResponse(response);
        Assert.assertEquals(actual, STRING_PARAM);
    }

    @Test(expectedExceptions = UncheckedIOException.class)
    public void test_simpleResponse_error() {
        RedisObject response = RedisObject.error("error");
        RedisCommands.simpleResponse(response);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void test_simpleResponse_null() {
        RedisCommands.simpleResponse(null);
    }

    @Test(expectedExceptions = UncheckedIOException.class)
    public void test_simpleResponse_wrongType() {
        RedisObject response = RedisObject.integer(-1);
        RedisCommands.simpleResponse(response);
    }

    @Test
    public void test_simpleOrNullResponse() {
        RedisObject response = RedisObject.simple(STRING_PARAM);
        String actual = RedisCommands.simpleOrNullResponse(response);
        Assert.assertEquals(actual, STRING_PARAM);
    }

    @Test
    public void test_simpleOrNullResponse_null_response() {
        RedisObject response = RedisObject.nullBulkString();
        String actual = RedisCommands.simpleOrNullResponse(response);
        Assert.assertNull(actual);
    }

    @Test(expectedExceptions = UncheckedIOException.class)
    public void test_simpleOrNullResponse_error() {
        RedisObject response = RedisObject.error("error");
        RedisCommands.simpleOrNullResponse(response);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void test_simpleOrNullResponse_null() {
        RedisCommands.simpleOrNullResponse(null);
    }

    @Test(expectedExceptions = UncheckedIOException.class)
    public void test_simpleOrNullResponse_wrongType() {
        RedisObject response = RedisObject.integer(-1);
        RedisCommands.simpleOrNullResponse(response);
    }

    @Test
    public void test_integerResponse() {
        RedisObject response = RedisObject.integer(1);
        long actual = RedisCommands.integerResponseAsLong(response);
        Assert.assertEquals(actual, 1);
    }

    @Test(expectedExceptions = UncheckedIOException.class)
    public void test_integerResponse_error() {
        RedisObject response = RedisObject.error("error");
        RedisCommands.integerResponseAsLong(response);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void test_integerResponse_null() {
        RedisCommands.integerResponseAsLong(null);
    }

    @Test(expectedExceptions = UncheckedIOException.class)
    public void test_integerResponse_wrongType() {
        RedisObject response = RedisObject.nullBulkString();
        RedisCommands.integerResponseAsLong(response);
    }
}
