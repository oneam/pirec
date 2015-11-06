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
package redis.clients.pirec;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import redis.clients.pirec.codec.object.RedisArray;
import redis.clients.pirec.codec.object.RedisInteger;
import redis.clients.pirec.codec.object.RedisObject;
import redis.clients.pirec.commands.Aggregate;
import redis.clients.pirec.commands.BitOp;
import redis.clients.pirec.commands.KeyValueBytesPair;
import redis.clients.pirec.commands.KeyValuePair;
import redis.clients.pirec.commands.KeyWeightPair;
import redis.clients.pirec.commands.MemberScorePair;
import redis.clients.pirec.commands.ScanResult;
import redis.clients.pirec.io.RedisClient;

public class TestPirec {

    // When set, the parameters of these tests are verified with an actual Redis Server.
    static final boolean VERIFY_TESTS = false;

    // Server hostname used to verify tests.
    static final String SERVER_HOSTNAME = "localhost";

    // Server db used to verify tests.
    static final int VERIFY_DB = 3;

    static final String[] TEST_KEYS = new String[] { "hash_key", "list_key", "num_key", "set2_key", "set_key", "sset_key", "string_key" };
    static final KeyValuePair[] TEST_HASH = new KeyValuePair[] {
            new KeyValuePair("a", "1"),
            new KeyValuePair("b", "2"),
            new KeyValuePair("c", "3") };
    static final MemberScorePair[] TEST_SSET = new MemberScorePair[] {
            new MemberScorePair("a", 1),
            new MemberScorePair("b", 2),
            new MemberScorePair("c", 3) };
    static final String[] STRING_VALUES = new String[] { "a", "b", "c" };

    @Mock
    RedisClient client;

    Pirec pirec;

    void setupVerify(Pirec pirec) throws Exception {
        pirec.connect(SERVER_HOSTNAME).get(2, TimeUnit.SECONDS);
        pirec.select(1);
        pirec.del("string_key");
        pirec.select(VERIFY_DB);
        pirec.flushDb();
        pirec.set("string_key", "value");
        pirec.set("num_key", "10");
        pirec.rPush("list_key", STRING_VALUES);
        pirec.hMSet("hash_key", TEST_HASH);
        pirec.sAdd("set_key", STRING_VALUES);
        pirec.sAdd("set2_key", STRING_VALUES);
        pirec.zAdd("sset_key", TEST_SSET);
        pirec.echo("ready").get(2, TimeUnit.SECONDS);
    }

    @BeforeMethod
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        Pirec pirecWithoutSpy;
        if (VERIFY_TESTS) {
            pirecWithoutSpy = new Pirec();
            setupVerify(pirecWithoutSpy);
        } else {
            pirecWithoutSpy = new Pirec(client);
        }

        pirec = Mockito.spy(pirecWithoutSpy);
    }

    @Test
    public void test_connect() throws Exception {
        String hostname = SERVER_HOSTNAME;
        int port = 6379;
        InetSocketAddress expected = new InetSocketAddress(hostname, port);
        doReturn(CompletableFuture.completedFuture((Void) null)).when(client).connect(eq(expected));

        pirec.connect(hostname).get();
        pirec.connect(hostname, port).get();
        pirec.connect(expected).get();
        pirec.connectSync(hostname);
        pirec.connectSync(hostname, port);
        pirec.connectSync(expected);
    }

    @Test
    public void test_numActiveRequests() throws Exception {
        int expected = 0;
        doReturn(expected).when(client).numActiveRequests();
        int actual = pirec.numActiveRequests();
        assertEquals(actual, expected, "numActiveRequests");
    }

    @Test
    public void test_auth() throws Exception {
        if (VERIFY_TESTS) return; // Skipped  for verification
        String password = "password";
        String expected = "OK";
        RedisObject request = RedisObject.array(RedisObject.bulkString("AUTH"), RedisObject.bulkString(password));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.auth(password);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_echo() throws Exception {
        String expected = "expected";
        RedisObject request = RedisObject.array(RedisObject.bulkString("ECHO"), RedisObject.bulkString(expected));
        RedisObject response = RedisObject.bulkString(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.echo(expected);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_ping() throws Exception {
        String expected = "PONG";
        RedisObject request = RedisObject.array(RedisObject.bulkString("PING"));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.ping();
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_quit() throws Exception {
        String expected = "OK";
        RedisObject request = RedisObject.array(RedisObject.bulkString("QUIT"));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.quit();
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_select() throws Exception {
        String expected = "OK";
        int db = 1;
        RedisObject request = RedisObject.array(RedisObject.bulkString("SELECT"), RedisObject.bulkString(Integer.toString(db)));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.select(db);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_append() throws Exception {
        String key = "string_key";
        String value = "value";
        long expected = 10;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("APPEND"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.append(key, value);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_bitCount() throws Exception {
        String key = "string_key";
        long expected = 21;
        RedisObject request = RedisObject.array(RedisObject.bulkString("BITCOUNT"), RedisObject.bulkString(key));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.bitCount(key);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_bitCount_start_end() throws Exception {
        String key = "string_key";
        long start = 1;
        long end = 3;
        long expected = 12;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("BITCOUNT"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Long.toString(start)),
                RedisObject.bulkString(Long.toString(end)));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.bitCount(key, start, end);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_bitOp() throws Exception {
        String dstKey = "bitOpDest_key";
        String srcKey1 = "string_key";
        String srcKey2 = "string_key";
        BitOp op = BitOp.OR;
        long expected = 5;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("BITOP"),
                RedisObject.bulkString(op.toString()),
                RedisObject.bulkString(dstKey),
                RedisObject.bulkString(srcKey1),
                RedisObject.bulkString(srcKey2));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.bitOp(op, dstKey, srcKey1, srcKey2);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_bitPos() throws Exception {
        String key = "string_key";
        long expected = 1;
        RedisObject request = RedisObject.array(RedisObject.bulkString("BITPOS"), RedisObject.bulkString(key), RedisObject.bulkString("1"));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.bitPos(key, true);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_bitPos_start_end() throws Exception {
        String key = "string_key";
        long start = 1;
        long end = 3;
        long expected = 9;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("BITPOS"),
                RedisObject.bulkString(key),
                RedisObject.bulkString("1"),
                RedisObject.bulkString(Long.toString(start)),
                RedisObject.bulkString(Long.toString(end)));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.bitPos(key, true, start, end);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_decr() throws Exception {
        String key = "num_key";
        long expected = 9;
        RedisObject request = RedisObject.array(RedisObject.bulkString("DECR"), RedisObject.bulkString(key));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.decr(key);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_decrBy() throws Exception {
        String key = "num_key";
        long decrBy = 4;
        long expected = 6;
        RedisObject request = RedisObject.array(RedisObject.bulkString("DECRBY"), RedisObject.bulkString(key), RedisObject.bulkString(Long.toString(decrBy)));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.decrBy(key, decrBy);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_get() throws Exception {
        String key = "string_key";
        String expected = "value";
        RedisObject request = RedisObject.array(RedisObject.bulkString("GET"), RedisObject.bulkString(key));
        RedisObject response = RedisObject.bulkString(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.get(key);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_getAsBytes() throws Exception {
        String key = "string_key";
        byte[] expected = utf8Bytes("value");
        RedisObject request = RedisObject.array(RedisObject.bulkString("GET"), RedisObject.bulkString(key));
        RedisObject response = RedisObject.bulkString(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<byte[]> actualFuture = pirec.getAsBytes(key);
        verify(pirec).execute(request);
        byte[] actual = actualFuture.get();
        assertEquals(intValues(actual), intValues(expected), "response");
    }

    @Test
    public void test_getBit() throws Exception {
        String key = "string_key";
        int offset = 5;
        boolean expected = true;
        RedisObject request = RedisObject.array(RedisObject.bulkString("GETBIT"), RedisObject.bulkString(key), RedisObject.bulkString(Long.toString(offset)));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.getBit(key, offset);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_getRange() throws Exception {
        String key = "string_key";
        String expected = "alu";
        int start = 1;
        int end = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("GETRANGE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(start)),
                RedisObject.bulkString(Integer.toString(end)));
        RedisObject response = RedisObject.bulkString(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.getRange(key, start, end);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_getRangeAsBytes() throws Exception {
        String key = "string_key";
        byte[] expected = utf8Bytes("alu");
        int start = 1;
        int end = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("GETRANGE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(start)),
                RedisObject.bulkString(Integer.toString(end)));
        RedisObject response = RedisObject.bulkString(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<byte[]> actualFuture = pirec.getRangeAsBytes(key, start, end);
        verify(pirec).execute(request);
        byte[] actual = actualFuture.get();
        assertEquals(intValues(actual), intValues(expected), "response");
    }

    @Test
    public void test_getSet() throws Exception {
        String key = "string_key";
        String expected = "value";
        String value = "value2";
        RedisObject request = RedisObject.array(RedisObject.bulkString("GETSET"), RedisObject.bulkString(key), RedisObject.bulkString(value));
        RedisObject response = RedisObject.bulkString(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.getSet(key, value);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_getSetBytes() throws Exception {
        String key = "string_key";
        byte[] expected = utf8Bytes("value");
        byte[] value = utf8Bytes("value2");
        RedisObject request = RedisObject.array(RedisObject.bulkString("GETSET"), RedisObject.bulkString(key), RedisObject.bulkString(value));
        RedisObject response = RedisObject.bulkString(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<byte[]> actualFuture = pirec.getSetBytes(key, value);
        verify(pirec).execute(request);
        byte[] actual = actualFuture.get();
        assertEquals(intValues(actual), intValues(expected), "response");
    }

    @Test
    public void test_incr() throws Exception {
        String key = "num_key";
        long expected = 11;
        RedisObject request = RedisObject.array(RedisObject.bulkString("INCR"), RedisObject.bulkString(key));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.incr(key);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_incrBy() throws Exception {
        String key = "num_key";
        long incrBy = 4;
        long expected = 14;
        RedisObject request = RedisObject.array(RedisObject.bulkString("INCRBY"), RedisObject.bulkString(key), RedisObject.bulkString(Long.toString(incrBy)));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.incrBy(key, incrBy);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_incrByFloat() throws Exception {
        String key = "num_key";
        double incrBy = 4.5;
        double expected = 14.5;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("INCRBYFLOAT"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Double.toString(incrBy)));
        RedisObject response = RedisObject.bulkString(Double.toString(expected));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Double> actualFuture = pirec.incrByFloat(key, incrBy);
        verify(pirec).execute(request);
        double actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_mget() throws Exception {
        String[] keys = new String[] { "string_key", "num_key" };
        String[] expected = new String[] { "value", "10" };
        RedisObject request = RedisObject.array(RedisObject.bulkString("MGET"), RedisObject.bulkString(keys[0]), RedisObject.bulkString(keys[1]));
        RedisObject response = RedisObject.array(RedisObject.bulkString(expected[0]), RedisObject.bulkString(expected[1]));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.mGet(keys);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_mgetAsBytes() throws Exception {
        String[] keys = new String[] { "string_key", "num_key" };
        byte[][] expected = new byte[][] { utf8Bytes("value"), utf8Bytes("10") };
        RedisObject request = RedisObject.array(RedisObject.bulkString("MGET"), RedisObject.bulkString(keys[0]), RedisObject.bulkString(keys[1]));
        RedisObject response = RedisObject.array(RedisObject.bulkString(expected[0]), RedisObject.bulkString(expected[1]));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<byte[][]> actualFuture = pirec.mGetAsBytes(keys);
        verify(pirec).execute(request);
        byte[][] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_mset_string() throws Exception {
        KeyValuePair[] pairs = new KeyValuePair[] {
                new KeyValuePair("mset_key1", "value1"),
                new KeyValuePair("mset_key2", "value2") };
        String expected = "OK";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("MSET"),
                RedisObject.bulkString(pairs[0].getKey()),
                RedisObject.bulkString(pairs[0].getValue()),
                RedisObject.bulkString(pairs[1].getKey()),
                RedisObject.bulkString(pairs[1].getValue()));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.mSet(pairs);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_mset_bytes() throws Exception {
        KeyValueBytesPair[] pairs = new KeyValueBytesPair[] {
                new KeyValueBytesPair("mset_key1", utf8Bytes("value1")),
                new KeyValueBytesPair("mset_key2", utf8Bytes("value2")) };
        String expected = "OK";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("MSET"),
                RedisObject.bulkString(pairs[0].getKey()),
                RedisObject.bulkString(pairs[0].getValueBytes()),
                RedisObject.bulkString(pairs[1].getKey()),
                RedisObject.bulkString(pairs[1].getValueBytes()));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.mSet(pairs);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_msetNX_string() throws Exception {
        KeyValuePair[] pairs = new KeyValuePair[] {
                new KeyValuePair("mset_key1", "value1"),
                new KeyValuePair("mset_key2", "value2") };
        boolean expected = true;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("MSETNX"),
                RedisObject.bulkString(pairs[0].getKey()),
                RedisObject.bulkString(pairs[0].getValue()),
                RedisObject.bulkString(pairs[1].getKey()),
                RedisObject.bulkString(pairs[1].getValue()));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.mSetNX(pairs);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_msetNX_bytes() throws Exception {
        KeyValueBytesPair[] pairs = new KeyValueBytesPair[] {
                new KeyValueBytesPair("mset_key1", utf8Bytes("value1")),
                new KeyValueBytesPair("mset_key2", utf8Bytes("value2")) };
        boolean expected = true;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("MSETNX"),
                RedisObject.bulkString(pairs[0].getKey()),
                RedisObject.bulkString(pairs[0].getValueBytes()),
                RedisObject.bulkString(pairs[1].getKey()),
                RedisObject.bulkString(pairs[1].getValueBytes()));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.mSetNX(pairs);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_pSetEx_string() throws Exception {
        String key = "set_key";
        String value = "value";
        int expiration = 10000;
        String expected = "OK";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("PSETEX"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(expiration)),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.pSetEx(key, expiration, value);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_pSetEx_bytes() throws Exception {
        String key = "set_key";
        byte[] value = utf8Bytes("value");
        int expiration = 10000;
        String expected = "OK";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("PSETEX"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(expiration)),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.pSetEx(key, expiration, value);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_set_string() throws Exception {
        String key = "set_key";
        String value = "value";
        String expected = "OK";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SET"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.set(key, value);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_set_bytes() throws Exception {
        String key = "set_key";
        byte[] value = utf8Bytes("value");
        String expected = "OK";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SET"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.set(key, value);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_setBit() throws Exception {
        String key = "string_key";
        int offset = 7;
        boolean value = true;
        boolean expected = false;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SETBIT"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(offset)),
                RedisObject.bulkString(value ? "1" : "0"));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.setBit(key, offset, value);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_setEx_string() throws Exception {
        String key = "unset_key";
        String value = "value";
        int expiration = 10;
        String expected = "OK";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SETEX"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(expiration)),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.setEx(key, expiration, value);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_setEx_bytes() throws Exception {
        String key = "unset_key";
        byte[] value = utf8Bytes("value");
        int expiration = 10;
        String expected = "OK";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SETEX"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(expiration)),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.setEx(key, expiration, value);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_setNX_string() throws Exception {
        String key = "unset_key";
        String value = "value";
        boolean expected = true;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SETNX"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.setNX(key, value);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_setNX_bytes() throws Exception {
        String key = "unset_key";
        byte[] value = utf8Bytes("value");
        boolean expected = true;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SETNX"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.setNX(key, value);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_setRange_string() throws Exception {
        String key = "string_key";
        String value = "value";
        int offset = 3;
        long expected = 8;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SETRANGE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(offset)),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.setRange(key, offset, value);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_setRange_bytes() throws Exception {
        String key = "string_key";
        byte[] value = utf8Bytes("value");
        int expiration = 3;
        long expected = 8;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SETRANGE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(expiration)),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.setRange(key, expiration, value);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_strLen() throws Exception {
        String key = "string_key";
        long expected = 5;
        RedisObject request = RedisObject.array(RedisObject.bulkString("STRLEN"), RedisObject.bulkString(key));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.strLen(key);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_del() throws Exception {
        String[] keys = new String[] { "string_key", "num_key" };
        long expected = 2;
        RedisObject request = RedisObject.array(RedisObject.bulkString("DEL"), RedisObject.bulkString(keys[0]), RedisObject.bulkString(keys[1]));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.del(keys);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_dump() throws Exception {
        String key = "string_key";
        byte[] expected = new byte[] { 0, 5, 118, 97, 108, 117, 101, 6, 0, 23, 27, -87, -72, 52, -1, -89, -3 };
        RedisObject request = RedisObject.array(RedisObject.bulkString("DUMP"), RedisObject.bulkString(key));
        RedisObject response = RedisObject.bulkString(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<byte[]> actualFuture = pirec.dump(key);
        verify(pirec).execute(request);
        byte[] actual = actualFuture.get();
        assertEquals(intValues(actual), intValues(expected), "response");
    }

    @Test
    public void test_exists() throws Exception {
        String[] keys = new String[] { "string_key" };
        long expected = 1;
        RedisObject request = RedisObject.array(RedisObject.bulkString("EXISTS"), RedisObject.bulkString(keys[0]));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.exists(keys);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_expire() throws Exception {
        String key = "string_key";
        int seconds = 10;
        boolean expected = true;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("EXPIRE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(seconds)));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.expire(key, seconds);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_expireAt() throws Exception {
        String key = "string_key";
        long timestamp = Instant.now().plusSeconds(10).getEpochSecond();
        boolean expected = true;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("EXPIREAT"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Long.toString(timestamp)));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.expireAt(key, timestamp);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_keys() throws Exception {
        String pattern = "str?ng_key";
        String[] expected = new String[] { "string_key" };
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("KEYS"),
                RedisObject.bulkString(pattern));
        RedisObject response = RedisObject.array(RedisObject.bulkString(expected[0]));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.keys(pattern);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_move() throws Exception {
        String key = "string_key";
        int db = 1;
        boolean expected = true;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("MOVE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(db)));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.move(key, db);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_persist() throws Exception {
        String key = "string_key";
        boolean expected = false;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("PERSIST"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.persist(key);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_pExpire() throws Exception {
        String key = "string_key";
        int millis = 10000;
        boolean expected = true;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("PEXPIRE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(millis)));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.pExpire(key, millis);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_pExpireAt() throws Exception {
        String key = "string_key";
        long milliTimestamp = Instant.now().toEpochMilli() + 10000;
        boolean expected = true;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("PEXPIREAT"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Long.toString(milliTimestamp)));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.pExpireAt(key, milliTimestamp);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_pTTL() throws Exception {
        String key = "string_key";
        long expected = -1;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("PTTL"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.pTTL(key);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_randomKey() throws Exception {
        String expected = "randomKey";
        RedisObject request = RedisObject.array(RedisObject.bulkString("RANDOMKEY"));
        RedisObject response = RedisObject.bulkString(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.randomKey();
        verify(pirec).execute(request);
        String actual = actualFuture.get();

        if (VERIFY_TESTS) {
            assertNotNull(actual, "response not null");
        } else {
            assertEquals(actual, expected, "response");
        }
    }

    @Test
    public void test_rename() throws Exception {
        String key = "string_key";
        String newKey = "new_string_key";
        String expected = "OK";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("RENAME"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(newKey));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.rename(key, newKey);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_renameNX() throws Exception {
        String key = "string_key";
        String newKey = "new_string_key";
        boolean expected = true;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("RENAMENX"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(newKey));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.renameNX(key, newKey);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_restore() throws Exception {
        String key = "unset_key";
        int ttl = 0;
        byte[] value = new byte[] { 0, 5, 118, 97, 108, 117, 101, 6, 0, 23, 27, -87, -72, 52, -1, -89, -3 };
        String expected = "OK";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("RESTORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Long.toString(ttl)),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.restore(key, ttl, value);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_scan() throws Exception {
        String cursor = "0";
        ScanResult<String> expected = new ScanResult<>(cursor, TEST_KEYS);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SCAN"),
                RedisObject.bulkString(cursor));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(cursor),
                RedisObject.array(Arrays
                        .stream(TEST_KEYS)
                        .map(RedisObject::bulkString)
                        .toArray(RedisObject[]::new)));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<ScanResult<String>> actualFuture = pirec.scan(cursor);
        verify(pirec).execute(request);
        ScanResult<String> actual = actualFuture.get();
        Arrays.sort(actual.getValues());
        assertEquals(actual.getCursor(), expected.getCursor(), "response cursor");

    }

    @Test
    public void test_scan_match() throws Exception {
        String cursor = "0";
        String match = "*_key";
        ScanResult<String> expected = new ScanResult<>(cursor, TEST_KEYS);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SCAN"),
                RedisObject.bulkString(cursor),
                RedisObject.bulkString("MATCH"),
                RedisObject.bulkString(match));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(cursor),
                RedisObject.array(Arrays
                        .stream(TEST_KEYS)
                        .map(RedisObject::bulkString)
                        .toArray(RedisObject[]::new)));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<ScanResult<String>> actualFuture = pirec.scan(cursor, match);
        verify(pirec).execute(request);
        ScanResult<String> actual = actualFuture.get();
        Arrays.sort(actual.getValues());
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_scan_count() throws Exception {
        String cursor = "0";
        int count = 10;
        ScanResult<String> expected = new ScanResult<>(cursor, TEST_KEYS);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SCAN"),
                RedisObject.bulkString(cursor),
                RedisObject.bulkString("COUNT"),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(cursor),
                RedisObject.array(Arrays
                        .stream(TEST_KEYS)
                        .map(RedisObject::bulkString)
                        .toArray(RedisObject[]::new)));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<ScanResult<String>> actualFuture = pirec.scan(cursor, count);
        verify(pirec).execute(request);
        ScanResult<String> actual = actualFuture.get();
        Arrays.sort(actual.getValues());
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_scan_match_count() throws Exception {
        String cursor = "0";
        String match = "*_key";
        int count = 10;
        ScanResult<String> expected = new ScanResult<>(cursor, TEST_KEYS);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SCAN"),
                RedisObject.bulkString(cursor),
                RedisObject.bulkString("MATCH"),
                RedisObject.bulkString(match),
                RedisObject.bulkString("COUNT"),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(cursor),
                RedisObject.array(Arrays
                        .stream(TEST_KEYS)
                        .map(RedisObject::bulkString)
                        .toArray(RedisObject[]::new)));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<ScanResult<String>> actualFuture = pirec.scan(cursor, match, count);
        verify(pirec).execute(request);
        ScanResult<String> actual = actualFuture.get();
        Arrays.sort(actual.getValues());
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_sort() throws Exception {
        String key = "list_key";
        String[] expected = new String[] { "c", "b", "a" };
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SORT"),
                RedisObject.bulkString(key),
                RedisObject.bulkString("ALPHA"),
                RedisObject.bulkString("DESC"));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString("c"),
                RedisObject.bulkString("b"),
                RedisObject.bulkString("a"));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.sort(key, "ALPHA", "DESC");
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_ttl() throws Exception {
        String key = "string_key";
        long expected = -1;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("TTL"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.ttl(key);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_type() throws Exception {
        String key = "list_key";
        String expected = "list";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("TYPE"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.type(key);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_wait() throws Exception {
        int numSlaves = 1;
        int timeout = 100;
        int expected = 0;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("WAIT"),
                RedisObject.bulkString(Integer.toString(numSlaves)),
                RedisObject.bulkString(Integer.toString(timeout)));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.wait(numSlaves, timeout);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_bLPop() throws Exception {
        String key = "list_key";
        int timeout = 2;
        String value = "a";
        KeyValuePair expected = new KeyValuePair(key, value);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("BLPOP"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(timeout)));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(key),
                RedisObject.bulkString(value));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<KeyValuePair> actualFuture = pirec.bLPop(key, timeout);
        verify(pirec).execute(request);
        KeyValuePair actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_bRPop() throws Exception {
        String key = "list_key";
        int timeout = 2;
        String value = "c";
        KeyValuePair expected = new KeyValuePair(key, value);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("BRPOP"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(timeout)));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(key),
                RedisObject.bulkString(value));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<KeyValuePair> actualFuture = pirec.bRPop(key, timeout);
        verify(pirec).execute(request);
        KeyValuePair actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_bRPopLPush() throws Exception {
        String key = "list_key";
        int timeout = 2;
        String expected = "c";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("BRPOPLPUSH"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(timeout)));
        RedisObject response = RedisObject.bulkString(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.bRPopLPush(key, key, timeout);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_lIndex() throws Exception {
        String key = "list_key";
        int index = 1;
        String expected = "b";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("LINDEX"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(index)));
        RedisObject response = RedisObject.bulkString(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.lIndex(key, index);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_lInsertBefore() throws Exception {
        String key = "list_key";
        String pivot = "c";
        String value = "d";
        int expected = 4;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("LINSERT"),
                RedisObject.bulkString(key),
                RedisObject.bulkString("BEFORE"),
                RedisObject.bulkString(pivot),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.lInsertBefore(key, pivot, value);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_lInsertAfter() throws Exception {
        String key = "list_key";
        String pivot = "c";
        String value = "d";
        int expected = 4;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("LINSERT"),
                RedisObject.bulkString(key),
                RedisObject.bulkString("AFTER"),
                RedisObject.bulkString(pivot),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.lInsertAfter(key, pivot, value);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_lLen() throws Exception {
        String key = "list_key";
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("LLEN"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.lLen(key);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_lPop() throws Exception {
        String key = "list_key";
        String expected = "a";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("LPOP"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.bulkString(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.lPop(key);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_lPush() throws Exception {
        String key = "list_key";
        String value = "d";
        int expected = 4;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("LPUSH"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.lPush(key, value);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_lPushX() throws Exception {
        String key = "list_key";
        String value = "d";
        int expected = 4;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("LPUSHX"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.lPushX(key, value);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_lRange() throws Exception {
        String key = "list_key";
        int start = 0;
        int end = 1;
        String[] expected = new String[] { "a", "b" };
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("LRANGE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(start)),
                RedisObject.bulkString(Integer.toString(end)));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(expected[0]),
                RedisObject.bulkString(expected[1]));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.lRange(key, start, end);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_lRem() throws Exception {
        String key = "list_key";
        int count = 0;
        String value = "a";
        int expected = 1;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("LREM"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(count)),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.lRem(key, count, value);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_lSet() throws Exception {
        String key = "list_key";
        int index = 0;
        String value = "d";
        String expected = "OK";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("LSET"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(index)),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.lSet(key, index, value);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_lTrim() throws Exception {
        String key = "list_key";
        int start = 0;
        int stop = 1;
        String expected = "OK";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("LTRIM"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(start)),
                RedisObject.bulkString(Integer.toString(stop)));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.lTrim(key, start, stop);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_rPop() throws Exception {
        String key = "list_key";
        String expected = "c";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("RPOP"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.bulkString(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.rPop(key);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_rPush() throws Exception {
        String key = "list_key";
        String value = "d";
        int expected = 4;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("RPUSH"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.rPush(key, value);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_rPushX() throws Exception {
        String key = "list_key";
        String value = "d";
        int expected = 4;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("RPUSHX"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.rPushX(key, value);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_rPopLPush() throws Exception {
        String key = "list_key";
        String expected = "c";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("RPOPLPUSH"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.bulkString(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.rPopLPush(key, key);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hDel() throws Exception {
        String key = "hash_key";
        String field1 = "a";
        String field2 = "b";
        int expected = 2;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HDEL"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(field1),
                RedisObject.bulkString(field2));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.hDel(key, field1, field2);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hExists() throws Exception {
        String key = "hash_key";
        String field = "a";
        boolean expected = true;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HEXISTS"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(field));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.hExists(key, field);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hGet() throws Exception {
        String key = "hash_key";
        String field = "a";
        String expected = "1";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HGET"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(field));
        RedisObject response = RedisObject.bulkString(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.hGet(key, field);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hGetAll() throws Exception {
        String key = "hash_key";
        KeyValuePair[] expected = new KeyValuePair[] {
                new KeyValuePair("a", "1"),
                new KeyValuePair("b", "2"),
                new KeyValuePair("c", "3") };
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HGETALL"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString("a"),
                RedisObject.bulkString("1"),
                RedisObject.bulkString("b"),
                RedisObject.bulkString("2"),
                RedisObject.bulkString("c"),
                RedisObject.bulkString("3"));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<KeyValuePair[]> actualFuture = pirec.hGetAll(key);
        verify(pirec).execute(request);
        KeyValuePair[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hIncrBy() throws Exception {
        String key = "hash_key";
        String field = "a";
        long incr = 2;
        long expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HINCRBY"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(field),
                RedisObject.bulkString(Long.toString(incr)));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Long> actualFuture = pirec.hIncrBy(key, field, incr);
        verify(pirec).execute(request);
        long actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hIncrByFloat() throws Exception {
        String key = "hash_key";
        String field = "a";
        double incr = 2.5;
        double expected = 3.5;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HINCRBYFLOAT"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(field),
                RedisObject.bulkString(Double.toString(incr)));
        RedisObject response = RedisObject.bulkString(Double.toString(expected));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Double> actualFuture = pirec.hIncrByFloat(key, field, incr);
        verify(pirec).execute(request);
        double actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hKeys() throws Exception {
        String key = "hash_key";
        String[] expected = new String[] { "a", "b", "c" };
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HKEYS"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString("a"),
                RedisObject.bulkString("b"),
                RedisObject.bulkString("c"));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.hKeys(key);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hLen() throws Exception {
        String key = "hash_key";
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HLEN"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.hLen(key);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hMGet() throws Exception {
        String key = "hash_key";
        String field1 = "a";
        String field2 = "b";
        String[] expected = new String[] { "1", "2" };
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HMGET"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(field1),
                RedisObject.bulkString(field2));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(expected[0]),
                RedisObject.bulkString(expected[1]));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.hMGet(key, field1, field2);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hMSet() throws Exception {
        String key = "hash_key";
        String field1 = "d";
        String value1 = "4";
        String field2 = "e";
        String value2 = "5";
        KeyValuePair[] pairs = new KeyValuePair[] {
                new KeyValuePair(field1, value1),
                new KeyValuePair(field2, value2)
        };
        String expected = "OK";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HMSET"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(field1),
                RedisObject.bulkString(value1),
                RedisObject.bulkString(field2),
                RedisObject.bulkString(value2));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.hMSet(key, pairs);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hScan() throws Exception {
        String key = "hash_key";
        String cursor = "0";
        ScanResult<KeyValuePair> expected = new ScanResult<>(cursor, TEST_HASH);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HSCAN"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(cursor));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(cursor),
                RedisObject.array(Arrays
                        .stream(TEST_HASH)
                        .flatMap(kvp -> Stream.of(
                                RedisObject.bulkString(kvp.getKey()),
                                RedisObject.bulkString(kvp.getValue())))
                        .toArray(RedisObject[]::new)));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<ScanResult<KeyValuePair>> actualFuture = pirec.hScan(key, cursor);
        verify(pirec).execute(request);
        ScanResult<KeyValuePair> actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hScan_match() throws Exception {
        String key = "hash_key";
        String match = "?";
        String cursor = "0";
        ScanResult<KeyValuePair> expected = new ScanResult<>(cursor, TEST_HASH);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HSCAN"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(cursor),
                RedisObject.bulkString("MATCH"),
                RedisObject.bulkString(match));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(cursor),
                RedisObject.array(Arrays
                        .stream(TEST_HASH)
                        .flatMap(kvp -> Stream.of(
                                RedisObject.bulkString(kvp.getKey()),
                                RedisObject.bulkString(kvp.getValue())))
                        .toArray(RedisObject[]::new)));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<ScanResult<KeyValuePair>> actualFuture = pirec.hScan(key, cursor, match);
        verify(pirec).execute(request);
        ScanResult<KeyValuePair> actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hScan_count() throws Exception {
        String key = "hash_key";
        int count = 10;
        String cursor = "0";
        ScanResult<KeyValuePair> expected = new ScanResult<>(cursor, TEST_HASH);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HSCAN"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(cursor),
                RedisObject.bulkString("COUNT"),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(cursor),
                RedisObject.array(Arrays
                        .stream(TEST_HASH)
                        .flatMap(kvp -> Stream.of(
                                RedisObject.bulkString(kvp.getKey()),
                                RedisObject.bulkString(kvp.getValue())))
                        .toArray(RedisObject[]::new)));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<ScanResult<KeyValuePair>> actualFuture = pirec.hScan(key, cursor, count);
        verify(pirec).execute(request);
        ScanResult<KeyValuePair> actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hScan_match_count() throws Exception {
        String key = "hash_key";
        String cursor = "0";
        String match = "?";
        int count = 10;
        ScanResult<KeyValuePair> expected = new ScanResult<>(cursor, TEST_HASH);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HSCAN"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(cursor),
                RedisObject.bulkString("MATCH"),
                RedisObject.bulkString(match),
                RedisObject.bulkString("COUNT"),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(cursor),
                RedisObject.array(Arrays
                        .stream(TEST_HASH)
                        .flatMap(kvp -> Stream.of(
                                RedisObject.bulkString(kvp.getKey()),
                                RedisObject.bulkString(kvp.getValue())))
                        .toArray(RedisObject[]::new)));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<ScanResult<KeyValuePair>> actualFuture = pirec.hScan(key, cursor, match, count);
        verify(pirec).execute(request);
        ScanResult<KeyValuePair> actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hSet() throws Exception {
        String key = "hash_key";
        String field = "d";
        String value = "4";
        boolean expected = true;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HSET"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(field),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.hSet(key, field, value);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hSetNX() throws Exception {
        String key = "hash_key";
        String field = "d";
        String value = "4";
        boolean expected = true;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HSETNX"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(field),
                RedisObject.bulkString(value));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.hSetNX(key, field, value);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hStrLen() throws Exception {
        // Requires Redis server 3.2 or higher
        if (VERIFY_TESTS) return;
        String key = "hash_key";
        String field = "a";
        int expected = 1;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HSTRLEN"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(field));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.hStrLen(key, field);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_hVals() throws Exception {
        String key = "hash_key";
        String[] expected = new String[] { "1", "2", "3" };
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("HVALS"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString("1"),
                RedisObject.bulkString("2"),
                RedisObject.bulkString("3"));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.hVals(key);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_sAdd() throws Exception {
        String key = "set_key";
        String member = "d";
        int expected = 1;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SADD"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(member));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.sAdd(key, member);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_sCard() throws Exception {
        String key = "set_key";
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SCARD"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.sCard(key);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_sDiff() throws Exception {
        String key = "set_key";
        String key2 = "set2_key";
        String[] expected = new String[0];
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SDIFF"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(key2));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.sDiff(key, key2);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_sDiffStore() throws Exception {
        String key = "set_key";
        String key2 = "set2_key";
        String destKey = "set3_key";
        int expected = 0;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SDIFFSTORE"),
                RedisObject.bulkString(destKey),
                RedisObject.bulkString(key),
                RedisObject.bulkString(key2));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.sDiffStore(destKey, key, key2);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_sInter() throws Exception {
        String key = "set_key";
        String key2 = "set2_key";
        String[] expected = STRING_VALUES;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SINTER"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(key2));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.sInter(key, key2);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        Arrays.sort(actual);
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_sInterStore() throws Exception {
        String key = "set_key";
        String key2 = "set2_key";
        String destKey = "set3_key";
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SINTERSTORE"),
                RedisObject.bulkString(destKey),
                RedisObject.bulkString(key),
                RedisObject.bulkString(key2));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.sInterStore(destKey, key, key2);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_sIsMember() throws Exception {
        String key = "set_key";
        String member = "a";
        boolean expected = true;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SISMEMBER"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(member));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.sIsMember(key, member);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_sMembers() throws Exception {
        String key = "set_key";
        String[] expected = STRING_VALUES;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SMEMBERS"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.sMembers(key);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        Arrays.sort(actual);
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_sMove() throws Exception {
        String key = "set_key";
        String destKey = "unset_key";
        String member = "a";
        boolean expected = true;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SMOVE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(destKey),
                RedisObject.bulkString(member));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.sMove(key, destKey, member);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_sPop() throws Exception {
        String key = "set_key";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SPOP"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.bulkString("a");
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.sPop(key);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertTrue(Arrays.binarySearch(STRING_VALUES, actual) >= 0, "response in STRING_VALUES");
    }

    @Test
    public void test_sRandMember() throws Exception {
        String key = "set_key";
        int count = 2;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SRANDMEMBER"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString("a"),
                RedisObject.bulkString("b"));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.sRandMember(key, count);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertTrue(Arrays.stream(actual).allMatch(a -> Arrays.binarySearch(STRING_VALUES, a) >= 0), "response in STRING_VALUES");
    }

    @Test
    public void test_sRem() throws Exception {
        String key = "set_key";
        String member = "a";
        int expected = 1;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SREM"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(member));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.sRem(key, member);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_sScan() throws Exception {
        String key = "set_key";
        String cursor = "0";
        ScanResult<String> expected = new ScanResult<>(cursor, STRING_VALUES);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SSCAN"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(cursor));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(cursor),
                RedisObject.array(Arrays
                        .stream(STRING_VALUES)
                        .map(RedisObject::bulkString)
                        .toArray(RedisObject[]::new)));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<ScanResult<String>> actualFuture = pirec.sScan(key, cursor);
        verify(pirec).execute(request);
        ScanResult<String> actual = actualFuture.get();
        Arrays.sort(actual.getValues());
        assertEquals(actual.getCursor(), expected.getCursor(), "response cursor");

    }

    @Test
    public void test_sScan_match() throws Exception {
        String key = "set_key";
        String cursor = "0";
        String match = "?";
        ScanResult<String> expected = new ScanResult<>(cursor, STRING_VALUES);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SSCAN"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(cursor),
                RedisObject.bulkString("MATCH"),
                RedisObject.bulkString(match));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(cursor),
                RedisObject.array(Arrays
                        .stream(STRING_VALUES)
                        .map(RedisObject::bulkString)
                        .toArray(RedisObject[]::new)));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<ScanResult<String>> actualFuture = pirec.sScan(key, cursor, match);
        verify(pirec).execute(request);
        ScanResult<String> actual = actualFuture.get();
        Arrays.sort(actual.getValues());
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_sScan_count() throws Exception {
        String key = "set_key";
        String cursor = "0";
        int count = 10;
        ScanResult<String> expected = new ScanResult<>(cursor, STRING_VALUES);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SSCAN"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(cursor),
                RedisObject.bulkString("COUNT"),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(cursor),
                RedisObject.array(Arrays
                        .stream(STRING_VALUES)
                        .map(RedisObject::bulkString)
                        .toArray(RedisObject[]::new)));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<ScanResult<String>> actualFuture = pirec.sScan(key, cursor, count);
        verify(pirec).execute(request);
        ScanResult<String> actual = actualFuture.get();
        Arrays.sort(actual.getValues());
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_sScan_match_count() throws Exception {
        String key = "set_key";
        String cursor = "0";
        String match = "?";
        int count = 10;
        ScanResult<String> expected = new ScanResult<>(cursor, STRING_VALUES);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SSCAN"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(cursor),
                RedisObject.bulkString("MATCH"),
                RedisObject.bulkString(match),
                RedisObject.bulkString("COUNT"),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(cursor),
                RedisObject.array(Arrays
                        .stream(STRING_VALUES)
                        .map(RedisObject::bulkString)
                        .toArray(RedisObject[]::new)));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<ScanResult<String>> actualFuture = pirec.sScan(key, cursor, match, count);
        verify(pirec).execute(request);
        ScanResult<String> actual = actualFuture.get();
        Arrays.sort(actual.getValues());
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_sUnion() throws Exception {
        String key = "set_key";
        String key2 = "set2_key";
        String[] expected = STRING_VALUES;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SUNION"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(key2));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.sUnion(key, key2);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        Arrays.sort(actual);
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_sUnionStore() throws Exception {
        String key = "set_key";
        String key2 = "set2_key";
        String destKey = "set3_key";
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("SUNIONSTORE"),
                RedisObject.bulkString(destKey),
                RedisObject.bulkString(key),
                RedisObject.bulkString(key2));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.sUnionStore(destKey, key, key2);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_pfAdd() throws Exception {
        String key = "pf_key";
        String member = "d";
        boolean expected = true;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("PFADD"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(member));
        RedisObject response = RedisObject.integer(expected ? 1 : 0);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Boolean> actualFuture = pirec.pfAdd(key, member);
        verify(pirec).execute(request);
        boolean actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_pfCount() throws Exception {
        String key = "pf_key";
        int expected = 0;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("PFCOUNT"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.pfCount(key);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_pfMerge() throws Exception {
        String key = "pf_key";
        String key2 = "pf2_key";
        String destKey = "pf3_key";
        String expected = "OK";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("PFMERGE"),
                RedisObject.bulkString(destKey),
                RedisObject.bulkString(key),
                RedisObject.bulkString(key2));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.pfMerge(destKey, key, key2);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_watch() throws Exception {
        String key = "string_key";
        String expected = "OK";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("WATCH"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.watch(key);
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_unwatch() throws Exception {
        String expected = "OK";
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("UNWATCH"));
        RedisObject response = RedisObject.simple(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String> actualFuture = pirec.unwatch();
        verify(pirec).execute(request);
        String actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_multi_exec() throws Exception {
        String ok = "OK";
        RedisObject request1 = RedisObject.array(RedisObject.bulkString("MULTI"));
        RedisObject request4 = RedisObject.array(RedisObject.bulkString("EXEC"));
        RedisObject response1 = RedisObject.simple(ok);
        RedisObject response4 = RedisObject.array(0);
        doReturn(CompletableFuture.completedFuture(response1)).when(client).sendRequest(eq(request1));
        doReturn(CompletableFuture.completedFuture(response4)).when(client).sendRequest(eq(request4));

        CompletableFuture<String> actualFuture1 = pirec.multi();
        CompletableFuture<RedisArray> actualFuture4 = pirec.exec();
        verify(pirec).execute(request1);
        verify(pirec).execute(request4);
        String actual1 = actualFuture1.get();
        assertEquals(actual1, ok, "response1");
        RedisArray actual4 = actualFuture4.get();
        assertEquals(actual4, response4, "response4");
    }

    @Test
    public void test_multi_discard() throws Exception {
        String ok = "OK";
        RedisObject request1 = RedisObject.array(RedisObject.bulkString("MULTI"));
        RedisObject request4 = RedisObject.array(RedisObject.bulkString("DISCARD"));
        RedisObject response1 = RedisObject.simple(ok);
        RedisObject response4 = RedisObject.simple(ok);
        doReturn(CompletableFuture.completedFuture(response1)).when(client).sendRequest(eq(request1));
        doReturn(CompletableFuture.completedFuture(response4)).when(client).sendRequest(eq(request4));

        CompletableFuture<String> actualFuture1 = pirec.multi();
        CompletableFuture<String> actualFuture4 = pirec.discard();
        verify(pirec).execute(request1);
        verify(pirec).execute(request4);
        String actual1 = actualFuture1.get();
        assertEquals(actual1, ok, "response1");
        String actual4 = actualFuture4.get();
        assertEquals(actual4, ok, "response4");
    }

    @Test
    public void test_zAdd() throws Exception {
        String key = "sset_key";
        double score = 4;
        String member = "d";
        MemberScorePair pair = new MemberScorePair(member, score);
        int expected = 1;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZADD"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Double.toString(score)),
                RedisObject.bulkString(member));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zAdd(key, pair);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zCard() throws Exception {
        String key = "sset_key";
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZCARD"),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zCard(key);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zCount() throws Exception {
        String key = "sset_key";
        double min = 1.5;
        double max = 3.5;
        int expected = 2;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZCOUNT"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Double.toString(min)),
                RedisObject.bulkString(Double.toString(max)));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zCount(key, min, max);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zIncrBy() throws Exception {
        String key = "sset_key";
        double incr = 4;
        String member = "a";
        double expected = 5;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZINCRBY"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Double.toString(incr)),
                RedisObject.bulkString(member));
        RedisObject response = RedisObject.bulkString(Double.toString(expected));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Double> actualFuture = pirec.zIncrBy(key, incr, member);
        verify(pirec).execute(request);
        double actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zInterStore_keys() throws Exception {
        String destKey = "sset2_key";
        String key = "sset_key";
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZINTERSTORE"),
                RedisObject.bulkString(destKey),
                RedisObject.bulkString(Integer.toString(1)),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zInterStore(destKey, key);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zInterStore_keys_aggregate() throws Exception {
        String destKey = "sset2_key";
        String key = "sset_key";
        Aggregate aggregate = Aggregate.MAX;
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZINTERSTORE"),
                RedisObject.bulkString(destKey),
                RedisObject.bulkString(Integer.toString(1)),
                RedisObject.bulkString(key),
                RedisObject.bulkString("AGGREGATE"),
                RedisObject.bulkString("MAX"));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zInterStore(destKey, aggregate, key);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zInterStore_keyWeightPairs() throws Exception {
        String destKey = "sset2_key";
        String key = "sset_key";
        Double weight = 1.1;
        KeyWeightPair pair = new KeyWeightPair(key, weight);
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZINTERSTORE"),
                RedisObject.bulkString(destKey),
                RedisObject.bulkString(Integer.toString(1)),
                RedisObject.bulkString(key),
                RedisObject.bulkString("WEIGHTS"),
                RedisObject.bulkString(Double.toString(weight)));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zInterStore(destKey, pair);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zInterStore_keyWeightPairs_aggregate() throws Exception {
        String destKey = "sset2_key";
        String key = "sset_key";
        Double weight = 1.1;
        KeyWeightPair pair = new KeyWeightPair(key, weight);
        Aggregate aggregate = Aggregate.MAX;
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZINTERSTORE"),
                RedisObject.bulkString(destKey),
                RedisObject.bulkString(Integer.toString(1)),
                RedisObject.bulkString(key),
                RedisObject.bulkString("WEIGHTS"),
                RedisObject.bulkString(Double.toString(weight)),
                RedisObject.bulkString("AGGREGATE"),
                RedisObject.bulkString("MAX"));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zInterStore(destKey, aggregate, pair);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zLexCount() throws Exception {
        String key = "sset_key";
        String min = "(a";
        String max = "(c";
        int expected = 1;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZLEXCOUNT"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(min),
                RedisObject.bulkString(max));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zLexCount(key, min, max);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRange() throws Exception {
        String key = "sset_key";
        int start = 0;
        int stop = -1;
        String[] expected = Arrays.stream(TEST_SSET).map(MemberScorePair::getMember).toArray(String[]::new);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZRANGE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(start)),
                RedisObject.bulkString(Integer.toString(stop)));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.zRange(key, start, stop);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRangeWithScores() throws Exception {
        String key = "sset_key";
        int start = 0;
        int stop = -1;
        MemberScorePair[] expected = TEST_SSET;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZRANGE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(start)),
                RedisObject.bulkString(Integer.toString(stop)),
                RedisObject.bulkString("WITHSCORES"));
        RedisObject response = RedisObject.array(Arrays
                .stream(expected)
                .flatMap(msp -> Stream.of(msp.getMember(), Double.toString(msp.getScore())))
                .map(RedisObject::bulkString)
                .toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<MemberScorePair[]> actualFuture = pirec.zRangeWithScores(key, start, stop);
        verify(pirec).execute(request);
        MemberScorePair[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRangeByLex() throws Exception {
        String key = "sset_key";
        String min = "[a";
        String max = "[c";
        String[] expected = Arrays.stream(TEST_SSET).map(MemberScorePair::getMember).toArray(String[]::new);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZRANGEBYLEX"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(min),
                RedisObject.bulkString(max));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.zRangeByLex(key, min, max);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRangeByLex_offset_count() throws Exception {
        String key = "sset_key";
        String min = "[a";
        String max = "[c";
        int offset = 0;
        int count = 3;
        String[] expected = Arrays.stream(TEST_SSET).map(MemberScorePair::getMember).toArray(String[]::new);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZRANGEBYLEX"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(min),
                RedisObject.bulkString(max),
                RedisObject.bulkString("LIMIT"),
                RedisObject.bulkString(Integer.toString(offset)),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.zRangeByLex(key, min, max, offset, count);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRangeByScore() throws Exception {
        String key = "sset_key";
        double min = 1;
        double max = 3;
        String[] expected = Arrays.stream(TEST_SSET).map(MemberScorePair::getMember).toArray(String[]::new);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Double.toString(min)),
                RedisObject.bulkString(Double.toString(max)));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.zRangeByScore(key, min, max);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRangeByScore_offset_count() throws Exception {
        String key = "sset_key";
        double min = 1;
        double max = 3;
        int offset = 0;
        int count = 3;
        String[] expected = Arrays.stream(TEST_SSET).map(MemberScorePair::getMember).toArray(String[]::new);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Double.toString(min)),
                RedisObject.bulkString(Double.toString(max)),
                RedisObject.bulkString("LIMIT"),
                RedisObject.bulkString(Integer.toString(offset)),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.zRangeByScore(key, min, max, offset, count);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRangeByScore_string() throws Exception {
        String key = "sset_key";
        String min = "(0.5";
        String max = "(3.5";
        String[] expected = Arrays.stream(TEST_SSET).map(MemberScorePair::getMember).toArray(String[]::new);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(min),
                RedisObject.bulkString(max));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.zRangeByScore(key, min, max);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRangeByScore_string_offset_count() throws Exception {
        String key = "sset_key";
        String min = "(0.5";
        String max = "(3.5";
        int offset = 0;
        int count = 3;
        String[] expected = Arrays.stream(TEST_SSET).map(MemberScorePair::getMember).toArray(String[]::new);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(min),
                RedisObject.bulkString(max),
                RedisObject.bulkString("LIMIT"),
                RedisObject.bulkString(Integer.toString(offset)),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.zRangeByScore(key, min, max, offset, count);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRangeByScoreWithScores() throws Exception {
        String key = "sset_key";
        double min = 1;
        double max = 3;
        MemberScorePair[] expected = TEST_SSET;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Double.toString(min)),
                RedisObject.bulkString(Double.toString(max)),
                RedisObject.bulkString("WITHSCORES"));
        RedisObject response = RedisObject.array(Arrays
                .stream(expected)
                .flatMap(p -> Stream.of(p.getMember(), Double.toString(p.getScore())))
                .map(RedisObject::bulkString)
                .toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<MemberScorePair[]> actualFuture = pirec.zRangeByScoreWithScores(key, min, max);
        verify(pirec).execute(request);
        MemberScorePair[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRangeByScoreWithScores_offset_count() throws Exception {
        String key = "sset_key";
        double min = 1;
        double max = 3;
        int offset = 0;
        int count = 3;
        MemberScorePair[] expected = TEST_SSET;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Double.toString(min)),
                RedisObject.bulkString(Double.toString(max)),
                RedisObject.bulkString("WITHSCORES"),
                RedisObject.bulkString("LIMIT"),
                RedisObject.bulkString(Integer.toString(offset)),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(Arrays
                .stream(expected)
                .flatMap(p -> Stream.of(p.getMember(), Double.toString(p.getScore())))
                .map(RedisObject::bulkString)
                .toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<MemberScorePair[]> actualFuture = pirec.zRangeByScoreWithScores(key, min, max, offset, count);
        verify(pirec).execute(request);
        MemberScorePair[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRangeByScoreWithScores_string() throws Exception {
        String key = "sset_key";
        String min = "(0.5";
        String max = "(3.5";
        MemberScorePair[] expected = TEST_SSET;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(min),
                RedisObject.bulkString(max),
                RedisObject.bulkString("WITHSCORES"));
        RedisObject response = RedisObject.array(Arrays
                .stream(expected)
                .flatMap(p -> Stream.of(p.getMember(), Double.toString(p.getScore())))
                .map(RedisObject::bulkString)
                .toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<MemberScorePair[]> actualFuture = pirec.zRangeByScoreWithScores(key, min, max);
        verify(pirec).execute(request);
        MemberScorePair[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRangeByScoreWithScores_string_offset_count() throws Exception {
        String key = "sset_key";
        String min = "(0.5";
        String max = "(3.5";
        int offset = 0;
        int count = 3;
        MemberScorePair[] expected = TEST_SSET;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(min),
                RedisObject.bulkString(max),
                RedisObject.bulkString("WITHSCORES"),
                RedisObject.bulkString("LIMIT"),
                RedisObject.bulkString(Integer.toString(offset)),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(Arrays
                .stream(expected)
                .flatMap(p -> Stream.of(p.getMember(), Double.toString(p.getScore())))
                .map(RedisObject::bulkString)
                .toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<MemberScorePair[]> actualFuture = pirec.zRangeByScoreWithScores(key, min, max, offset, count);
        verify(pirec).execute(request);
        MemberScorePair[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRank() throws Exception {
        String key = "sset_key";
        String member = "b";
        Integer expected = 1;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZRANK"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(member));
        RedisObject response = RedisInteger.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zRank(key, member);
        verify(pirec).execute(request);
        Integer actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRem() throws Exception {
        String key = "sset_key";
        String member1 = "b";
        String member2 = "c";
        Integer expected = 2;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREM"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(member1),
                RedisObject.bulkString(member2));
        RedisObject response = RedisInteger.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zRem(key, member1, member2);
        verify(pirec).execute(request);
        Integer actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRemRangeByLex() throws Exception {
        String key = "sset_key";
        String min = "[a";
        String max = "[c";
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREMRANGEBYLEX"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(min),
                RedisObject.bulkString(max));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zRemRangeByLex(key, min, max);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRemRangeByRank() throws Exception {
        String key = "sset_key";
        int start = 0;
        int stop = -1;
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREMRANGEBYRANK"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(start)),
                RedisObject.bulkString(Integer.toString(stop)));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zRemRangeByRank(key, start, stop);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRemRangeByScore() throws Exception {
        String key = "sset_key";
        double min = 0;
        double max = 4;
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREMRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Double.toString(min)),
                RedisObject.bulkString(Double.toString(max)));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zRemRangeByScore(key, min, max);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRemRangeByScore_strings() throws Exception {
        String key = "sset_key";
        String min = "(0";
        String max = "(4";
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREMRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(min),
                RedisObject.bulkString(max));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zRemRangeByScore(key, min, max);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRevRange() throws Exception {
        String key = "sset_key";
        int start = 0;
        int stop = -1;
        String[] expected = Arrays.stream(reverse(TEST_SSET)).map(MemberScorePair::getMember).toArray(String[]::new);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREVRANGE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(start)),
                RedisObject.bulkString(Integer.toString(stop)));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.zRevRange(key, start, stop);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRevRangeWithScores() throws Exception {
        String key = "sset_key";
        int start = 0;
        int stop = -1;
        MemberScorePair[] expected = reverse(TEST_SSET);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREVRANGE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Integer.toString(start)),
                RedisObject.bulkString(Integer.toString(stop)),
                RedisObject.bulkString("WITHSCORES"));
        RedisObject response = RedisObject.array(Arrays
                .stream(expected)
                .flatMap(msp -> Stream.of(msp.getMember(), Double.toString(msp.getScore())))
                .map(RedisObject::bulkString)
                .toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<MemberScorePair[]> actualFuture = pirec.zRevRangeWithScores(key, start, stop);
        verify(pirec).execute(request);
        MemberScorePair[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRevRangeByLex() throws Exception {
        String key = "sset_key";
        String min = "[c";
        String max = "[a";
        String[] expected = Arrays.stream(reverse(TEST_SSET)).map(MemberScorePair::getMember).toArray(String[]::new);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREVRANGEBYLEX"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(min),
                RedisObject.bulkString(max));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.zRevRangeByLex(key, min, max);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRevRangeByLex_offset_count() throws Exception {
        String key = "sset_key";
        String min = "[c";
        String max = "[a";
        int offset = 0;
        int count = 3;
        String[] expected = Arrays.stream(reverse(TEST_SSET)).map(MemberScorePair::getMember).toArray(String[]::new);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREVRANGEBYLEX"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(min),
                RedisObject.bulkString(max),
                RedisObject.bulkString("LIMIT"),
                RedisObject.bulkString(Integer.toString(offset)),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.zRevRangeByLex(key, min, max, offset, count);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRevRangeByScore() throws Exception {
        String key = "sset_key";
        double min = 3;
        double max = 1;
        String[] expected = Arrays.stream(reverse(TEST_SSET)).map(MemberScorePair::getMember).toArray(String[]::new);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREVRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Double.toString(min)),
                RedisObject.bulkString(Double.toString(max)));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.zRevRangeByScore(key, min, max);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRevRangeByScore_offset_count() throws Exception {
        String key = "sset_key";
        double min = 3;
        double max = 1;
        int offset = 0;
        int count = 3;
        String[] expected = Arrays.stream(reverse(TEST_SSET)).map(MemberScorePair::getMember).toArray(String[]::new);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREVRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Double.toString(min)),
                RedisObject.bulkString(Double.toString(max)),
                RedisObject.bulkString("LIMIT"),
                RedisObject.bulkString(Integer.toString(offset)),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.zRevRangeByScore(key, min, max, offset, count);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRevRangeByScore_string() throws Exception {
        String key = "sset_key";
        String min = "(3.5";
        String max = "(0.5";
        String[] expected = Arrays.stream(reverse(TEST_SSET)).map(MemberScorePair::getMember).toArray(String[]::new);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREVRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(min),
                RedisObject.bulkString(max));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.zRevRangeByScore(key, min, max);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRevRangeByScore_string_offset_count() throws Exception {
        String key = "sset_key";
        String min = "(3.5";
        String max = "(0.5";
        int offset = 0;
        int count = 3;
        String[] expected = Arrays.stream(reverse(TEST_SSET)).map(MemberScorePair::getMember).toArray(String[]::new);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREVRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(min),
                RedisObject.bulkString(max),
                RedisObject.bulkString("LIMIT"),
                RedisObject.bulkString(Integer.toString(offset)),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(Arrays.stream(expected).map(RedisObject::bulkString).toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<String[]> actualFuture = pirec.zRevRangeByScore(key, min, max, offset, count);
        verify(pirec).execute(request);
        String[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRevRangeByScoreWithScores() throws Exception {
        String key = "sset_key";
        double min = 3;
        double max = 1;
        MemberScorePair[] expected = reverse(TEST_SSET);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREVRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Double.toString(min)),
                RedisObject.bulkString(Double.toString(max)),
                RedisObject.bulkString("WITHSCORES"));
        RedisObject response = RedisObject.array(Arrays
                .stream(expected)
                .flatMap(p -> Stream.of(p.getMember(), Double.toString(p.getScore())))
                .map(RedisObject::bulkString)
                .toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<MemberScorePair[]> actualFuture = pirec.zRevRangeByScoreWithScores(key, min, max);
        verify(pirec).execute(request);
        MemberScorePair[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRevRangeByScoreWithScores_offset_count() throws Exception {
        String key = "sset_key";
        double min = 3;
        double max = 1;
        int offset = 0;
        int count = 3;
        MemberScorePair[] expected = reverse(TEST_SSET);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREVRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(Double.toString(min)),
                RedisObject.bulkString(Double.toString(max)),
                RedisObject.bulkString("WITHSCORES"),
                RedisObject.bulkString("LIMIT"),
                RedisObject.bulkString(Integer.toString(offset)),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(Arrays
                .stream(expected)
                .flatMap(p -> Stream.of(p.getMember(), Double.toString(p.getScore())))
                .map(RedisObject::bulkString)
                .toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<MemberScorePair[]> actualFuture = pirec.zRevRangeByScoreWithScores(key, min, max, offset, count);
        verify(pirec).execute(request);
        MemberScorePair[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRevRangeByScoreWithScores_string() throws Exception {
        String key = "sset_key";
        String min = "(3.5";
        String max = "(0.5";
        MemberScorePair[] expected = reverse(TEST_SSET);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREVRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(min),
                RedisObject.bulkString(max),
                RedisObject.bulkString("WITHSCORES"));
        RedisObject response = RedisObject.array(Arrays
                .stream(expected)
                .flatMap(p -> Stream.of(p.getMember(), Double.toString(p.getScore())))
                .map(RedisObject::bulkString)
                .toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<MemberScorePair[]> actualFuture = pirec.zRevRangeByScoreWithScores(key, min, max);
        verify(pirec).execute(request);
        MemberScorePair[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRevRangeByScoreWithScores_string_offset_count() throws Exception {
        String key = "sset_key";
        String min = "(3.5";
        String max = "(0.5";
        int offset = 0;
        int count = 3;
        MemberScorePair[] expected = reverse(TEST_SSET);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREVRANGEBYSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(min),
                RedisObject.bulkString(max),
                RedisObject.bulkString("WITHSCORES"),
                RedisObject.bulkString("LIMIT"),
                RedisObject.bulkString(Integer.toString(offset)),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(Arrays
                .stream(expected)
                .flatMap(p -> Stream.of(p.getMember(), Double.toString(p.getScore())))
                .map(RedisObject::bulkString)
                .toArray(RedisObject[]::new));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<MemberScorePair[]> actualFuture = pirec.zRevRangeByScoreWithScores(key, min, max, offset, count);
        verify(pirec).execute(request);
        MemberScorePair[] actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zRevRank() throws Exception {
        String key = "sset_key";
        String member = "b";
        Integer expected = 1;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZREVRANK"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(member));
        RedisObject response = RedisInteger.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zRevRank(key, member);
        verify(pirec).execute(request);
        Integer actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zScan() throws Exception {
        String key = "sset_key";
        String cursor = "0";
        ScanResult<MemberScorePair> expected = new ScanResult<>(cursor, TEST_SSET);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZSCAN"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(cursor));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(cursor),
                RedisObject.array(Arrays
                        .stream(expected.getValues())
                        .flatMap(msp -> Stream.of(
                                RedisObject.bulkString(msp.getMember()),
                                RedisObject.bulkString(Double.toString(msp.getScore()))))
                        .toArray(RedisObject[]::new)));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<ScanResult<MemberScorePair>> actualFuture = pirec.zScan(key, cursor);
        verify(pirec).execute(request);
        ScanResult<MemberScorePair> actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zScan_match() throws Exception {
        String key = "sset_key";
        String match = "?";
        String cursor = "0";
        ScanResult<MemberScorePair> expected = new ScanResult<>(cursor, TEST_SSET);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZSCAN"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(cursor),
                RedisObject.bulkString("MATCH"),
                RedisObject.bulkString(match));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(cursor),
                RedisObject.array(Arrays
                        .stream(expected.getValues())
                        .flatMap(msp -> Stream.of(
                                RedisObject.bulkString(msp.getMember()),
                                RedisObject.bulkString(Double.toString(msp.getScore()))))
                        .toArray(RedisObject[]::new)));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<ScanResult<MemberScorePair>> actualFuture = pirec.zScan(key, cursor, match);
        verify(pirec).execute(request);
        ScanResult<MemberScorePair> actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zScan_count() throws Exception {
        String key = "sset_key";
        int count = 10;
        String cursor = "0";
        ScanResult<MemberScorePair> expected = new ScanResult<>(cursor, TEST_SSET);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZSCAN"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(cursor),
                RedisObject.bulkString("COUNT"),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(cursor),
                RedisObject.array(Arrays
                        .stream(expected.getValues())
                        .flatMap(msp -> Stream.of(
                                RedisObject.bulkString(msp.getMember()),
                                RedisObject.bulkString(Double.toString(msp.getScore()))))
                        .toArray(RedisObject[]::new)));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<ScanResult<MemberScorePair>> actualFuture = pirec.zScan(key, cursor, count);
        verify(pirec).execute(request);
        ScanResult<MemberScorePair> actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zScan_match_count() throws Exception {
        String key = "sset_key";
        String cursor = "0";
        String match = "?";
        int count = 10;
        ScanResult<MemberScorePair> expected = new ScanResult<>(cursor, TEST_SSET);
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZSCAN"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(cursor),
                RedisObject.bulkString("MATCH"),
                RedisObject.bulkString(match),
                RedisObject.bulkString("COUNT"),
                RedisObject.bulkString(Integer.toString(count)));
        RedisObject response = RedisObject.array(
                RedisObject.bulkString(cursor),
                RedisObject.array(Arrays
                        .stream(expected.getValues())
                        .flatMap(msp -> Stream.of(
                                RedisObject.bulkString(msp.getMember()),
                                RedisObject.bulkString(Double.toString(msp.getScore()))))
                        .toArray(RedisObject[]::new)));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<ScanResult<MemberScorePair>> actualFuture = pirec.zScan(key, cursor, match, count);
        verify(pirec).execute(request);
        ScanResult<MemberScorePair> actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zScore() throws Exception {
        String key = "sset_key";
        String member = "b";
        double expected = 2;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZSCORE"),
                RedisObject.bulkString(key),
                RedisObject.bulkString(member));
        RedisObject response = RedisInteger.bulkString(Double.toString(expected));
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Double> actualFuture = pirec.zScore(key, member);
        verify(pirec).execute(request);
        Double actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zUnionStore_keys() throws Exception {
        String destKey = "sset2_key";
        String key = "sset_key";
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZUNIONSTORE"),
                RedisObject.bulkString(destKey),
                RedisObject.bulkString(Integer.toString(1)),
                RedisObject.bulkString(key));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zUnionStore(destKey, key);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zUnionStore_keys_aggregate() throws Exception {
        String destKey = "sset2_key";
        String key = "sset_key";
        Aggregate aggregate = Aggregate.MAX;
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZUNIONSTORE"),
                RedisObject.bulkString(destKey),
                RedisObject.bulkString(Integer.toString(1)),
                RedisObject.bulkString(key),
                RedisObject.bulkString("AGGREGATE"),
                RedisObject.bulkString("MAX"));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zUnionStore(destKey, aggregate, key);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zUnionStore_keyWeightPairs() throws Exception {
        String destKey = "sset2_key";
        String key = "sset_key";
        Double weight = 1.1;
        KeyWeightPair pair = new KeyWeightPair(key, weight);
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZUNIONSTORE"),
                RedisObject.bulkString(destKey),
                RedisObject.bulkString(Integer.toString(1)),
                RedisObject.bulkString(key),
                RedisObject.bulkString("WEIGHTS"),
                RedisObject.bulkString(Double.toString(weight)));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zUnionStore(destKey, pair);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    @Test
    public void test_zUnionStore_keyWeightPairs_aggregate() throws Exception {
        String destKey = "sset2_key";
        String key = "sset_key";
        Double weight = 1.1;
        KeyWeightPair pair = new KeyWeightPair(key, weight);
        Aggregate aggregate = Aggregate.MAX;
        int expected = 3;
        RedisObject request = RedisObject.array(
                RedisObject.bulkString("ZUNIONSTORE"),
                RedisObject.bulkString(destKey),
                RedisObject.bulkString(Integer.toString(1)),
                RedisObject.bulkString(key),
                RedisObject.bulkString("WEIGHTS"),
                RedisObject.bulkString(Double.toString(weight)),
                RedisObject.bulkString("AGGREGATE"),
                RedisObject.bulkString("MAX"));
        RedisObject response = RedisObject.integer(expected);
        doReturn(CompletableFuture.completedFuture(response)).when(client).sendRequest(eq(request));

        CompletableFuture<Integer> actualFuture = pirec.zUnionStore(destKey, aggregate, pair);
        verify(pirec).execute(request);
        int actual = actualFuture.get();
        assertEquals(actual, expected, "response");
    }

    byte[] utf8Bytes(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }

    String intValues(byte[] bytes) {
        return IntStream.range(0, bytes.length)
                .map(i -> bytes[i])
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(", "));
    }

    <T> T[] reverse(T[] array) {
        T[] result = Arrays.copyOf(array, array.length);
        for (int i = 0; i < array.length; ++i) {
            result[array.length - i - 1] = array[i];
        }
        return result;
    }
}
