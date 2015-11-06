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

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import redis.clients.pirec.codec.Validate;
import redis.clients.pirec.codec.object.RedisArray;
import redis.clients.pirec.codec.object.RedisObject;
import redis.clients.pirec.commands.Aggregate;
import redis.clients.pirec.commands.BitOp;
import redis.clients.pirec.commands.KeyValueBytesPair;
import redis.clients.pirec.commands.KeyValuePair;
import redis.clients.pirec.commands.KeyWeightPair;
import redis.clients.pirec.commands.MemberScorePair;
import redis.clients.pirec.commands.RedisCommands;
import redis.clients.pirec.commands.ScanResult;
import redis.clients.pirec.io.RedisClient;

public class Pirec {

    RedisClient client;

    public Pirec() {
        this(new RedisClient());
    }

    public Pirec(RedisClient client) {
        this.client = client;
    }

    /**
     * Returns the number of currently active and outstanding requests.
     */
    public int numActiveRequests() {
        return client.numActiveRequests();
    }

    /**
     * Connect to the Redis server.
     */
    public CompletableFuture<Void> connect(String hostname) {
        return connect(hostname, 6379);
    }

    /**
     * Connect to the Redis server.
     */
    public CompletableFuture<Void> connect(String hostname, int port) {
        InetSocketAddress remote = new InetSocketAddress(hostname, port);
        return connect(remote);
    }

    /**
     * Connect to the Redis server.
     */
    public CompletableFuture<Void> connect(InetSocketAddress remote) {
        return client.connect(remote);
    }

    /**
     * Connect to the Redis server. This call blocks until the connection is complete.
     */
    public void connectSync(String hostname) throws InterruptedException, ExecutionException {
        connect(hostname).get();
    }

    /**
     * Connect to the Redis server. This call blocks until the connection is complete.
     */
    public void connectSync(String hostname, int port) throws InterruptedException, ExecutionException {
        connect(hostname, port).get();
    }

    /**
     * Connect to the Redis server. This call blocks until the connection is complete.
     */
    public void connectSync(InetSocketAddress remote) throws InterruptedException, ExecutionException {
        connect(remote).get();
    }

    /**
     * Low level function that allows you to send commands that are not yet implemented by this client.
     * 
     * Use RedisCommands static methods to help build the request and process the response.
     * 
     * Example: Implementing request GETBASE64 that takes a key and returns the base64 encoded version of the value for
     * that key.
     * 
     * <pre>
     * RedisObject request = RedisCommands.createRequest(&quot;GETBASE64&quot;, key);
     * CompletableFuture&lt;String&gt; base64Value =
     *         pirec.execute(request)
     *                 .thenApply(RedisCommands::bulkStringResponseAsString);
     * </pre>
     * 
     * @param request A RedisObject used as a request
     * @return The RedisObject response to the request
     */
    public CompletableFuture<RedisObject> execute(RedisObject request) {
        return client.sendRequest(request);
    }

    /**
     * Low level function that allows you to send commands that are not yet implemented by this client.
     * 
     * Use RedisCommands static methods to help process the response.
     * 
     * Example: Implementing request BEEP that causes the server to beep when received.
     * 
     * <pre>
     * pirec.execute(&quot;BEEP&quot;, key)
     *         .thenApply(RedisCommands::simpleResponse);
     * </pre>
     * 
     * @param request A RedisObject used as a request
     * @return The RedisObject response to the request
     */
    public CompletableFuture<RedisObject> execute(String command) {
        return execute(RedisCommands.createRequest(command));
    }

    /**
     * Low level function that allows you to send commands that are not yet implemented by this client.
     * 
     * Use RedisCommands static methods to help process the response.
     * 
     * Example: Implementing request ENCRYPT that takes a bulkString and a key containing a shared secret and returns an
     * encrypted version of the string.
     * 
     * <pre>
     * CompletableFuture&lt;byte[]&gt; encryptedMessage =
     *         pirec.execute(&quot;ENCRYPT&quot;, key, message)
     *                 .thenApply(RedisCommands::bulkStringResponseAsBytes);
     * </pre>
     * 
     * @param command The Redis command to call
     * @param params String parameters for the call
     * @return The RedisObject response to the request
     */
    public CompletableFuture<RedisObject> execute(String command, String... params) {
        return execute(RedisCommands.createRequest(command, params));
    }

    /**
     * Low level function that allows you to send commands that are not yet implemented by this client.
     * 
     * Use RedisCommands static methods to help process the response.
     * 
     * Example: Implementing request DECRYPT that takes a encrypted bulkString and a key that stores a shared secret
     * and returns a decrypted message.
     * 
     * <pre>
     * CompletableFuture&lt;String&gt; decrypted =
     *         pirec.execute(&quot;DECRYPT&quot;, key.getBytes(), encryptedMessage)
     *                 .thenApply(RedisCommands::bulkStringResponseAsString);
     * </pre>
     * 
     * @param command The Redis command to call
     * @param params Byte array parameters for the call
     * @return The RedisObject response to the request
     */
    public CompletableFuture<RedisObject> execute(String command, byte[]... params) {
        return execute(RedisCommands.createRequest(command, params));
    }



    // Connection Commands



    /**
     * Authenticate to the server.
     * 
     * @see <a href="http://redis.io/commands/auth">http://redis.io/commands/auth</a>
     */
    public CompletableFuture<String> auth(String password) {
        return execute("AUTH", password)
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Echo the given string.
     * 
     * @see <a href="http://redis.io/commands/echo">http://redis.io/commands/echo</a>
     */
    public CompletableFuture<String> echo(String message) {
        return execute("ECHO", message)
                .thenApply(RedisCommands::bulkStringResponseAsString);
    }

    private static final RedisObject PING = RedisCommands.createRequest("PING");

    /**
     * Ping the server.
     * 
     * @see <a href="http://redis.io/commands/ping">http://redis.io/commands/ping</a>
     */
    public CompletableFuture<String> ping() {
        return execute(PING).thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Close the connection.
     * 
     * @see <a href="http://redis.io/commands/quit">http://redis.io/commands/quit</a>
     */
    public CompletableFuture<String> quit() {
        return execute("QUIT")
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Change the selected database for the current connection.
     * 
     * @see <a href="http://redis.io/commands/select">http://redis.io/commands/select</a>
     */
    public CompletableFuture<String> select(int index) {
        return execute("SELECT", Integer.toString(index))
                .thenApply(RedisCommands::simpleResponse);
    }



    // String Commands



    /**
     * Append a value to a key.
     * 
     * @see <a href="http://redis.io/commands/append">http://redis.io/commands/append</a>
     */
    public CompletableFuture<Long> append(String key, String value) {
        return execute("APPEND", key, value)
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Count set bits in a string.
     * 
     * @see <a href="http://redis.io/commands/bitcount">http://redis.io/commands/bitcount</a>
     */
    public CompletableFuture<Long> bitCount(String key) {
        return execute("BITCOUNT", key)
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Count set bits in a string.
     * 
     * @see <a href="http://redis.io/commands/bitcount">http://redis.io/commands/bitcount</a>
     */
    public CompletableFuture<Long> bitCount(String key, long start, long end) {
        return execute("BITCOUNT", key, Long.toString(start), Long.toString(end))
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Perform bitwise operations between strings.
     * 
     * @see <a href="http://redis.io/commands/bitop">http://redis.io/commands/bitop</a>
     */
    public CompletableFuture<Long> bitOp(BitOp op, String destKey, String... srcKeys) {
        Validate.notEmpty(srcKeys, "There must be at least one srcKey");
        String[] params = combineParams(op.toString(), destKey, srcKeys);
        return execute("BITOP", params)
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Find first bit set or clear in a string.
     * 
     * @see <a href="http://redis.io/commands/bitpos">http://redis.io/commands/bitpos</a>
     */
    public CompletableFuture<Long> bitPos(String key, boolean value) {
        return execute("BITPOS", key, boolToStringValue(value))
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Find first bit set or clear in a string.
     * 
     * @see <a href="http://redis.io/commands/bitpos">http://redis.io/commands/bitpos</a>
     */
    public CompletableFuture<Long> bitPos(String key, boolean value, long start, long end) {
        return execute("BITPOS", key, boolToStringValue(value), Long.toString(start), Long.toString(end))
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Decrement the integer value of a key by one.
     * 
     * @see <a href="http://redis.io/commands/decr">http://redis.io/commands/decr</a>
     */
    public CompletableFuture<Long> decr(String key) {
        return execute("DECR", key)
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Decrement the integer value of a key by the given number.
     * 
     * @see <a href="http://redis.io/commands/decrby">http://redis.io/commands/decrby</a>
     */
    public CompletableFuture<Long> decrBy(String key, long decrement) {
        return execute("DECRBY", key, Long.toString(decrement))
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Get the value of a key.
     * 
     * @see <a href="http://redis.io/commands/get">http://redis.io/commands/get</a>
     */
    public CompletableFuture<String> get(String key) {
        return execute("GET", key)
                .thenApply(RedisCommands::bulkStringResponseAsString);
    }

    /**
     * Get the value of a key.
     * 
     * @see <a href="http://redis.io/commands/get">http://redis.io/commands/get</a>
     */
    public CompletableFuture<byte[]> getAsBytes(String key) {
        return execute("GET", key)
                .thenApply(RedisCommands::bulkStringResponseAsBytes);
    }

    /**
     * Returns the bit value at offset in the string value stored at key.
     * 
     * @see <a href="http://redis.io/commands/getbit">http://redis.io/commands/getbit</a>
     */
    public CompletableFuture<Boolean> getBit(String key, int offset) {
        return execute("GETBIT", key, Integer.toString(offset))
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Get a substring of the string stored at a key.
     * 
     * @see <a href="http://redis.io/commands/getrange">http://redis.io/commands/getrange</a>
     */
    public CompletableFuture<String> getRange(String key, int start, int end) {
        return execute("GETRANGE", key, Integer.toString(start), Integer.toString(end))
                .thenApply(RedisCommands::bulkStringResponseAsString);
    }

    /**
     * Get a substring of the string stored at a key.
     * 
     * @see <a href="http://redis.io/commands/getrange">http://redis.io/commands/getrange</a>
     */
    public CompletableFuture<byte[]> getRangeAsBytes(String key, int start, int end) {
        return execute("GETRANGE", key, Integer.toString(start), Integer.toString(end))
                .thenApply(RedisCommands::bulkStringResponseAsBytes);
    }

    /**
     * Set the string value of a key and return its old value.
     * 
     * @see <a href="http://redis.io/commands/getset">http://redis.io/commands/getset</a>
     */
    public CompletableFuture<String> getSet(String key, String value) {
        return execute("GETSET", key, value)
                .thenApply(RedisCommands::bulkStringResponseAsString);
    }

    /**
     * Set the string value of a key and return its old value.
     * 
     * @see <a href="http://redis.io/commands/getset">http://redis.io/commands/getset</a>
     */
    public CompletableFuture<byte[]> getSetBytes(String key, byte[] value) {
        return execute("GETSET", toBytes(key), value)
                .thenApply(RedisCommands::bulkStringResponseAsBytes);
    }

    /**
     * Increment the integer value of a key by one.
     * 
     * @see <a href="http://redis.io/commands/incr">http://redis.io/commands/incr</a>
     */
    public CompletableFuture<Long> incr(String key) {
        return execute("INCR", key)
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Increment the integer value of a key by the given amount.
     * 
     * @see <a href="http://redis.io/commands/incrby">http://redis.io/commands/incrby</a>
     */
    public CompletableFuture<Long> incrBy(String key, long increment) {
        return execute("INCRBY", key, Long.toString(increment))
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Increment the float value of a key by the given amount.
     * 
     * @see <a href="http://redis.io/commands/incrbyfloat">http://redis.io/commands/incrbyfloat</a>
     */
    public CompletableFuture<Double> incrByFloat(String key, double increment) {
        return execute("INCRBYFLOAT", key, Double.toString(increment))
                .thenApply(RedisCommands::bulkStringResponseAsString)
                .thenApply(Double::parseDouble);
    }

    /**
     * Get the values of all the given keys.
     * 
     * @see <a href="http://redis.io/commands/mget">http://redis.io/commands/mget</a>
     */
    public CompletableFuture<String[]> mGet(String... keys) {
        return execute("MGET", keys)
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Get the values of all the given keys.
     * 
     * @see <a href="http://redis.io/commands/mget">http://redis.io/commands/mget</a>
     */
    public CompletableFuture<byte[][]> mGetAsBytes(String... keys) {
        return execute("MGET", keys)
                .thenApply(RedisCommands::multiBulkResponseAsBytes);
    }

    /**
     * Set multiple keys to multiple values.
     *
     * @see <a href="http://redis.io/commands/mset">http://redis.io/commands/mset</a>
     */
    public CompletableFuture<String> mSet(KeyValuePair... keyValuePairs) {
        Validate.notEmpty(keyValuePairs, "There must be at least 1 keyValuePair");
        String[] params = combineParams(keyValuePairs);
        return execute("MSET", params)
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Set multiple keys to multiple values.
     * 
     * @see <a href="http://redis.io/commands/mset">http://redis.io/commands/mset</a>
     */
    public CompletableFuture<String> mSet(KeyValueBytesPair... keyValueBytesPairs) {
        Validate.notEmpty(keyValueBytesPairs, "There must be at least 1 keyValueBytesPair");
        byte[][] params = combineByteParams(keyValueBytesPairs);
        return execute("MSET", params)
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Set multiple keys to multiple values, only if none of the keys exist.
     * 
     * @see <a href="http://redis.io/commands/msetnx">http://redis.io/commands/msetnx</a>
     */
    public CompletableFuture<Boolean> mSetNX(KeyValuePair... keyValuePairs) {
        Validate.notEmpty(keyValuePairs, "There must be at least 1 keyValuePair");
        String[] params = combineParams(keyValuePairs);
        return execute("MSETNX", params)
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Set multiple keys to multiple values, only if none of the keys exist.
     * 
     * @see <a href="http://redis.io/commands/msetnx">http://redis.io/commands/msetnx</a>
     */
    public CompletableFuture<Boolean> mSetNX(KeyValueBytesPair... keyValueBytesPairs) {
        Validate.notEmpty(keyValueBytesPairs, "There must be at least 1 keyValueBytesPair");
        byte[][] params = combineByteParams(keyValueBytesPairs);
        return execute("MSETNX", params)
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Set the value and expiration in milliseconds of a key.
     * 
     * @see <a href="http://redis.io/commands/psetex">http://redis.io/commands/psetex</a>
     */
    public CompletableFuture<String> pSetEx(String key, int expirationMillis, String value) {
        return execute("PSETEX", key, Integer.toString(expirationMillis), value)
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Set the value and expiration in milliseconds of a key.
     * 
     * @see <a href="http://redis.io/commands/psetex">http://redis.io/commands/psetex</a>
     */
    public CompletableFuture<String> pSetEx(String key, int expirationMillis, byte[] value) {
        return execute("PSETEX", toBytes(key), toBytes(expirationMillis), value)
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Set the string value of a key.
     * 
     * @see <a href="http://redis.io/commands/set">http://redis.io/commands/set</a>
     */
    public CompletableFuture<String> set(String key, String value) {
        return execute("SET", key, value)
                .thenApply(RedisCommands::simpleOrNullResponse);
    }

    /**
     * Set the string value of a key.
     * 
     * @see <a href="http://redis.io/commands/set">http://redis.io/commands/set</a>
     */
    public CompletableFuture<String> set(String key, byte[] value) {
        return execute("SET", toBytes(key), value)
                .thenApply(RedisCommands::simpleOrNullResponse);
    }

    /**
     * Sets or clears the bit at offset in the string value stored at key.
     * 
     * @see <a href="http://redis.io/commands/setbit">http://redis.io/commands/setbit</a>
     */
    public CompletableFuture<Boolean> setBit(String key, int offset, boolean value) {
        String offsetString = Integer.toString(offset);
        String valueString = boolToStringValue(value);
        return execute("SETBIT", key, offsetString, valueString)
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Set the value and expiration of a key.
     * 
     * @see <a href="http://redis.io/commands/setex">http://redis.io/commands/setex</a>
     */
    public CompletableFuture<String> setEx(String key, int expirationSeconds, String value) {
        return execute("SETEX", key, Integer.toString(expirationSeconds), value)
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Set the value and expiration of a key.
     * 
     * @see <a href="http://redis.io/commands/setex">http://redis.io/commands/setex</a>
     */
    public CompletableFuture<String> setEx(String key, int expirationSeconds, byte[] value) {
        return execute("SETEX", toBytes(key), toBytes(expirationSeconds), value)
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Set the value of a key, only if the key does not exist.
     * 
     * @see <a href="http://redis.io/commands/setnx">http://redis.io/commands/setnx</a>
     */
    public CompletableFuture<Boolean> setNX(String key, String value) {
        return execute("SETNX", key, value)
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Set the value of a key, only if the key does not exist.
     * 
     * @see <a href="http://redis.io/commands/setnx">http://redis.io/commands/setnx</a>
     */
    public CompletableFuture<Boolean> setNX(String key, byte[] value) {
        return execute("SETNX", key.getBytes(StandardCharsets.UTF_8), value)
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Overwrite part of a string at key starting at the specified offset.
     * 
     * @see <a href="http://redis.io/commands/setrange">http://redis.io/commands/setrange</a>
     */
    public CompletableFuture<Long> setRange(String key, int offset, String value) {
        String offsetString = Integer.toString(offset);
        return execute("SETRANGE", key, offsetString, value)
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Overwrite part of a string at key starting at the specified offset.
     * 
     * @see <a href="http://redis.io/commands/setrange">http://redis.io/commands/setrange</a>
     */
    public CompletableFuture<Long> setRange(String key, int offset, byte[] value) {
        return execute("SETRANGE", toBytes(key), toBytes(offset), value)
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Get the length of the value stored in a key.
     * 
     * @see <a href="http://redis.io/commands/strlen">http://redis.io/commands/strlen</a>
     */
    public CompletableFuture<Long> strLen(String key) {
        return execute("STRLEN", key)
                .thenApply(RedisCommands::integerResponseAsLong);
    }



    // Keys Commands



    /**
     * Delete a key.
     * 
     * @see <a href="http://redis.io/commands/del">http://redis.io/commands/del</a>
     */
    public CompletableFuture<Long> del(String... keys) {
        return execute("DEL", keys)
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Return a serialized version of the value stored at the specified key.
     * 
     * @see <a href="http://redis.io/commands/dump">http://redis.io/commands/dump</a>
     */
    public CompletableFuture<byte[]> dump(String key) {
        return execute("DUMP", key)
                .thenApply(RedisCommands::bulkStringResponseAsBytes);
    }

    /**
     * Determine if a key exists.
     * 
     * @see <a href="http://redis.io/commands/exists">http://redis.io/commands/exists</a>
     */
    public CompletableFuture<Long> exists(String... keys) {
        return execute("EXISTS", keys)
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Set a key's time to live in seconds.
     * 
     * @see <a href="http://redis.io/commands/expire">http://redis.io/commands/expire</a>
     */
    public CompletableFuture<Boolean> expire(String key, int seconds) {
        return execute("EXPIRE", key, Integer.toString(seconds))
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Set the expiration for a key as a UNIX timestamp.
     *
     * @see <a href="http://redis.io/commands/expireat">http://redis.io/commands/expireat</a>
     */
    public CompletableFuture<Boolean> expireAt(String key, long timestamp) {
        return execute("EXPIREAT", key, Long.toString(timestamp))
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Find all keys matching the given pattern.
     * 
     * @see <a href="http://redis.io/commands/keys">http://redis.io/commands/keys</a>
     */
    public CompletableFuture<String[]> keys(String pattern) {
        return execute("KEYS", pattern)
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Move a key to another database.
     * 
     * @see <a href="http://redis.io/commands/move">http://redis.io/commands/move</a>
     */
    public CompletableFuture<Boolean> move(String key, int db) {
        return execute("MOVE", key, Integer.toString(db))
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Remove the expiration from a key.
     * 
     * @see <a href="http://redis.io/commands/persist">http://redis.io/commands/persist</a>
     */
    public CompletableFuture<Boolean> persist(String key) {
        return execute("PERSIST", key)
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Set a key's time to live in milliseconds.
     * 
     * @see <a href="http://redis.io/commands/pexpire">http://redis.io/commands/pexpire</a>
     */
    public CompletableFuture<Boolean> pExpire(String key, int millis) {
        return execute("PEXPIRE", key, Integer.toString(millis))
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Set the expiration for a key as a UNIX timestamp specified in milliseconds.
     * 
     * @see <a href="http://redis.io/commands/pexpireat">http://redis.io/commands/pexpireat</a>
     */
    public CompletableFuture<Boolean> pExpireAt(String key, long timestampMillis) {
        return execute("PEXPIREAT", key, Long.toString(timestampMillis))
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Get the time to live for a key in milliseconds.
     * 
     * @see <a href="http://redis.io/commands/pttl">http://redis.io/commands/pttl</a>
     */
    public CompletableFuture<Long> pTTL(String key) {
        return execute("PTTL", key)
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Return a random key from the keyspace.
     * 
     * @see <a href="http://redis.io/commands/randomkey">http://redis.io/commands/randomkey</a>
     */
    public CompletableFuture<String> randomKey() {
        return execute("RANDOMKEY")
                .thenApply(RedisCommands::bulkStringResponseAsString);
    }

    /**
     * Rename a key.
     * 
     * @see <a href="http://redis.io/commands/rename">http://redis.io/commands/rename</a>
     */
    public CompletableFuture<String> rename(String key, String newKey) {
        return execute("RENAME", key, newKey)
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Rename a key, only if the new key does not exist.
     *
     * @see <a href="http://redis.io/commands/renamenx">http://redis.io/commands/renamenx</a>
     */
    public CompletableFuture<Boolean> renameNX(String key, String newKey) {
        return execute("RENAMENX", key, newKey)
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Create a key using the provided serialized value, previously obtained using DUMP.
     *
     * @see <a href="http://redis.io/commands/restore">http://redis.io/commands/restore</a>
     */
    public CompletableFuture<String> restore(String key, int expireMillis, byte[] value) {
        return execute("RESTORE", toBytes(key), toBytes(expireMillis), value)
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Incrementally iterate the keys space.
     *
     * @see <a href="http://redis.io/commands/scan">http://redis.io/commands/scan</a>
     */
    public CompletableFuture<ScanResult<String>> scan(String cursor) {
        return execute("SCAN", cursor)
                .thenApply(RedisCommands.scanResponse(RedisCommands::multiBulkResponseAsString));
    }

    /**
     * Incrementally iterate the keys space.
     *
     * @see <a href="http://redis.io/commands/scan">http://redis.io/commands/scan</a>
     */
    public CompletableFuture<ScanResult<String>> scan(String cursor, String match) {
        return execute("SCAN", cursor, "MATCH", match)
                .thenApply(RedisCommands.scanResponse(RedisCommands::multiBulkResponseAsString));
    }

    /**
     * Incrementally iterate the keys space.
     *
     * @see <a href="http://redis.io/commands/scan">http://redis.io/commands/scan</a>
     */
    public CompletableFuture<ScanResult<String>> scan(String cursor, String match, int count) {
        return execute("SCAN", cursor, "MATCH", match, "COUNT", Integer.toString(count))
                .thenApply(RedisCommands.scanResponse(RedisCommands::multiBulkResponseAsString));
    }

    /**
     * Incrementally iterate the keys space.
     *
     * @see <a href="http://redis.io/commands/scan">http://redis.io/commands/scan</a>
     */
    public CompletableFuture<ScanResult<String>> scan(String cursor, int count) {
        return execute("SCAN", cursor, "COUNT", Integer.toString(count))
                .thenApply(RedisCommands.scanResponse(RedisCommands::multiBulkResponseAsString));
    }

    /**
     * Sort the elements in a list, set or sorted set.
     *
     * @see <a href="http://redis.io/commands/sort">http://redis.io/commands/sort</a>
     */
    public CompletableFuture<String[]> sort(String key, String... sortParams) {
        String[] params = combineParams(key, sortParams);
        return execute("SORT", params)
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Get the time to live for a key.
     *
     * @see <a href="http://redis.io/commands/ttl">http://redis.io/commands/ttl</a>
     */
    public CompletableFuture<Long> ttl(String key) {
        return execute("TTL", key)
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Determine the type stored at key.
     *
     * @see <a href="http://redis.io/commands/type">http://redis.io/commands/type</a>
     */
    public CompletableFuture<String> type(String key) {
        return execute("TYPE", key)
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Wait for the synchronous replication of all the write commands sent in the context of the current connection.
     *
     * @see <a href="http://redis.io/commands/wait">http://redis.io/commands/wait</a>
     */
    public CompletableFuture<Long> wait(int numSlaves, int timeoutMillis) {
        return execute("WAIT", Integer.toString(numSlaves), Integer.toString(timeoutMillis))
                .thenApply(RedisCommands::integerResponseAsLong);
    }



    // List Commands



    /**
     * Remove and get the first element in a list, or block until one is available.
     *
     * @see <a href="http://redis.io/commands/blpop">http://redis.io/commands/blpop</a>
     */
    public CompletableFuture<KeyValuePair> bLPop(String key, int timeoutSeconds, String... keys) {
        String[] params = combineParams(key, keys, Integer.toString(timeoutSeconds));
        return execute("BLPOP", params)
                .thenApply(RedisCommands::keyValueResponse);
    }

    /**
     * Remove and get the last element in a list, or block until one is available.
     *
     * @see <a href="http://redis.io/commands/brpop">http://redis.io/commands/brpop</a>
     */
    public CompletableFuture<KeyValuePair> bRPop(String key, int timeoutSeconds, String... keys) {
        String[] params = combineParams(key, keys, Integer.toString(timeoutSeconds));
        return execute("BRPOP", params)
                .thenApply(RedisCommands::keyValueResponse);
    }

    /**
     * Pop a value from a list, push it to another list and return it; or block until one is available.
     *
     * @see <a href="http://redis.io/commands/brpoplpush">http://redis.io/commands/brpoplpush</a>
     */
    public CompletableFuture<String> bRPopLPush(String srcKey, String dstKey, int timeoutSeconds) {
        String timeout = Integer.toString(timeoutSeconds);
        return execute("BRPOPLPUSH", srcKey, dstKey, timeout)
                .thenApply(RedisCommands::bulkStringResponseAsString);
    }

    /**
     * Get an element from a list by its index.
     *
     * @see <a href="http://redis.io/commands/lindex">http://redis.io/commands/lindex</a>
     */
    public CompletableFuture<String> lIndex(String key, int index) {
        return execute("LINDEX", key, Integer.toString(index))
                .thenApply(RedisCommands::bulkStringResponseAsString);
    }

    /**
     * Insert an element before another element in a list.
     *
     * @see <a href="http://redis.io/commands/linsert">http://redis.io/commands/linsert</a>
     */
    public CompletableFuture<Integer> lInsertBefore(String key, String pivot, String value) {
        return execute("LINSERT", key, "BEFORE", pivot, value)
                .thenApply(RedisCommands::integerResponseAsLong)
                .thenApply(Long::intValue);
    }

    /**
     * Insert an element after another element in a list.
     *
     * @see <a href="http://redis.io/commands/linsert">http://redis.io/commands/linsert</a>
     */
    public CompletableFuture<Integer> lInsertAfter(String key, String pivot, String value) {
        return execute("LINSERT", key, "AFTER", pivot, value)
                .thenApply(RedisCommands::integerResponseAsLong)
                .thenApply(Long::intValue);
    }

    /**
     * Get the length of a list.
     *
     * @see <a href="http://redis.io/commands/llen">http://redis.io/commands/llen</a>
     */
    public CompletableFuture<Integer> lLen(String key) {
        return execute("LLEN", key)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Remove and get the first element in a list.
     *
     * @see <a href="http://redis.io/commands/lpop">http://redis.io/commands/lpop</a>
     */
    public CompletableFuture<String> lPop(String key) {
        return execute("LPOP", key)
                .thenApply(RedisCommands::bulkStringResponseAsString);
    }

    /**
     * Prepend one or multiple values to a list.
     *
     * @see <a href="http://redis.io/commands/lpush">http://redis.io/commands/lpush</a>
     */
    public CompletableFuture<Integer> lPush(String key, String... values) {
        Validate.notEmpty(values, "There must be at least one value");
        String[] params = combineParams(key, values);
        return execute("LPUSH", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Prepend a value to a list, only if the list exists.
     *
     * @see <a href="http://redis.io/commands/lpushx">http://redis.io/commands/lpushx</a>
     */
    public CompletableFuture<Integer> lPushX(String key, String... values) {
        Validate.notEmpty(values, "There must be at least one value");
        String[] params = combineParams(key, values);
        return execute("LPUSHX", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Get a range of elements from a list.
     *
     * @see <a href="http://redis.io/commands/lrange">http://redis.io/commands/lrange</a>
     */
    public CompletableFuture<String[]> lRange(String key, int start, int end) {
        return execute("LRANGE", key, Integer.toString(start), Integer.toString(end))
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Remove elements from a list.
     *
     * @see <a href="http://redis.io/commands/lrem">http://redis.io/commands/lrem</a>
     */
    public CompletableFuture<Integer> lRem(String key, int count, String value) {
        return execute("LREM", key, Integer.toString(count), value)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Set the value of an element in a list by its index.
     *
     * @see <a href="http://redis.io/commands/lset">http://redis.io/commands/lset</a>
     */
    public CompletableFuture<String> lSet(String key, int index, String value) {
        return execute("LSET", key, Integer.toString(index), value)
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Trim a list to the specified range.
     *
     * @see <a href="http://redis.io/commands/ltrim">http://redis.io/commands/ltrim</a>
     */
    public CompletableFuture<String> lTrim(String key, int start, int stop) {
        return execute("LTRIM", key, Integer.toString(start), Integer.toString(stop))
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Remove and get the last element in a list.
     *
     * @see <a href="http://redis.io/commands/rpop">http://redis.io/commands/rpop</a>
     */
    public CompletableFuture<String> rPop(String key) {
        return execute("RPOP", key)
                .thenApply(RedisCommands::bulkStringResponseAsString);
    }

    /**
     * Remove the last element in a list, prepend it to another list and return it.
     *
     * @see <a href="http://redis.io/commands/rpoplpush">http://redis.io/commands/rpoplpush</a>
     */
    public CompletableFuture<String> rPopLPush(String srcKey, String dstKey) {
        return execute("RPOPLPUSH", srcKey, dstKey)
                .thenApply(RedisCommands::bulkStringResponseAsString);
    }

    /**
     * Append one or multiple values to a list.
     *
     * @see <a href="http://redis.io/commands/rpush">http://redis.io/commands/rpush</a>
     */
    public CompletableFuture<Integer> rPush(String key, String... values) {
        Validate.notEmpty(values, "There must be at least one value");
        String[] params = combineParams(key, values);
        return execute("RPUSH", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Append a value to a list, only if the list exists.
     *
     * @see <a href="http://redis.io/commands/rpushx">http://redis.io/commands/rpushx</a>
     */
    public CompletableFuture<Integer> rPushX(String key, String... values) {
        Validate.notEmpty(values, "There must be at least one value");
        String[] params = combineParams(key, values);
        return execute("RPUSHX", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }



    // Hash Commands



    /**
     * Delete one or more hash fields.
     *
     * @see <a href="http://redis.io/commands/hdel">http://redis.io/commands/hdel</a>
     */
    public CompletableFuture<Integer> hDel(String key, String field, String... fields) {
        String[] params = combineParams(key, field, fields);
        return execute("HDEL", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Determine if a hash field exists.
     *
     * @see <a href="http://redis.io/commands/hexists">http://redis.io/commands/hexists</a>
     */
    public CompletableFuture<Boolean> hExists(String key, String field) {
        return execute("HEXISTS", key, field)
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Get the value of a hash field.
     *
     * @see <a href="http://redis.io/commands/hget">http://redis.io/commands/hget</a>
     */
    public CompletableFuture<String> hGet(String key, String field) {
        return execute("HGET", key, field)
                .thenApply(RedisCommands::bulkStringResponseAsString);
    }

    /**
     * Get all the fields and values in a hash.
     *
     * @see <a href="http://redis.io/commands/hgetall">http://redis.io/commands/hgetall</a>
     */
    public CompletableFuture<KeyValuePair[]> hGetAll(String key) {
        return execute("HGETALL", key)
                .thenApply(RedisCommands::multiBulkResponseAsKeyValuePairs);
    }

    /**
     * Increment the integer value of a hash field by the given number.
     *
     * @see <a href="http://redis.io/commands/hincrby">http://redis.io/commands/hincrby</a>
     */
    public CompletableFuture<Long> hIncrBy(String key, String field, long increment) {
        return execute("HINCRBY", key, field, Long.toString(increment))
                .thenApply(RedisCommands::integerResponseAsLong);
    }

    /**
     * Increment the float value of a hash field by the given amount.
     *
     * @see <a href="http://redis.io/commands/hincrbyfloat">http://redis.io/commands/hincrbyfloat</a>
     */
    public CompletableFuture<Double> hIncrByFloat(String key, String field, double increment) {
        return execute("HINCRBYFLOAT", key, field, Double.toString(increment))
                .thenApply(RedisCommands::bulkStringResponseAsString)
                .thenApply(Double::parseDouble);
    }

    /**
     * Get all the fields in a hash.
     *
     * @see <a href="http://redis.io/commands/hkeys">http://redis.io/commands/hkeys</a>
     */
    public CompletableFuture<String[]> hKeys(String key) {
        return execute("HKEYS", key)
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Get the number of fields in a hash.
     *
     * @see <a href="http://redis.io/commands/hlen">http://redis.io/commands/hlen</a>
     */
    public CompletableFuture<Integer> hLen(String key) {
        return execute("HLEN", key)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Get the values of all the given hash fields.
     *
     * @see <a href="http://redis.io/commands/hmget">http://redis.io/commands/hmget</a>
     */
    public CompletableFuture<String[]> hMGet(String key, String... fields) {
        Validate.notEmpty(fields, "fields must have at least one value");
        String[] params = combineParams(key, fields);
        return execute("HMGET", params)
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Set multiple hash fields to multiple values.
     *
     * @see <a href="http://redis.io/commands/hmset">http://redis.io/commands/hmset</a>
     */
    public CompletableFuture<String> hMSet(String key, KeyValuePair... pairs) {
        Validate.notEmpty(pairs, "There must be at least one pair");
        String[] params = combineParams(key, pairs);
        return execute("HMSET", params)
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Incrementally iterate hash fields and associated values.
     *
     * @see <a href="http://redis.io/commands/hscan">http://redis.io/commands/hscan</a>
     */
    public CompletableFuture<ScanResult<KeyValuePair>> hScan(String key, String cursor) {
        return execute("HSCAN", key, cursor)
                .thenApply(RedisCommands.scanResponse(RedisCommands::multiBulkResponseAsKeyValuePairs));
    }

    /**
     * Incrementally iterate hash fields and associated values.
     *
     * @see <a href="http://redis.io/commands/hscan">http://redis.io/commands/hscan</a>
     */
    public CompletableFuture<ScanResult<KeyValuePair>> hScan(String key, String cursor, String match) {
        return execute("HSCAN", key, cursor, "MATCH", match)
                .thenApply(RedisCommands.scanResponse(RedisCommands::multiBulkResponseAsKeyValuePairs));
    }

    /**
     * Incrementally iterate hash fields and associated values.
     *
     * @see <a href="http://redis.io/commands/hscan">http://redis.io/commands/hscan</a>
     */
    public CompletableFuture<ScanResult<KeyValuePair>> hScan(String key, String cursor, int count) {
        return execute("HSCAN", key, cursor, "COUNT", Integer.toString(count))
                .thenApply(RedisCommands.scanResponse(RedisCommands::multiBulkResponseAsKeyValuePairs));
    }

    /**
     * Incrementally iterate hash fields and associated values.
     *
     * @see <a href="http://redis.io/commands/hscan">http://redis.io/commands/hscan</a>
     */
    public CompletableFuture<ScanResult<KeyValuePair>> hScan(String key, String cursor, String match, int count) {
        return execute("HSCAN", key, cursor, "MATCH", match, "COUNT", Integer.toString(count))
                .thenApply(RedisCommands.scanResponse(RedisCommands::multiBulkResponseAsKeyValuePairs));
    }

    /**
     * Set the string value of a hash field.
     *
     * @see <a href="http://redis.io/commands/hset">http://redis.io/commands/hset</a>
     */
    public CompletableFuture<Boolean> hSet(String key, String field, String value) {
        return execute("HSET", key, field, value)
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Set the value of a hash field, only if the field does not exist.
     *
     * @see <a href="http://redis.io/commands/hsetnx">http://redis.io/commands/hsetnx</a>
     */
    public CompletableFuture<Boolean> hSetNX(String key, String field, String value) {
        return execute("HSETNX", key, field, value)
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Get the length of the value of a hash field.
     *
     * @see <a href="http://redis.io/commands/hstrlen">http://redis.io/commands/hstrlen</a>
     */
    public CompletableFuture<Integer> hStrLen(String key, String field) {
        return execute("HSTRLEN", key, field)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Get all the values in a hash.
     *
     * @see <a href="http://redis.io/commands/hvals">http://redis.io/commands/hvals</a>
     */
    public CompletableFuture<String[]> hVals(String key) {
        return execute("HVALS", key)
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }



    // Set Commands



    /**
     * Add one or more members to a set.
     *
     * @see <a href="http://redis.io/commands/sadd">http://redis.io/commands/sadd</a>
     */
    public CompletableFuture<Integer> sAdd(String key, String... values) {
        Validate.notEmpty(values, "There must be at least one value");
        String[] params = combineParams(key, values);
        return execute("SADD", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Get the number of members in a set.
     *
     * @see <a href="http://redis.io/commands/scard">http://redis.io/commands/scard</a>
     */
    public CompletableFuture<Integer> sCard(String key) {
        return execute("SCARD", key)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Subtract multiple sets.
     *
     * @see <a href="http://redis.io/commands/sdiff">http://redis.io/commands/sdiff</a>
     */
    public CompletableFuture<String[]> sDiff(String... keys) {
        return execute("SDIFF", keys)
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Subtract multiple sets and store the resulting set in a key.
     *
     * @see <a href="http://redis.io/commands/sdiffstore">http://redis.io/commands/sdiffstore</a>
     */
    public CompletableFuture<Integer> sDiffStore(String destKey, String... keys) {
        String[] params = combineParams(destKey, keys);
        return execute("SDIFFSTORE", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Intersect multiple sets.
     *
     * @see <a href="http://redis.io/commands/sinter">http://redis.io/commands/sinter</a>
     */
    public CompletableFuture<String[]> sInter(String... keys) {
        return execute("SINTER", keys)
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Intersect multiple sets and store the resulting set in a key.
     *
     * @see <a href="http://redis.io/commands/sinterstore">http://redis.io/commands/sinterstore</a>
     */
    public CompletableFuture<Integer> sInterStore(String destKey, String... keys) {
        String[] params = combineParams(destKey, keys);
        return execute("SINTERSTORE", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Determine if a given value is a member of a set.
     *
     * @see <a href="http://redis.io/commands/sismember">http://redis.io/commands/sismember</a>
     */
    public CompletableFuture<Boolean> sIsMember(String key, String member) {
        return execute("SISMEMBER", key, member)
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Get all the members in a set.
     *
     * @see <a href="http://redis.io/commands/smembers">http://redis.io/commands/smembers</a>
     */
    public CompletableFuture<String[]> sMembers(String key) {
        return execute("SMEMBERS", key)
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Move a member from one set to another.
     *
     * @see <a href="http://redis.io/commands/smove">http://redis.io/commands/smove</a>
     */
    public CompletableFuture<Boolean> sMove(String key, String destKey, String member) {
        return execute("SMOVE", key, destKey, member)
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Remove and return one or multiple random members from a set.
     *
     * @see <a href="http://redis.io/commands/spop">http://redis.io/commands/spop</a>
     */
    public CompletableFuture<String> sPop(String key) {
        return execute("SPOP", key)
                .thenApply(RedisCommands::bulkStringResponseAsString);
    }

    /**
     * Get one or multiple random members from a set.
     *
     * @see <a href="http://redis.io/commands/srandmember">http://redis.io/commands/srandmember</a>
     */
    public CompletableFuture<String[]> sRandMember(String key, int count) {
        return execute("SRANDMEMBER", key, Integer.toString(count))
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Remove one or more members from a set.
     *
     * @see <a href="http://redis.io/commands/srem">http://redis.io/commands/srem</a>
     */
    public CompletableFuture<Integer> sRem(String key, String... values) {
        Validate.notEmpty(values, "There must be at least one value");
        String[] params = combineParams(key, values);
        return execute("SREM", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Incrementally iterate Set elements.
     *
     * @see <a href="http://redis.io/commands/sscan">http://redis.io/commands/sscan</a>
     */
    public CompletableFuture<ScanResult<String>> sScan(String key, String cursor) {
        return execute("SSCAN", key, cursor)
                .thenApply(RedisCommands.scanResponse(RedisCommands::multiBulkResponseAsString));
    }

    /**
     * Incrementally iterate Set elements.
     *
     * @see <a href="http://redis.io/commands/sscan">http://redis.io/commands/sscan</a>
     */
    public CompletableFuture<ScanResult<String>> sScan(String key, String cursor, String match) {
        return execute("SSCAN", key, cursor, "MATCH", match)
                .thenApply(RedisCommands.scanResponse(RedisCommands::multiBulkResponseAsString));
    }

    /**
     * Incrementally iterate Set elements.
     *
     * @see <a href="http://redis.io/commands/sscan">http://redis.io/commands/sscan</a>
     */
    public CompletableFuture<ScanResult<String>> sScan(String key, String cursor, String match, int count) {
        return execute("SSCAN", key, cursor, "MATCH", match, "COUNT", Integer.toString(count))
                .thenApply(RedisCommands.scanResponse(RedisCommands::multiBulkResponseAsString));
    }

    /**
     * Incrementally iterate Set elements.
     *
     * @see <a href="http://redis.io/commands/sscan">http://redis.io/commands/sscan</a>
     */
    public CompletableFuture<ScanResult<String>> sScan(String key, String cursor, int count) {
        return execute("SSCAN", key, cursor, "COUNT", Integer.toString(count))
                .thenApply(RedisCommands.scanResponse(RedisCommands::multiBulkResponseAsString));
    }

    /**
     * Add multiple sets.
     *
     * @see <a href="http://redis.io/commands/sunion">http://redis.io/commands/sunion</a>
     */
    public CompletableFuture<String[]> sUnion(String... keys) {
        return execute("SUNION", keys)
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Add multiple sets and store the resulting set in a key.
     *
     * @see <a href="http://redis.io/commands/sunionstore">http://redis.io/commands/sunionstore</a>
     */
    public CompletableFuture<Integer> sUnionStore(String destKey, String... keys) {
        String[] params = combineParams(destKey, keys);
        return execute("SUNIONSTORE", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }



    // HyperLogLog Commands



    /**
     * Adds the specified elements to the specified HyperLogLog.
     *
     * @see <a href="http://redis.io/commands/pfadd">http://redis.io/commands/pfadd</a>
     */
    public CompletableFuture<Boolean> pfAdd(String key, String... values) {
        Validate.notEmpty(values, "There must be at least one value");
        String[] params = combineParams(key, values);
        return execute("PFADD", params)
                .thenApply(RedisCommands::integerResponseAsBoolean);
    }

    /**
     * Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s).
     *
     * @see <a href="http://redis.io/commands/pfcount">http://redis.io/commands/pfcount</a>
     */
    public CompletableFuture<Integer> pfCount(String key) {
        return execute("PFCOUNT", key)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Merge N different HyperLogLogs into a single one.
     *
     * @see <a href="http://redis.io/commands/pfmerge">http://redis.io/commands/pfmerge</a>
     */
    public CompletableFuture<String> pfMerge(String destKey, String... srcKeys) {
        Validate.notEmpty(srcKeys, "srcKeys can not be empty");
        String[] params = combineParams(destKey, srcKeys);
        return execute("PFMERGE", params)
                .thenApply(RedisCommands::simpleResponse);
    }



    // Sorted Set Commands



    /**
     * Add one or more members to a sorted set, or update its score if it already exists.
     *
     * @see <a href="http://redis.io/commands/zadd">http://redis.io/commands/zadd</a>
     */
    public CompletableFuture<Integer> zAdd(String key, MemberScorePair... pairs) {
        Validate.notEmpty(pairs, "There must be at least one pair");
        String[] params = combineParams(key, pairs);
        return execute("ZADD", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Get the number of members in a sorted set.
     *
     * @see <a href="http://redis.io/commands/zcard">http://redis.io/commands/zcard</a>
     */
    public CompletableFuture<Integer> zCard(String key) {
        return execute("ZCARD", key)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Count the members in a sorted set with scores within the given values.
     *
     * @see <a href="http://redis.io/commands/zcount">http://redis.io/commands/zcount</a>
     */
    public CompletableFuture<Integer> zCount(String key, double min, double max) {
        return execute("ZCOUNT", key, Double.toString(min), Double.toString(max))
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Increment the score of a member in a sorted set.
     *
     * @see <a href="http://redis.io/commands/zincrby">http://redis.io/commands/zincrby</a>
     */
    public CompletableFuture<Double> zIncrBy(String key, double incr, String member) {
        return execute("ZINCRBY", key, Double.toString(incr), member)
                .thenApply(RedisCommands::bulkStringResponseAsDouble);
    }

    /**
     * Intersect multiple sorted sets and store the resulting sorted set in a new key.
     *
     * @see <a href="http://redis.io/commands/zinterstore">http://redis.io/commands/zinterstore</a>
     */
    public CompletableFuture<Integer> zInterStore(String destKey, String... keys) {
        Validate.notEmpty(keys, "There must be at least one key");
        String[] params = combineParams(destKey, Integer.toString(keys.length), keys);
        return execute("ZINTERSTORE", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Intersect multiple sorted sets and store the resulting sorted set in a new key.
     *
     * @see <a href="http://redis.io/commands/zinterstore">http://redis.io/commands/zinterstore</a>
     */
    public CompletableFuture<Integer> zInterStore(String destKey, Aggregate aggregate, String... keys) {
        Validate.notEmpty(keys, "There must be at least one key");
        String[] params = combineParams(destKey, Integer.toString(keys.length), keys, "AGGREGATE", aggregate.toString());
        return execute("ZINTERSTORE", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Intersect multiple sorted sets and store the resulting sorted set in a new key.
     *
     * @see <a href="http://redis.io/commands/zinterstore">http://redis.io/commands/zinterstore</a>
     */
    public CompletableFuture<Integer> zInterStore(String destKey, KeyWeightPair... keyWeightPairs) {
        Validate.notEmpty(keyWeightPairs, "There must be at least one keyWeightPair");
        String[] params = combineParams(destKey, keyWeightPairs);
        return execute("ZINTERSTORE", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Intersect multiple sorted sets and store the resulting sorted set in a new key.
     *
     * @see <a href="http://redis.io/commands/zinterstore">http://redis.io/commands/zinterstore</a>
     */
    public CompletableFuture<Integer> zInterStore(String destKey, Aggregate aggregate, KeyWeightPair... keyWeightPairs) {
        Validate.notEmpty(keyWeightPairs, "There must be at least one keyWeightPair");
        String[] params = combineParams(destKey, keyWeightPairs, "AGGREGATE", aggregate.toString());
        return execute("ZINTERSTORE", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Count the number of members in a sorted set between a given lexicographical range.
     *
     * @see <a href="http://redis.io/commands/zlexcount">http://redis.io/commands/zlexcount</a>
     */
    public CompletableFuture<Integer> zLexCount(String key, String min, String max) {
        return execute("ZLEXCOUNT", key, min, max)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Return a range of members in a sorted set, by index.
     *
     * @see <a href="http://redis.io/commands/zrange">http://redis.io/commands/zrange</a>
     */
    public CompletableFuture<String[]> zRange(String key, int start, int stop) {
        return execute("ZRANGE", key, Integer.toString(start), Integer.toString(stop))
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Return a range of members in a sorted set, by index.
     *
     * @see <a href="http://redis.io/commands/zrange">http://redis.io/commands/zrange</a>
     */
    public CompletableFuture<MemberScorePair[]> zRangeWithScores(String key, int start, int stop) {
        return execute("ZRANGE", key, Integer.toString(start), Integer.toString(stop), "WITHSCORES")
                .thenApply(RedisCommands::multiBulkResponseAsMemberScorePairs);
    }

    /**
     * Return a range of members in a sorted set, by lexicographical range.
     *
     * @see <a href="http://redis.io/commands/zrangebylex">http://redis.io/commands/zrangebylex</a>
     */
    public CompletableFuture<String[]> zRangeByLex(String key, String min, String max) {
        return execute("ZRANGEBYLEX", key, min, max)
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Return a range of members in a sorted set, by lexicographical range.
     *
     * @see <a href="http://redis.io/commands/zrangebylex">http://redis.io/commands/zrangebylex</a>
     */
    public CompletableFuture<String[]> zRangeByLex(String key, String min, String max, int offset, int count) {
        return execute("ZRANGEBYLEX", key, min, max, "LIMIT", Integer.toString(offset), Integer.toString(count))
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Return a range of members in a sorted set, by score.
     *
     * @see <a href="http://redis.io/commands/zrangebyscore">http://redis.io/commands/zrangebyscore</a>
     */
    public CompletableFuture<String[]> zRangeByScore(String key, double min, double max) {
        return execute("ZRANGEBYSCORE", key, Double.toString(min), Double.toString(max))
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Return a range of members in a sorted set, by score.
     *
     * @see <a href="http://redis.io/commands/zrangebyscore">http://redis.io/commands/zrangebyscore</a>
     */
    public CompletableFuture<String[]> zRangeByScore(String key, double min, double max, int offset, int count) {
        return execute("ZRANGEBYSCORE", key, Double.toString(min), Double.toString(max), "LIMIT", Integer.toString(offset), Integer.toString(count))
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Return a range of members in a sorted set, by score.
     *
     * @see <a href="http://redis.io/commands/zrangebyscore">http://redis.io/commands/zrangebyscore</a>
     */
    public CompletableFuture<String[]> zRangeByScore(String key, String min, String max) {
        return execute("ZRANGEBYSCORE", key, min, max)
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Return a range of members in a sorted set, by score.
     *
     * @see <a href="http://redis.io/commands/zrangebyscore">http://redis.io/commands/zrangebyscore</a>
     */
    public CompletableFuture<String[]> zRangeByScore(String key, String min, String max, int offset, int count) {
        return execute("ZRANGEBYSCORE", key, min, max, "LIMIT", Integer.toString(offset), Integer.toString(count))
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Return a range of members in a sorted set, by score.
     *
     * @see <a href="http://redis.io/commands/zrangebyscore">http://redis.io/commands/zrangebyscore</a>
     */
    public CompletableFuture<MemberScorePair[]> zRangeByScoreWithScores(String key, double min, double max) {
        return execute("ZRANGEBYSCORE", key, Double.toString(min), Double.toString(max), "WITHSCORES")
                .thenApply(RedisCommands::multiBulkResponseAsMemberScorePairs);
    }

    /**
     * Return a range of members in a sorted set, by score.
     *
     * @see <a href="http://redis.io/commands/zrangebyscore">http://redis.io/commands/zrangebyscore</a>
     */
    public CompletableFuture<MemberScorePair[]> zRangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return execute(
                "ZRANGEBYSCORE",
                key,
                Double.toString(min),
                Double.toString(max),
                "WITHSCORES",
                "LIMIT",
                Integer.toString(offset),
                Integer.toString(count))
                .thenApply(RedisCommands::multiBulkResponseAsMemberScorePairs);
    }

    /**
     * Return a range of members in a sorted set, by score.
     *
     * @see <a href="http://redis.io/commands/zrangebyscore">http://redis.io/commands/zrangebyscore</a>
     */
    public CompletableFuture<MemberScorePair[]> zRangeByScoreWithScores(String key, String min, String max) {
        return execute("ZRANGEBYSCORE", key, min, max, "WITHSCORES")
                .thenApply(RedisCommands::multiBulkResponseAsMemberScorePairs);
    }

    /**
     * Return a range of members in a sorted set, by score.
     *
     * @see <a href="http://redis.io/commands/zrangebyscore">http://redis.io/commands/zrangebyscore</a>
     */
    public CompletableFuture<MemberScorePair[]> zRangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return execute("ZRANGEBYSCORE", key, min, max, "WITHSCORES", "LIMIT", Integer.toString(offset), Integer.toString(count))
                .thenApply(RedisCommands::multiBulkResponseAsMemberScorePairs);
    }

    /**
     * Determine the index of a member in a sorted set.
     *
     * @see <a href="http://redis.io/commands/zrank">http://redis.io/commands/zrank</a>
     */
    public CompletableFuture<Integer> zRank(String key, String member) {
        return execute("ZRANK", key, member)
                .thenApply(RedisCommands::integerOrNullResponseAsInteger);
    }

    /**
     * Remove one or more members from a sorted set.
     *
     * @see <a href="http://redis.io/commands/zrem">http://redis.io/commands/zrem</a>
     */
    public CompletableFuture<Integer> zRem(String key, String... members) {
        Validate.notEmpty(members, "There must be at least one member to remove");
        String[] params = combineParams(key, members);
        return execute("ZREM", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Remove all members in a sorted set between the given lexicographical range.
     *
     * @see <a href="http://redis.io/commands/zremrangebylex">http://redis.io/commands/zremrangebylex</a>
     */
    public CompletableFuture<Integer> zRemRangeByLex(String key, String min, String max) {
        return execute("ZREMRANGEBYLEX", key, min, max)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Remove all members in a sorted set within the given indexes.
     *
     * @see <a href="http://redis.io/commands/zremrangebyrank">http://redis.io/commands/zremrangebyrank</a>
     */
    public CompletableFuture<Integer> zRemRangeByRank(String key, int start, int stop) {
        return execute("ZREMRANGEBYRANK", key, Integer.toString(start), Integer.toString(stop))
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Remove all members in a sorted set within the given scores.
     *
     * @see <a href="http://redis.io/commands/zremrangebyscore">http://redis.io/commands/zremrangebyscore</a>
     */
    public CompletableFuture<Integer> zRemRangeByScore(String key, double min, double max) {
        return execute("ZREMRANGEBYSCORE", key, Double.toString(min), Double.toString(max))
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Remove all members in a sorted set within the given scores.
     *
     * @see <a href="http://redis.io/commands/zremrangebyscore">http://redis.io/commands/zremrangebyscore</a>
     */
    public CompletableFuture<Integer> zRemRangeByScore(String key, String min, String max) {
        return execute("ZREMRANGEBYSCORE", key, min, max)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Return a range of members in a sorted set, by index, with scores ordered from high to low.
     *
     * @see <a href="http://redis.io/commands/zrevrange">http://redis.io/commands/zrevrange</a>
     */
    public CompletableFuture<String[]> zRevRange(String key, int start, int stop) {
        return execute("ZREVRANGE", key, Integer.toString(start), Integer.toString(stop))
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Return a range of members in a sorted set, by index, with scores ordered from high to low.
     *
     * @see <a href="http://redis.io/commands/zrevrange">http://redis.io/commands/zrevrange</a>
     */
    public CompletableFuture<MemberScorePair[]> zRevRangeWithScores(String key, int start, int stop) {
        return execute("ZREVRANGE", key, Integer.toString(start), Integer.toString(stop), "WITHSCORES")
                .thenApply(RedisCommands::multiBulkResponseAsMemberScorePairs);
    }

    /**
     * Return a range of members in a sorted set, by lexicographical range, ordered from higher to lower strings..
     *
     * @see <a href="http://redis.io/commands/zrevrangebylex">http://redis.io/commands/zrevrangebylex</a>
     */
    public CompletableFuture<String[]> zRevRangeByLex(String key, String min, String max) {
        return execute("ZREVRANGEBYLEX", key, min, max)
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Return a range of members in a sorted set, by lexicographical range, ordered from higher to lower strings..
     *
     * @see <a href="http://redis.io/commands/zrevrangebylex">http://redis.io/commands/zrevrangebylex</a>
     */
    public CompletableFuture<String[]> zRevRangeByLex(String key, String min, String max, int offset, int count) {
        return execute("ZREVRANGEBYLEX", key, min, max, "LIMIT", Integer.toString(offset), Integer.toString(count))
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Return a range of members in a sorted set, by score, with scores ordered from high to low.
     *
     * @see <a href="http://redis.io/commands/zrevrangebyscore">http://redis.io/commands/zrevrangebyscore</a>
     */
    public CompletableFuture<String[]> zRevRangeByScore(String key, double min, double max) {
        return execute("ZREVRANGEBYSCORE", key, Double.toString(min), Double.toString(max))
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Return a range of members in a sorted set, by score, with scores ordered from high to low.
     *
     * @see <a href="http://redis.io/commands/zrevrangebyscore">http://redis.io/commands/zrevrangebyscore</a>
     */
    public CompletableFuture<String[]> zRevRangeByScore(String key, double min, double max, int offset, int count) {
        return execute("ZREVRANGEBYSCORE", key, Double.toString(min), Double.toString(max), "LIMIT", Integer.toString(offset), Integer.toString(count))
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Return a range of members in a sorted set, by score, with scores ordered from high to low.
     *
     * @see <a href="http://redis.io/commands/zrevrangebyscore">http://redis.io/commands/zrevrangebyscore</a>
     */
    public CompletableFuture<String[]> zRevRangeByScore(String key, String min, String max) {
        return execute("ZREVRANGEBYSCORE", key, min, max)
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Return a range of members in a sorted set, by score, with scores ordered from high to low.
     *
     * @see <a href="http://redis.io/commands/zrevrangebyscore">http://redis.io/commands/zrevrangebyscore</a>
     */
    public CompletableFuture<String[]> zRevRangeByScore(String key, String min, String max, int offset, int count) {
        return execute("ZREVRANGEBYSCORE", key, min, max, "LIMIT", Integer.toString(offset), Integer.toString(count))
                .thenApply(RedisCommands::multiBulkResponseAsString);
    }

    /**
     * Return a range of members in a sorted set, by score, with scores ordered from high to low.
     *
     * @see <a href="http://redis.io/commands/zrevrangebyscore">http://redis.io/commands/zrevrangebyscore</a>
     */
    public CompletableFuture<MemberScorePair[]> zRevRangeByScoreWithScores(String key, double min, double max) {
        return execute("ZREVRANGEBYSCORE", key, Double.toString(min), Double.toString(max), "WITHSCORES")
                .thenApply(RedisCommands::multiBulkResponseAsMemberScorePairs);
    }

    /**
     * Return a range of members in a sorted set, by score, with scores ordered from high to low.
     *
     * @see <a href="http://redis.io/commands/zrevrangebyscore">http://redis.io/commands/zrevrangebyscore</a>
     */
    public CompletableFuture<MemberScorePair[]> zRevRangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return execute(
                "ZREVRANGEBYSCORE",
                key,
                Double.toString(min),
                Double.toString(max),
                "WITHSCORES",
                "LIMIT",
                Integer.toString(offset),
                Integer.toString(count))
                .thenApply(RedisCommands::multiBulkResponseAsMemberScorePairs);
    }

    /**
     * Return a range of members in a sorted set, by score, with scores ordered from high to low.
     *
     * @see <a href="http://redis.io/commands/zrevrangebyscore">http://redis.io/commands/zrevrangebyscore</a>
     */
    public CompletableFuture<MemberScorePair[]> zRevRangeByScoreWithScores(String key, String min, String max) {
        return execute("ZREVRANGEBYSCORE", key, min, max, "WITHSCORES")
                .thenApply(RedisCommands::multiBulkResponseAsMemberScorePairs);
    }

    /**
     * Return a range of members in a sorted set, by score, with scores ordered from high to low.
     *
     * @see <a href="http://redis.io/commands/zrevrangebyscore">http://redis.io/commands/zrevrangebyscore</a>
     */
    public CompletableFuture<MemberScorePair[]> zRevRangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return execute("ZREVRANGEBYSCORE", key, min, max, "WITHSCORES", "LIMIT", Integer.toString(offset), Integer.toString(count))
                .thenApply(RedisCommands::multiBulkResponseAsMemberScorePairs);
    }

    /**
     * Determine the index of a member in a sorted set, with scores ordered from high to low.
     *
     * @see <a href="http://redis.io/commands/zrevrank">http://redis.io/commands/zrevrank</a>
     */
    public CompletableFuture<Integer> zRevRank(String key, String member) {
        return execute("ZREVRANK", key, member)
                .thenApply(RedisCommands::integerOrNullResponseAsInteger);
    }

    /**
     * Incrementally iterate sorted sets elements and associated scores.
     *
     * @see <a href="http://redis.io/commands/zscan">http://redis.io/commands/zscan</a>
     */
    public CompletableFuture<ScanResult<MemberScorePair>> zScan(String key, String cursor) {
        return execute("ZSCAN", key, cursor)
                .thenApply(RedisCommands.scanResponse(RedisCommands::multiBulkResponseAsMemberScorePairs));
    }

    /**
     * Incrementally iterate sorted sets elements and associated scores.
     *
     * @see <a href="http://redis.io/commands/zscan">http://redis.io/commands/zscan</a>
     */
    public CompletableFuture<ScanResult<MemberScorePair>> zScan(String key, String cursor, String match) {
        return execute("ZSCAN", key, cursor, "MATCH", match)
                .thenApply(RedisCommands.scanResponse(RedisCommands::multiBulkResponseAsMemberScorePairs));
    }

    /**
     * Incrementally iterate sorted sets elements and associated scores.
     *
     * @see <a href="http://redis.io/commands/zscan">http://redis.io/commands/zscan</a>
     */
    public CompletableFuture<ScanResult<MemberScorePair>> zScan(String key, String cursor, int count) {
        return execute("ZSCAN", key, cursor, "COUNT", Integer.toString(count))
                .thenApply(RedisCommands.scanResponse(RedisCommands::multiBulkResponseAsMemberScorePairs));
    }

    /**
     * Incrementally iterate sorted sets elements and associated scores.
     *
     * @see <a href="http://redis.io/commands/zscan">http://redis.io/commands/zscan</a>
     */
    public CompletableFuture<ScanResult<MemberScorePair>> zScan(String key, String cursor, String match, int count) {
        return execute("ZSCAN", key, cursor, "MATCH", match, "COUNT", Integer.toString(count))
                .thenApply(RedisCommands.scanResponse(RedisCommands::multiBulkResponseAsMemberScorePairs));
    }

    /**
     * Get the score associated with the given member in a sorted set.
     *
     * @see <a href="http://redis.io/commands/zscore">http://redis.io/commands/zscore</a>
     */
    public CompletableFuture<Double> zScore(String key, String member) {
        return execute("ZSCORE", key, member)
                .thenApply(RedisCommands::bulkStringResponseAsDouble);
    }

    /**
     * Add multiple sorted sets and store the resulting sorted set in a new key.
     *
     * @see <a href="http://redis.io/commands/zunionstore">http://redis.io/commands/zunionstore</a>
     */
    public CompletableFuture<Integer> zUnionStore(String destKey, String... keys) {
        Validate.notEmpty(keys, "There must be at least one key");
        String[] params = combineParams(destKey, Integer.toString(keys.length), keys);
        return execute("ZUNIONSTORE", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Add multiple sorted sets and store the resulting sorted set in a new key.
     *
     * @see <a href="http://redis.io/commands/zunionstore">http://redis.io/commands/zunionstore</a>
     */
    public CompletableFuture<Integer> zUnionStore(String destKey, Aggregate aggregate, String... keys) {
        Validate.notEmpty(keys, "There must be at least one key");
        String[] params = combineParams(destKey, Integer.toString(keys.length), keys, "AGGREGATE", aggregate.toString());
        return execute("ZUNIONSTORE", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Add multiple sorted sets and store the resulting sorted set in a new key.
     *
     * @see <a href="http://redis.io/commands/zunionstore">http://redis.io/commands/zunionstore</a>
     */
    public CompletableFuture<Integer> zUnionStore(String destKey, KeyWeightPair... keyWeightPairs) {
        Validate.notEmpty(keyWeightPairs, "There must be at least one keyWeightPair");
        String[] params = combineParams(destKey, keyWeightPairs);
        return execute("ZUNIONSTORE", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }

    /**
     * Add multiple sorted sets and store the resulting sorted set in a new key.
     *
     * @see <a href="http://redis.io/commands/zunionstore">http://redis.io/commands/zunionstore</a>
     */
    public CompletableFuture<Integer> zUnionStore(String destKey, Aggregate aggregate, KeyWeightPair... keyWeightPairs) {
        Validate.notEmpty(keyWeightPairs, "There must be at least one keyWeightPair");
        String[] params = combineParams(destKey, keyWeightPairs, "AGGREGATE", aggregate.toString());
        return execute("ZUNIONSTORE", params)
                .thenApply(RedisCommands::integerResponseAsInteger);
    }



    // Transaction Commands



    /**
     * Discard all commands issued after MULTI.
     *
     * @see <a href="http://redis.io/commands/discard">http://redis.io/commands/discard</a>
     */
    public CompletableFuture<String> discard() {
        return execute("DISCARD")
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Execute all commands issued after MULTI.
     *
     * @see <a href="http://redis.io/commands/exec">http://redis.io/commands/exec</a>
     */
    public CompletableFuture<RedisArray> exec() {
        return execute("EXEC")
                .thenApply(RedisCommands::arrayResponse);
    }

    /**
     * Mark the start of a transaction block.
     *
     * @see <a href="http://redis.io/commands/multi">http://redis.io/commands/multi</a>
     */
    public CompletableFuture<String> multi() {
        return execute("MULTI")
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Forget about all watched keys.
     *
     * @see <a href="http://redis.io/commands/unwatch">http://redis.io/commands/unwatch</a>
     */
    public CompletableFuture<String> unwatch() {
        return execute("UNWATCH")
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Watch the given keys to determine execution of the MULTI/EXEC block.
     *
     * @see <a href="http://redis.io/commands/watch">http://redis.io/commands/watch</a>
     */
    public CompletableFuture<String> watch(String... keys) {
        return execute("WATCH", keys)
                .thenApply(RedisCommands::simpleResponse);
    }



    // Server Commands



    /**
     * Asynchronously rewrite the append-only file.
     *
     * @see <a href="http://redis.io/commands/bgrewriteaof">http://redis.io/commands/bgrewriteaof</a>
     */
    public CompletableFuture<String> bgRewriteAOF() {
        return execute("BGREWRITEAOF")
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Asynchronously save the dataset to disk.
     *
     * @see <a href="http://redis.io/commands/bgsave">http://redis.io/commands/bgsave</a>
     */
    public CompletableFuture<String> bgSave() {
        return execute("BGSAVE")
                .thenApply(RedisCommands::simpleResponse);
    }

    /**
     * Remove all keys from the current database.
     *
     * @see <a href="http://redis.io/commands/flushdb">http://redis.io/commands/flushdb</a>
     */
    public CompletableFuture<String> flushDb() {
        return execute("FLUSHDB")
                .thenApply(RedisCommands::simpleResponse);
    }



    // Utility Methods



    String boolToStringValue(boolean boolValue) {
        return boolValue ? "1" : "0";
    }

    byte[] toBytes(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }

    byte[] toBytes(int value) {
        return Integer.toString(value).getBytes(StandardCharsets.UTF_8);
    }

    String[] combineParams(String head, String[] rest) {
        String[] params = new String[rest.length + 1];
        params[0] = head;
        System.arraycopy(rest, 0, params, 1, rest.length);
        return params;
    }

    String[] combineParams(String head, String[] rest, String tail) {
        String[] params = new String[rest.length + 2];
        params[0] = head;
        System.arraycopy(rest, 0, params, 1, rest.length);
        params[params.length - 1] = tail;
        return params;
    }

    String[] combineParams(String head, String head2, String[] rest) {
        String[] params = new String[rest.length + 2];
        params[0] = head;
        params[1] = head2;
        System.arraycopy(rest, 0, params, 2, rest.length);
        return params;
    }

    String[] combineParams(String head, String head2, String[] rest, String tail1, String tail2) {
        String[] params = new String[rest.length + 4];
        params[0] = head;
        params[1] = head2;
        params[params.length - 1] = tail2;
        params[params.length - 2] = tail1;
        System.arraycopy(rest, 0, params, 2, rest.length);
        return params;
    }

    String[] combineParams(String head, KeyValuePair... pairs) {
        String[] params = new String[pairs.length * 2 + 1];
        params[0] = head;
        for (int i = 0; i < pairs.length; ++i) {
            params[i * 2 + 1] = pairs[i].getKey();
            params[i * 2 + 2] = pairs[i].getValue();
        }
        return params;
    }

    String[] combineParams(KeyValuePair... pairs) {
        String[] params = new String[pairs.length * 2];
        for (int i = 0; i < pairs.length; ++i) {
            params[i * 2] = pairs[i].getKey();
            params[i * 2 + 1] = pairs[i].getValue();
        }
        return params;
    }

    byte[][] combineByteParams(KeyValueBytesPair... pairs) {
        byte[][] params = new byte[pairs.length * 2][];
        for (int i = 0; i < pairs.length; ++i) {
            params[i * 2] = toBytes(pairs[i].getKey());
            params[i * 2 + 1] = pairs[i].getValueBytes();
        }
        return params;
    }

    String[] combineParams(String head, MemberScorePair... pairs) {
        String[] params = new String[pairs.length * 2 + 1];
        params[0] = head;
        for (int i = 0; i < pairs.length; ++i) {
            params[i * 2 + 1] = Double.toString(pairs[i].getScore());
            params[i * 2 + 2] = pairs[i].getMember();
        }
        return params;
    }

    String[] combineParams(String head1, KeyWeightPair... pairs) {
        String[] params = new String[pairs.length * 2 + 3];
        params[0] = head1;
        params[1] = Integer.toString(pairs.length);
        params[pairs.length + 2] = "WEIGHTS";
        for (int i = 0; i < pairs.length; ++i) {
            params[i + 2] = pairs[i].getKey();
            params[pairs.length + i + 3] = Double.toString(pairs[i].getWeight());
        }
        return params;
    }

    String[] combineParams(String head1, KeyWeightPair[] pairs, String tail1, String tail2) {
        String[] params = new String[pairs.length * 2 + 5];
        params[0] = head1;
        params[1] = Integer.toString(pairs.length);
        params[pairs.length + 2] = "WEIGHTS";
        params[params.length - 2] = tail1;
        params[params.length - 1] = tail2;
        for (int i = 0; i < pairs.length; ++i) {
            params[i + 2] = pairs[i].getKey();
            params[pairs.length + i + 3] = Double.toString(pairs[i].getWeight());
        }
        return params;
    }
}
