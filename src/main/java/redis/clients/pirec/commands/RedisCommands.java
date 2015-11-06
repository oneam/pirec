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
package redis.clients.pirec.commands;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import redis.clients.pirec.codec.Validate;
import redis.clients.pirec.codec.object.RedisArray;
import redis.clients.pirec.codec.object.RedisBulkString;
import redis.clients.pirec.codec.object.RedisError;
import redis.clients.pirec.codec.object.RedisInteger;
import redis.clients.pirec.codec.object.RedisNull;
import redis.clients.pirec.codec.object.RedisObject;
import redis.clients.pirec.codec.object.RedisSimple;

public class RedisCommands {

    private RedisCommands() {
        throw new UnsupportedOperationException();
    }

    public static RedisObject createRequest(String command) {
        Validate.notEmpty(command, "command must not be null or empty");
        return RedisObject.array(RedisObject.bulkString(command));
    }

    public static RedisObject createRequest(String command, byte[]... params) {
        Validate.notEmpty(command, "command must not be null or empty");
        RedisArray request = RedisObject.array(params.length + 1);
        request.set(0, RedisObject.bulkString(command));
        for (int i = 0; i < params.length; ++i) {
            Validate.notNull(params[i], "All params must be non-null");
            request.set(i + 1, RedisObject.bulkString(params[i]));
        }

        return request;
    }

    public static RedisObject createRequest(String command, String... params) {
        Validate.notEmpty(command, "command must not be null or empty");
        RedisArray request = RedisObject.array(params.length + 1);
        request.set(0, RedisObject.bulkString(command));
        for (int i = 0; i < params.length; ++i) {
            Validate.notNull(params[i], "All params must be non-null");
            request.set(i + 1, RedisObject.bulkString(params[i]));
        }

        return request;
    }

    public static byte[] bulkStringResponseAsBytes(RedisObject response) {
        checkForError(response);
        if (isNull(response)) return null;
        RedisBulkString bulkString = checkType(response, RedisBulkString.class);

        return bulkString.getContent();
    }

    public static String bulkStringResponseAsString(RedisObject response) {
        byte[] bytes = bulkStringResponseAsBytes(response);
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    public static Double bulkStringResponseAsDouble(RedisObject response) {
        String string = bulkStringResponseAsString(response);
        return string == null ? null : Double.parseDouble(string);
    }

    public static String[] multiBulkResponseAsString(RedisObject response) {
        checkForError(response);
        if (isNull(response)) return null;
        RedisArray array = checkType(response, RedisArray.class);
        return array.stream().map(RedisCommands::bulkStringResponseAsString).toArray(String[]::new);
    }

    public static KeyValuePair[] multiBulkResponseAsKeyValuePairs(RedisObject response) {
        checkForError(response);
        if (isNull(response)) return null;
        RedisArray array = checkType(response, RedisArray.class);
        int numPairs = array.size() / 2;
        KeyValuePair[] pairs = new KeyValuePair[numPairs];
        for (int i = 0; i < numPairs; ++i) {
            String key = bulkStringResponseAsString(array.get(i * 2));
            String value = bulkStringResponseAsString(array.get(i * 2 + 1));
            pairs[i] = new KeyValuePair(key, value);
        }

        return pairs;
    }

    public static MemberScorePair[] multiBulkResponseAsMemberScorePairs(RedisObject response) {
        checkForError(response);
        if (isNull(response)) return null;
        RedisArray array = checkType(response, RedisArray.class);
        int numPairs = array.size() / 2;
        MemberScorePair[] pairs = new MemberScorePair[numPairs];
        for (int i = 0; i < numPairs; ++i) {
            String member = bulkStringResponseAsString(array.get(i * 2));
            double score = bulkStringResponseAsDouble(array.get(i * 2 + 1));
            pairs[i] = new MemberScorePair(member, score);
        }

        return pairs;
    }

    public static byte[][] multiBulkResponseAsBytes(RedisObject response) {
        checkForError(response);
        if (isNull(response)) return null;
        RedisArray array = checkType(response, RedisArray.class);
        return array.stream().map(RedisCommands::bulkStringResponseAsBytes).toArray(byte[][]::new);
    }

    public static String simpleResponse(RedisObject response) {
        checkForError(response);
        RedisSimple simple = checkType(response, RedisSimple.class);

        return simple.getValue();
    }

    public static RedisArray arrayResponse(RedisObject response) {
        checkForError(response);
        RedisArray array = checkType(response, RedisArray.class);

        return array;
    }

    public static String simpleOrNullResponse(RedisObject response) {
        checkForError(response);
        if (isNull(response)) return null;
        RedisSimple simple = checkType(response, RedisSimple.class);

        return simple.getValue();
    }

    public static Long integerOrNullResponseAsLong(RedisObject response) {
        checkForError(response);
        if (isNull(response)) return null;
        RedisInteger integer = checkType(response, RedisInteger.class);

        return integer.getValue();
    }

    public static Integer integerOrNullResponseAsInteger(RedisObject response) {
        Long longValue = integerOrNullResponseAsLong(response);
        return longValue == null ? null : longValue.intValue();
    }

    public static long integerResponseAsLong(RedisObject response) {
        checkForError(response);
        RedisInteger integer = checkType(response, RedisInteger.class);

        return integer.getValue();
    }

    public static int integerResponseAsInteger(RedisObject response) {
        return (int) integerResponseAsLong(response);
    }

    public static boolean integerResponseAsBoolean(RedisObject response) {
        return integerResponseAsLong(response) > 0;
    }

    public static <T> Function<RedisObject, ScanResult<T>> scanResponse(Function<RedisObject, T[]> elementMapper) {
        return response -> {
            checkForError(response);
            RedisArray array = checkType(response, RedisArray.class);

            String cursor = bulkStringResponseAsString(array.get(0));
            RedisArray elements = checkType(array.get(1), RedisArray.class);
            T[] values = elementMapper.apply(elements);
            return new ScanResult<>(cursor, values);
        };
    }

    public static KeyValuePair keyValueResponse(RedisObject response) {
        checkForError(response);
        RedisArray array = checkType(response, RedisArray.class);

        String key = bulkStringResponseAsString(array.get(0));
        String value = bulkStringResponseAsString(array.get(1));
        return new KeyValuePair(key, value);
    }

    public static boolean isNull(RedisObject response) {
        return RedisNull.class.isInstance(response);
    }

    public static <T extends RedisObject> T checkType(RedisObject response, Class<T> type)
            throws UncheckedIOException {
        if (!type.isInstance(response)) {
            String message = String.format(
                    "Invalid response type %s, expected a %s or RedisError",
                    response.getClass().getSimpleName(),
                    type.getSimpleName());
            IOException cause = new RedisInvalidResponseException(message);
            throw new UncheckedIOException(cause);
        }

        return type.cast(response);
    }

    public static void checkForError(RedisObject response) throws UncheckedIOException {
        Validate.notNull(response, "message must not be null");

        if (response instanceof RedisError) {
            IOException cause = new RedisResponseException(RedisError.class.cast(response));
            throw new UncheckedIOException(cause);
        }
    }

    public static class RedisInvalidResponseException extends IOException {
        private static final long serialVersionUID = 3357646810499871711L;

        public RedisInvalidResponseException(String message) {
            super(message);
        }
    }

    public static class RedisResponseException extends IOException {
        private static final long serialVersionUID = 8189487697406598790L;
        private final RedisError response;

        public RedisResponseException(RedisError response) {
            super(response.getMessage());
            this.response = response;
        }

        public RedisError getResponse() {
            return response;
        }
    }
}
