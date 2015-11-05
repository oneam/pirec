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
import java.util.ArrayList;
import java.util.Arrays;

import redis.clients.pirec.codec.object.RedisArray;
import redis.clients.pirec.codec.object.RedisBulkString;
import redis.clients.pirec.codec.object.RedisError;
import redis.clients.pirec.codec.object.RedisInteger;
import redis.clients.pirec.codec.object.RedisNull;
import redis.clients.pirec.codec.object.RedisObject;
import redis.clients.pirec.codec.object.RedisSimple;

public class RedisEncoder {
    static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);
    static final byte[] SIMPLE_STRING_BYTE = "+".getBytes(StandardCharsets.UTF_8);
    static final byte[] ERROR_BYTE = "-".getBytes(StandardCharsets.UTF_8);
    static final byte[] ARRAY_BYTE = "*".getBytes(StandardCharsets.UTF_8);
    static final byte[] BULK_STRING_BYTE = "$".getBytes(StandardCharsets.UTF_8);
    static final byte[] INTEGER_BYTE = ":".getBytes(StandardCharsets.UTF_8);
    static final byte[] NULL_BULK_STRING = "$-1\r\n".getBytes(StandardCharsets.UTF_8);
    static final byte[] NULL_ARRAY = "*-1\r\n".getBytes(StandardCharsets.UTF_8);

    public int encode(RedisObject object, ByteBuffer dest) throws RedisEncodeException {
        byte[][] encodedBytes = encode(object);
        int totalBytes = Arrays.stream(encodedBytes).mapToInt(Array::getLength).sum();
        if (dest.remaining() >= totalBytes) {
            for (byte[] bytes : encodedBytes) {
                dest.put(bytes);
            }
            return totalBytes;
        } else {
            return 0;
        }
    }

    public byte[][] encode(RedisObject object) throws RedisEncodeException {
        if (object instanceof RedisSimple) {
            return encodeSimple((RedisSimple) object);
        } else if (object instanceof RedisError) {
            return encodeError((RedisError) object);
        } else if (object instanceof RedisInteger) {
            return encodeInteger((RedisInteger) object);
        } else if (object instanceof RedisBulkString) {
            return encodeBulkString((RedisBulkString) object);
        } else if (object instanceof RedisNull) {
            return encodeNull((RedisNull) object);
        } else if (object instanceof RedisArray) {
            return encodeArray((RedisArray) object);
        } else {
            throw new RedisEncodeException(String.format("Unknown RedisObject Type %s", object.getClass().getName()));
        }
    }

    protected byte[][] encodeSimple(RedisSimple object) throws RedisEncodeException {
        byte[] content = object.getValue().getBytes(StandardCharsets.UTF_8);
        return new byte[][] { SIMPLE_STRING_BYTE, content, CRLF };
    }

    protected byte[][] encodeError(RedisError object) throws RedisEncodeException {
        byte[] content = object.getMessage().getBytes(StandardCharsets.UTF_8);
        return new byte[][] { ERROR_BYTE, content, CRLF };
    }

    protected byte[][] encodeInteger(RedisInteger object) throws RedisEncodeException {
        byte[] content = Long.toString(object.getValue()).getBytes(StandardCharsets.UTF_8);
        return new byte[][] { INTEGER_BYTE, content, CRLF };
    }

    protected byte[][] encodeBulkString(RedisBulkString object) throws RedisEncodeException {
        byte[] lengthBytes = Integer.toString(object.getContent().length).getBytes(StandardCharsets.UTF_8);
        return new byte[][] { BULK_STRING_BYTE, lengthBytes, CRLF, object.getContent(), CRLF };
    }

    protected byte[][] encodeNull(RedisNull object) throws RedisEncodeException {
        if (RedisObject.nullBulkString().equals(object)) {
            return new byte[][] { NULL_BULK_STRING };
        } else if (RedisObject.nullArray().equals(object)) {
            return new byte[][] { NULL_ARRAY };
        } else {
            throw new RedisEncodeException("Unknown null type");
        }
    }

    protected byte[][] encodeArray(RedisArray objects) throws RedisEncodeException {
        ArrayList<byte[]> acc = new ArrayList<>();

        byte[] arraySize = Integer.toString(objects.size()).getBytes(StandardCharsets.UTF_8);
        acc.add(ARRAY_BYTE);
        acc.add(arraySize);
        acc.add(CRLF);

        for (RedisObject object : objects) {
            acc.addAll(Arrays.asList(encode(object)));
        }

        return acc.toArray(new byte[acc.size()][]);
    }

    public static class RedisEncodeException extends Exception {
        private static final long serialVersionUID = -3664942752748031528L;

        public RedisEncodeException(String message) {
            super(message);
        }
    }
}
