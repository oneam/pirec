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

import redis.clients.pirec.codec.Decoder.DecoderException;
import redis.clients.pirec.codec.object.RedisArray;
import redis.clients.pirec.codec.object.RedisObject;

public class RedisDecoder {
    static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);
    static final byte SIMPLE_STRING_BYTE = "+".getBytes(StandardCharsets.UTF_8)[0];
    static final byte ERROR_BYTE = "-".getBytes(StandardCharsets.UTF_8)[0];
    static final byte ARRAY_BYTE = "*".getBytes(StandardCharsets.UTF_8)[0];
    static final byte BULK_STRING_BYTE = "$".getBytes(StandardCharsets.UTF_8)[0];
    static final byte INTEGER_BYTE = ":".getBytes(StandardCharsets.UTF_8)[0];

    final Decoder<RedisObject> decoder;

    public RedisDecoder() {
        this.decoder = newDecoder();
    }

    public RedisObject decode(ByteBuffer buffer) throws DecoderException {
        RedisObject object = decoder.decode(buffer);
        if (object != null) {
            decoder.reset();
        }
        return object;
    }

    static Decoder<RedisObject> newDecoder() {
        return Decoder.delimited(CRLF, 4096).flatMap(RedisDecoder::newObjectDecoder);
    }

    static Decoder<RedisObject> newObjectDecoder(ByteBuffer firstLine) {
        byte typeByte = firstLine.get();
        if (typeByte == SIMPLE_STRING_BYTE) {
            return Decoder.just(RedisObject.simple(parseString(firstLine)));
        } else if (typeByte == ERROR_BYTE) {
            return Decoder.just(RedisObject.error(parseString(firstLine)));
        } else if (typeByte == ARRAY_BYTE) {
            int objectCount = parseInt(firstLine);
            if (objectCount < 0) {
                return Decoder.just(RedisObject.nullArray());
            }

            Decoder<RedisObject> parser = Decoder.just(RedisObject.array(objectCount));
            for (int i = 0; i < objectCount; ++i) {
                int index = i;
                parser = parser.flatMap(object -> RedisDecoder.newDecoder().map(value -> {
                    RedisArray array = (RedisArray) object;
                    array.set(index, value);
                    return array;
                }));
            }
            return parser;
        } else if (typeByte == BULK_STRING_BYTE) {
            int byteCount = parseInt(firstLine);
            if (byteCount < 0) {
                return Decoder.just(RedisObject.nullBulkString());
            }

            return Decoder.fixedLength(byteCount + 2).flatMap(byteBuffer -> {
                byte[] content = new byte[byteCount];
                byteBuffer.get(content);
                for (int i = 0; i < CRLF.length; ++i) {
                    byte b = byteBuffer.get();
                    if (b != CRLF[i]) {
                        return Decoder.error(new Decoder.DecoderException("Bulk String does not end with \r\n"));
                    }
                }
                return Decoder.just(RedisObject.bulkString(content));
            });
        } else if (typeByte == INTEGER_BYTE) {
            return Decoder.just(RedisObject.integer(parseLong(firstLine)));
        } else {
            return Decoder.error(new Decoder.DecoderException(String.format(
                    "Invalid type byte 0x%x '%c'",
                    typeByte,
                    (char) typeByte)));
        }
    }

    static int parseInt(ByteBuffer buffer) {
        return Integer.parseInt(parseString(buffer));
    }

    static long parseLong(ByteBuffer buffer) {
        return Long.parseLong(parseString(buffer));
    }

    static String parseString(ByteBuffer buffer) {
        int offset = buffer.arrayOffset() + buffer.position();
        int length = buffer.remaining() - 2;
        return new String(buffer.array(), offset, length, StandardCharsets.UTF_8);
    }
}
