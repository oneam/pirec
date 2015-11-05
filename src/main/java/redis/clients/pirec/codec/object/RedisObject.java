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
package redis.clients.pirec.codec.object;

import java.nio.charset.StandardCharsets;

public abstract class RedisObject {

    public static RedisSimple simple(String value) {
        return new RedisSimple(value);
    }

    public static RedisError error(String message) {
        return new RedisError(message);
    }

    public static RedisInteger integer(long value) {
        return new RedisInteger(value);
    }

    public static RedisBulkString bulkString(byte[] content) {
        return new RedisBulkString(content);
    }

    public static RedisBulkString bulkString(String content) {
        return new RedisBulkString(content.getBytes(StandardCharsets.UTF_8));
    }

    public static RedisArray array(RedisObject... values) {
        return new RedisArray(values);
    }

    public static RedisArray array(int count) {
        return new RedisArray(count);
    }

    public static RedisNull nullBulkString() {
        return RedisNull.nullBulkString();
    }

    public static RedisNull nullArray() {
        return RedisNull.nullArray();
    }
}
