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
import java.util.Arrays;
import java.util.Objects;

public class RedisBulkString extends RedisObject {

    final byte[] content;

    public RedisBulkString(byte[] content) {
        this.content = content;
    }

    public byte[] getContent() {
        return content;
    }

    public String getContentAsString() {
        return new String(content, StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof RedisBulkString)) return false;

        RedisBulkString other = (RedisBulkString) obj;
        return Objects.deepEquals(getContent(), other.getContent());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(content);
    }

    @Override
    public String toString() {
        if (content.length < 256) {
            return String.format("RedisBulkString(\"%s\")", new String(content, StandardCharsets.UTF_8));
        } else {
            return String.format("RedisBulkString(%d bytes)", content.length);
        }
    }
}
