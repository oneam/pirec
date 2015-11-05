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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RedisArray extends RedisObject implements Iterable<RedisObject> {

    final RedisObject[] values;

    public RedisArray(RedisObject[] values) {
        this.values = values;
    }

    public RedisArray(int count) {
        this.values = new RedisObject[count];
    }

    public RedisObject get(int index) {
        return values[index];
    }

    public void set(int index, RedisObject value) {
        values[index] = value;
    }

    public int size() {
        return values.length;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof RedisArray)) return false;

        RedisArray other = (RedisArray) obj;
        return Objects.deepEquals(values, other.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public Iterator<RedisObject> iterator() {
        return Arrays.asList(values).iterator();
    }

    @Override
    public String toString() {
        String contents = stream().map(Object::toString).collect(Collectors.joining(", "));
        return String.format("RedisArray[%s]", contents);
    }

    public Stream<RedisObject> stream() {
        return Stream.of(values);
    }
}
