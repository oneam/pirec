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

import java.util.Arrays;
import java.util.Objects;

public class ScanResult<T> {
    final String cursor;
    final T[] values;

    public ScanResult(String cursor, T[] values) {
        this.cursor = cursor;
        this.values = values;
    }

    public String getCursor() {
        return cursor;
    }

    public T[] getValues() {
        return values;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof ScanResult<?>)) return false;

        ScanResult<?> other = (ScanResult<?>) obj;
        String otherCursor = other.getCursor();
        Object[] otherValues = other.getValues();
        return Objects.equals(cursor, otherCursor) &&
                Arrays.deepEquals(values, otherValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cursor, values);
    }
}
