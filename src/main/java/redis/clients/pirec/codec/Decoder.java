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
import java.util.Objects;
import java.util.function.Function;

public abstract class Decoder<T> {
    public abstract T decode(ByteBuffer buffer) throws DecoderException;

    public abstract void reset();

    public <V> Decoder<V> flatMap(Function<T, Decoder<V>> func) {
        return new Decoder<V>() {

            Decoder<V> parser = null;

            @Override
            public V decode(ByteBuffer buffer) throws DecoderException {
                if (parser != null) {
                    return parser.decode(buffer);
                }

                T input = Decoder.this.decode(buffer);
                if (Objects.nonNull(input)) {
                    parser = func.apply(input);
                    return parser.decode(buffer);
                }

                return null;
            }

            @Override
            public void reset() {
                parser = null;
                Decoder.this.reset();
            }
        };
    }

    public <V> Decoder<V> map(Function<T, V> func) {
        return flatMap(value -> just(func.apply(value)));
    }

    static public Decoder<ByteBuffer> delimited(byte[] delimiter, int maxLength) {
        return new Decoder<ByteBuffer>() {

            @Override
            public ByteBuffer decode(ByteBuffer buffer) throws DecoderException {
                int delimiterIndex = 0;
                buffer.mark();
                int length = 0;
                while (buffer.hasRemaining() && length <= maxLength) {
                    ++length;
                    byte nextByte = buffer.get();
                    if (nextByte != delimiter[delimiterIndex]) {
                        delimiterIndex = 0;
                        continue;
                    }

                    ++delimiterIndex;

                    if (delimiterIndex >= delimiter.length) {
                        ByteBuffer output = buffer.duplicate();
                        output.limit(output.position());
                        output.reset();
                        return output;
                    }
                }

                buffer.reset();
                if (length > maxLength) {
                    throw new DecoderException("Message length too long");
                }
                return null;
            }

            @Override
            public void reset() {
            }
        };
    }

    static public Decoder<ByteBuffer> fixedLength(int length) {
        return new Decoder<ByteBuffer>() {

            @Override
            public ByteBuffer decode(ByteBuffer buffer) {
                if (buffer.remaining() >= length) {
                    int limit = buffer.position() + length;
                    ByteBuffer output = buffer.duplicate();
                    output.limit(limit);
                    buffer.position(limit);
                    return output;
                }

                return null;
            }

            @Override
            public void reset() {
            }
        };
    }

    static public <T> Decoder<T> just(T value) {
        return new Decoder<T>() {

            @Override
            public T decode(ByteBuffer buffer) {
                return value;
            }

            @Override
            public void reset() {
            }
        };
    }

    static public <T> Decoder<T> error(DecoderException exc) {
        return new Decoder<T>() {

            @Override
            public T decode(ByteBuffer buffer) throws DecoderException {
                throw exc;
            }

            @Override
            public void reset() {
            }
        };
    }

    public static class DecoderException extends Exception {
        private static final long serialVersionUID = -1134860992982045264L;

        public DecoderException(String message) {
            super(message);
        }

        public DecoderException(String message, Throwable cause) {
            super(message, cause);
        }

        public DecoderException(Throwable cause) {
            super(cause);
        }
    }
}
