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
package redis.clients.pirec.io;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;

public class NioFutures {
    private NioFutures() {
        throw new UnsupportedOperationException();
    }

    private static class NioAsyncHandler<R> implements CompletionHandler<R, CompletableFuture<R>> {

        @Override
        public void completed(R result, CompletableFuture<R> future) {
            future.complete(result);
        }

        @Override
        public void failed(Throwable exc, CompletableFuture<R> future) {
            future.completeExceptionally(exc);
        }
    }

    public static <R> CompletableFuture<R> get(NioAsyncSupplier<R> nioAction) {
        CompletableFuture<R> future = new CompletableFuture<R>();
        nioAction.get(future, new NioAsyncHandler<R>());
        return future;
    }

    public interface NioAsyncSupplier<R> {
        void get(CompletableFuture<R> future, CompletionHandler<R, CompletableFuture<R>> handler);
    }

    public static <T1, R> CompletableFuture<R> apply(NioAsyncFunction<T1, R> nioAction, T1 param1) {
        CompletableFuture<R> future = new CompletableFuture<R>();
        nioAction.apply(param1, future, new NioAsyncHandler<R>());
        return future;
    }

    public interface NioAsyncFunction<T, R> {
        void apply(T t, CompletableFuture<R> future, CompletionHandler<R, CompletableFuture<R>> handler);
    }

    public static <T1, R> CompletableFuture<R> applyAsync(NioFunction<T1, R> nioAction, T1 param1) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return nioAction.apply(param1);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public interface NioFunction<T, R> {
        R apply(T t) throws IOException;
    }

    public static <T> CompletableFuture<Void> acceptAsync(NioConsumer<T> nioAction, T param) {
        return CompletableFuture.runAsync(() -> {
            try {
                nioAction.accept(param);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public interface NioConsumer<T> {
        void accept(T t) throws IOException;
    }

    public static <R> CompletableFuture<R> completedExceptionally(Throwable ex) {
        CompletableFuture<R> future = new CompletableFuture<R>();
        future.completeExceptionally(ex);
        return future;
    }
}
