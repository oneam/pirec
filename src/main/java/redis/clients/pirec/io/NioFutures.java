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

import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;

public class NioFutures {
    private NioFutures() {
        throw new UnsupportedOperationException();
    }

    private static class NioFutureHandler<R> implements CompletionHandler<R, CompletableFuture<R>> {

        @Override
        public void completed(R result, CompletableFuture<R> future) {
            future.complete(result);
        }

        @Override
        public void failed(Throwable exc, CompletableFuture<R> future) {
            future.completeExceptionally(exc);
        }
    }

    public static <R> CompletableFuture<R> get(NioSupplier<R> nioAction) {
        CompletableFuture<R> future = new CompletableFuture<R>();
        nioAction.get(future, new NioFutureHandler<R>());
        return future;
    }

    public interface NioSupplier<R> {
        void get(CompletableFuture<R> future, CompletionHandler<R, CompletableFuture<R>> handler);
    }

    public static <T1, R> CompletableFuture<R> apply(NioFunction<T1, R> nioAction, T1 param1) {
        CompletableFuture<R> future = new CompletableFuture<R>();
        nioAction.apply(param1, future, new NioFutureHandler<R>());
        return future;
    }

    public interface NioFunction<T, R> {
        void apply(T t, CompletableFuture<R> future, CompletionHandler<R, CompletableFuture<R>> handler);
    }

    public static <R> CompletableFuture<R> completedExceptionally(Throwable ex) {
        CompletableFuture<R> future = new CompletableFuture<R>();
        future.completeExceptionally(ex);
        return future;
    }
}
