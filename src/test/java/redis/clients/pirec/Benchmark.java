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

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class Benchmark {

    public static void main(String[] args) throws Exception {
        int numRequests = 10000000;
        Pirec pirec = new Pirec();
        pirec.connectSync("localhost");
        AtomicLong errorCounter = new AtomicLong();
        AtomicLong successCounter = new AtomicLong();
        long[] latency = new long[numRequests];

        long start = System.nanoTime();
        Semaphore rateLimiter = new Semaphore(500);
        for (int i = 1; i <= numRequests; ++i) {
            rateLimiter.acquire();
            int latencyIndex = i - 1;
            long requestStart = System.nanoTime();
            String requestKey = Integer.toString(i);
            CompletableFuture<String> sendFuture = pirec.get(requestKey);
            sendFuture.whenComplete((responseKey, exc) -> {
                rateLimiter.release();
                long requestEnd = System.nanoTime();
                latency[latencyIndex] = requestEnd - requestStart;

                if (exc != null) {
                    exc.printStackTrace();
                    errorCounter.incrementAndGet();
                    return;
                }

                if (!responseKey.equals(requestKey) && !responseKey.equals("OK")) {
                    System.out.format("requestKey %s != responseKey %s\n", requestKey, responseKey);
                    errorCounter.incrementAndGet();
                    return;
                }

                successCounter.incrementAndGet();
            });

            if (i % 1000000 == 0) {
                System.out.format("%d requests sent\n", i);
            }
        }

        CompletableFuture<String> quitFuture = pirec.quit();
        quitFuture.get();

        long end = System.nanoTime();
        Duration duration = Duration.ofNanos(end - start);
        System.out.format("Total time: %s\n", duration);

        double rate = (double) numRequests / (double) duration.toNanos() * 1e9;
        System.out.format("Rate: %s\n", rate);

        System.out.format("Success: %s\n", successCounter.get());
        System.out.format("Error: %s\n", errorCounter.get());

        Arrays.sort(latency);
        double p50ms = (double) latency[numRequests / 2] / 1e6;
        System.out.format("P50 latency: %fms\n", p50ms);

        double p90ms = (double) latency[numRequests * 9 / 10] / 1e6;
        System.out.format("P90 latency: %fms\n", p90ms);

        double p99ms = (double) latency[numRequests * 99 / 100] / 1e6;
        System.out.format("P99 latency: %fms\n", p99ms);
    }
}
