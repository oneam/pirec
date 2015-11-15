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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import redis.clients.pirec.Pirec;

public class MultithreadedExample {

    public static void main(String[] args) throws Exception {
        Pirec pirec = new Pirec();
        pirec.connectSync("localhost"); // Connect to the server

        AtomicLong counter = new AtomicLong();
        Runnable getSetLoop = () -> {
            try {
                while (true) {
                    pirec.set("myKey", "myValue").get(); // SET, then GET myKey
                    pirec.get("myKey").get();

                    long counterValue = counter.incrementAndGet(); // Update the overall counter
                    if (counterValue % 10000 == 0) {
                        System.out.format("Completed %d loops\n", counterValue);
                    }
                }
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        };

        int numThreads = 100;
        for (int i = 0; i < numThreads; ++i) {
            Thread thread = new Thread(getSetLoop); // Start each thread
            thread.start();
        }

        System.out.println("Press enter to quit...");
        System.in.read();
    }

}
