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

import java.util.concurrent.CompletableFuture;

import redis.clients.pirec.Pirec;

public class SimpleExample {

    public static void main(String[] args) throws Exception {
        Pirec pirec = new Pirec();

        pirec.connectSync("localhost"); // Connect to the server

        CompletableFuture<String> requestFuture = pirec // Send a GET request
                .get("myKey")
                .whenComplete((myValue, exc) -> {
                    System.out.format("The value of myKey is %s", myValue); // Print the result
                    });

        requestFuture.get(); // Wait for the GET request to complete
    }

}
