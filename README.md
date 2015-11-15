Pirec
=====

A fully pipelined Redis client for high-throughput operation.

[![Build Status](https://travis-ci.org/oneam/pirec.svg)](https://travis-ci.org/oneam/pirec) [![Coverage Status](https://coveralls.io/repos/oneam/pirec/badge.svg?branch=master&service=github)](https://coveralls.io/github/oneam/pirec?branch=master)

Why Another Redis client?
-------------------------

Redis has this cool feature called [Pipelining](http://redis.io/topics/pipelining) that allows multiple requests to be sent without waiting for each reply. The RedisLab documentation claims that you can improve your throughput by a factor of 5 by using pipelining. However, in order to use the feature in most clients you are forced to batch together all of the requests you want to send and wait for all of the responses.

Pirec takes a different approach. _Every_ command is sent through a Pirec client is pipelined. As a result, the throughput can increase by a factor of 5-10x for _all_ requests (especially under high load).

There's a Benchmark class included so that you can see for yourself. I've demonstrated a throughput of 400000 TPS for random GET and SET operations with no latency degradation.


Building the cient
------------------

Pirec is built using Gradle. You can use the included gradlew scripts to build the library:

```
./gradlew jar
```

Pirec.jar will end up in the build/jar directory.

Using the client
-----------------

All commands in Pirec are asynchronous. The library uses `CompletableFuture` to keep track of the requests in flight.

Example:

```
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
```

Multithreading
--------------

If you don't like using `CompletableFuture.whenComplete(...)` to handle responses, you can still use the performance advantages of Pirec. Simply share the Pirec client amongst multiple threads and use the `CompletableFuture.get()` method to make your requests blocking.

Here's an example:

```
import java.util.concurrent.ExecutionException;
import redis.clients.pirec.Pirec;


public class MultithreadedExample {
    public static void main(String[] args) throws Exception {

        Pirec pirec = new Pirec();
        pirec.connectSync("localhost"); // Connect to the server

        Runnable getSetLoop = () -> {
            try {
                while (true) {
                    pirec.set("myKey", "myValue").get(); // SET, then GET myKey
                    String myValue = pirec.get("myKey").get();
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

        System.in.read(); // Keep looping until someone hits enter
    }
}
```

Even though each thread uses synchronous commands, the Pirec library will pipeline requests from multiple threads. In this case, up to 100 requests can be sent over a single connection.

What's on the Roadmap
---------------------

I'm not finished with Pirec. Here's a shortlist of what I still want to do:

* Complete implementing the Redis Cluster Commands: http://redis.io/commands#cluster
* Implement the Redis Server Commands: http://redis.io/commands#server
* Implement the Redis Geo Commands: http://redis.io/commands#geo
* Add Pirec to Maven
* Create a PirecSubscriber client for using Redis subscribed state: http://redis.io/commands/subscribe
* Improve the socket model to reduce the unnecessary time spent reading
