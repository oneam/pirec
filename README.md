Pirec
=====

A fully pipelined Redis client for high-throughput operation.

[![Build Status](https://travis-ci.org/oneam/pirec.svg)](https://travis-ci.org/oneam/pirec) [![codecov.io](https://codecov.io/github/oneam/pirec/coverage.svg?branch=master)](https://codecov.io/github/oneam/pirec?branch=master)

Why Another Redis client?
-------------------------

Redis has this cool feature called [Pipelining](http://redis.io/topics/pipelining) that allows multiple requests to be sent without waiting for each reply. The RedisLab documentation claims that you can improve your throughput by a factor of 5 by using pipelining. However, in order to use the feature in most clients you are forced to batch together all of the requests you want to send and wait for all of the responses.

Pirec uses a combination of features built around pipelining to improve overall performance. There's a Benchmark class included so that you can see for yourself. I've demonstrated a throughput of 400000 TPS for random GET and SET operations with no latency degradation.

Pirec uses a combination of 3 features to improve performance:

### Asynchronous Commands

In order to allow multiple commands to be sent at once, it's helpful to separate sending requests and receiving responses. All request methods on the Pirec client return a CompletableFuture response which represents the value of the response once it arrives.

CompletableFuture was chosen as it also provides the ability to be composed with other futures into full asynchronous workflows.

### Multiplexing

In addition to using asynchronous commands, the Pirec client is fully thread-safe. In fact, using multiple threads is another way to allow multiple outstanding commands. It's a recommended practice for multiple threads to share the same instance of the Pirec client. When requests are sent from multiple threads, the requests can be processed concurrently, thus improving overall performance.

### Batching

Bandwidth and latency are often stated as performance metrics in networking systems. Redis is often used as a tool in localhost and datacenter environments where bandwidth is high and latency is low. In these scenarios a third bottleneck appears, number of packets. Pirec batches commands together into a single call to socket.write(...) in order to help the JVM make better use of the available throughput.

Note, this batching is done in a way that still keeps latency low. Pirec will send the first command immediately, and only batch commands when the rate of socket.write(...) is les than the rate of command creation.

Building Pirec
--------------

Pirec is built using Gradle. You can use the included gradlew scripts to build the library:

```
./gradlew jar
```

Pirec.jar will end up in the build/jar directory.

Including Pirec in your project
-------------------------------

### Gradle

Add the following to your build.gradle

```
repositories {
    maven {
        url  "http://dl.bintray.com/oneam/maven" 
    }
}

dependencies {
    compile 'redis.clients.pirec:pirec:0.1-Beta'
}
```

### Everything Else

Go to https://bintray.com/package/buildSettings?pkgPath=%2Foneam%2Fmaven%2FPirec for details

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
