# _quasar-actors-integration-examples_

## ZeroMQ

Clone (or download sources from a release) and [install ZeroMQ 4.1](https://raw.githubusercontent.com/zeromq/zeromq4-1/master/INSTALL) (also available on Linux Brew) [as well as jzmq-jni](https://github.com/zeromq/jzmq/tree/master/jzmq-jni) (not available on Linux Brew as of 2016-04-10), then select your main class at the end of `build.gradle` and run `./gradlew`.

## Apache Kafka

[Download and uncompress the Kafka 0.9.0.1 distro](http://kafka.apache.org/downloads.html) and start ZooKeeper on port 2181 and Kafka on port 9092, then select your main class at the end of `build.gradle` and run `./gradlew`.
