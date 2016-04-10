# _quasar-actors-integration-examples_

## ZeroMQ

Clone (or download sources from a release) and [install ZeroMQ 4.1](https://raw.githubusercontent.com/zeromq/zeromq4-1/master/INSTALL) (also available on Linux Brew) [as well as jzmq-jni](https://github.com/zeromq/jzmq/tree/master/jzmq-jni) (not available on Linux Brew as of 2016-04-10), then select your main class at the end of `build.gradle` and run `./gradlew`.

## Apache Kafka

Install and start ZooKeeper on port 2181 and Kafka on port 9092, then select your main class at the end of `build.gradle` and run `./gradlew`.

The following complete instructions are for `brew` (tried on Linux):

* Terminal 1:

```
brew install kafka
zookeeper-server-start ~/.linuxbrew/Cellar/kafka/0.9.0.0/libexec/config/zookeeper.properties
```

* Terminal 2: `kafka-server-start ~/.linuxbrew/Cellar/kafka/0.9.0.0/libexec/config/server.properties`
* Terminal 3:

```
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic quasar-actors-kafka-test-direct
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic quasar-actors-kafka-test-proxied
```

Finally select your main class at the end of `build.gradle` and run `./gradlew`.
