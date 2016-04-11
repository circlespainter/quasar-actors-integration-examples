# _quasar-actors-integration-examples_

## ZeroMQ

Clone (or download sources from a release) and [install ZeroMQ 4.1](https://raw.githubusercontent.com/zeromq/zeromq4-1/master/INSTALL) (also available on Linux Brew) [as well as jzmq-jni](https://github.com/zeromq/jzmq/tree/master/jzmq-jni) (not available on Linux Brew as of 2016-04-10), then select your main class at the end of `build.gradle` and run `./gradlew`.

## Apache Kafka

[Download and uncompress the Kafka 0.9.0.1 distro](http://kafka.apache.org/downloads.html) and start ZooKeeper on port 2181 and then Kafka on port 9092 using two different terminals:

```
bin/zookeeper-server-start.sh config/zookeeper.properties
```

```
bin/kafka-server-start.sh config/server.properties
```

Then in another terminal create your topics like so:

```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic kafkadirect
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic kafkaproxied
```

Finally select your main class at the end of `build.gradle` and run `./gradlew`.

You can use `. cleanup-kafka.sh` to remove all ZooKeeper and Kafka data files if you want to start from a clean situation.