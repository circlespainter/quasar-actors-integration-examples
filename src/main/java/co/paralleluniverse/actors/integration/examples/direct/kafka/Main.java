package co.paralleluniverse.actors.integration.examples.direct.kafka;

import co.paralleluniverse.actors.Actor;
import co.paralleluniverse.actors.MailboxConfig;
import co.paralleluniverse.actors.integration.examples.Msg;
import co.paralleluniverse.common.util.CheckedCallable;
import co.paralleluniverse.fibers.FiberAsync;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.kafka.FiberKafkaProducer;
import com.google.common.base.Charsets;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static co.paralleluniverse.actors.integration.examples.Util.*;

public final class Main {
    private static final ExecutorService e = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try {
            final ProducerActor pa = Actor.newActor(ProducerActor.class);
            final ConsumerActor ca = Actor.newActor(ConsumerActor.class);
            pa.spawn();
            System.err.println("Producer started");
            ca.spawn();
            System.err.println("Consumer started");
            pa.join();
            System.err.println("Producer finished");
            ca.join();
            System.err.println("Consumer finished");
        } finally {
            e.shutdown();
        }
    }

    private static final MailboxConfig DEFAULT_MBC = new MailboxConfig();
    private static final String BOOTSTRAP = "localhost:9092";
    private static final String TOPIC = "kafkadirect";

    private final static Properties producerConfig;

    static {
        producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", BOOTSTRAP);
        producerConfig.put("client.id", "DemoProducer");
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    }

    private static final class ProducerActor extends Actor<Object, Void> {
        public ProducerActor() {
            super("producer", DEFAULT_MBC);
        }

        @Override
        protected final Void doRun() throws InterruptedException, SuspendExecution {
            try (final FiberKafkaProducer<Integer, byte[]> producer = new FiberKafkaProducer<>(new KafkaProducer<>(producerConfig))) {
                //noinspection InfiniteLoopStatement
                for (int i = 0; i < MSGS ; i++) {
                    final Msg m = new Msg(i);
                    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024) ;
                         final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                        oos.writeObject(m);
                        oos.flush();
                        baos.flush();
                        System.err.printf("PRODUCER: topic = %s, key = %d, value = %s\n", TOPIC, i, m);
                        producer.send(new ProducerRecord<>(TOPIC, i, baos.toByteArray()));
                        producer.flush();
                    } catch (final IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
                System.err.println("PRODUCER: sending EXIT");
                producer.send(new ProducerRecord<>(TOPIC, null, "exit".getBytes(Charsets.US_ASCII)));
                System.err.println("PRODUCER: exiting");
                return null;
            }
        }
    }

    private final static Properties consumerConfig;
    static {
        consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerConfig.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consumerConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    }

    private static final class ConsumerActor extends Actor<Object, Void> {
        public ConsumerActor() {
            super("consumer", DEFAULT_MBC);
        }

        @Override
        protected final Void doRun() throws InterruptedException, SuspendExecution {
            try (final Consumer<Integer, byte[]> consumer = new KafkaConsumer<>(consumerConfig)) {
                consumer.subscribe(Collections.singletonList(TOPIC));
                for (;;) {
                    System.err.println("CONSUMER: polling");
                    final ConsumerRecords<Integer, byte[]> records = call(e, () -> consumer.poll(1000L));
                    System.err.println("CONSUMER: received");
                    for (final ConsumerRecord<Integer, byte[]> record : records) {
                        final byte[] v = record.value();
                        if (Arrays.equals("exit".getBytes(Charsets.US_ASCII), v)) {
                            System.err.printf("CONSUMER: exiting\n");
                            return null;
                        }

                        try (final ByteArrayInputStream bis = new ByteArrayInputStream(v);
                             final ObjectInputStream ois = new ObjectInputStream(bis)) {
                            System.err.printf("CONSUMER: topic = %s, offset = %d, key = %d, value = %s\n", record.topic(), record.offset(), record.key(), ois.readObject());
                        } catch (final IOException | ClassNotFoundException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }

    private Main() {}
}
