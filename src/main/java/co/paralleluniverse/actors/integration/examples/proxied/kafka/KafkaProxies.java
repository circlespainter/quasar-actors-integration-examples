package co.paralleluniverse.actors.integration.examples.proxied.kafka;

import co.paralleluniverse.actors.Actor;
import co.paralleluniverse.actors.ActorRef;
import co.paralleluniverse.actors.ActorSpec;
import co.paralleluniverse.actors.BasicActor;
import co.paralleluniverse.fibers.FiberUtil;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.kafka.FiberKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static co.paralleluniverse.actors.integration.examples.Util.call;

@SuppressWarnings("WeakerAccess")
public final class KafkaProxies implements AutoCloseable {
    /**
     * Creates a new kafka proxy actors factory specific to a cluster and a topic.
     *
     * @param bootstrap The Kafka bootstrap server(s)
     * @param topic     The Kafka topic
     */
    public KafkaProxies(String bootstrap, String topic) {
        this(bootstrap, topic, null, null);
    }

    /**
     * Creates a new kafka proxy actors factory specific to a cluster and a topic.
     *
     * @param bootstrap                       The Kafka bootstrap server(s)
     * @param topic                           The Kafka topic
     * @param kafkaProducerPropertiesOverride Overrides Kafka producer properties (can be {@code null})
     * @param kafkaConsumerPropertiesOverride Overrides Kafka consumer properties (can be {@code null})
     */
    public KafkaProxies(String bootstrap, String topic, Properties kafkaProducerPropertiesOverride, Properties kafkaConsumerPropertiesOverride) {
        this.topic = topic;
        producer = buildProducer(bootstrap, kafkaProducerPropertiesOverride);
        startConsumers(bootstrap, kafkaConsumerPropertiesOverride);
    }

    /**
     * Creates and returns a new Kafka proxy actor.
     *
     * @param actorID  The ID of the target actor
     * @param <M>      The base type of the message (must be serializable by Kryo)
     */
    public final <M> ActorRef<M> create(String actorID) {
        ensureOpen();

        final ActorRef<M> a;
        try {
            //noinspection unchecked
            a = Actor.newActor (
                new ActorSpec<>(
                    ProducerActor.class.getConstructor(KafkaProxies.class, String.class),
                    new Object[] { this, actorID }
                )
            ).spawn();
        } catch (final NoSuchMethodException e) {
            throw new AssertionError(e);
        }
        tos.add(a);
        return a;
    }

    /**
     * Releases a Kafka proxy actor.
     *
     * @param ref The proxy actor to release
     */
    @SuppressWarnings("unused")
    public final void drop(ActorRef ref) throws ExecutionException, InterruptedException {
        ensureOpen();

        //noinspection unchecked
        FiberUtil.runInFiber(() -> ref.send(EXIT));
        tos.remove(ref);
    }

    /**
     * Subscribes a Kafka consumer actor.
     *
     * @param consumer The actor that will receive Kafka incoming messages
     * @param actorID  The actor's remote ID
     * @param <M>      The message type
     */
    public final <M> void subscribe(ActorRef<? super M> consumer, String actorID) {
        ensureOpen();
        subscribers.compute(actorID, (s, actorRefs) -> {
            final List<ActorRef> l = actorRefs != null ? actorRefs : new ArrayList<>();
            l.add(consumer);
            return l;
        });
    }

    /**
     * Unsubscribes a Kafka consumer actor.
     *
     * @param consumer The actor that will receive Kafka incoming messages
     * @param actorID The actor's remote ID
     */
    @SuppressWarnings("unused")
    public final void unsubscribe(ActorRef<?> consumer, String actorID) {
        ensureOpen();
        subscribers.compute(actorID, (s, actorRefs) -> {
            final List<ActorRef> l = actorRefs != null ? actorRefs : new ArrayList<>();
            l.remove(consumer);
            return l;
        });
    }

    /**
     * Closes all the actors and Kafka objects produced by this factory.
     *
     * @throws Exception
     */
    @Override
    public final void close() throws Exception {
        if (!closed.compareAndSet(false, true))
            throw new IllegalStateException("closed");

        try {
            FiberUtil.runInFiber(() -> {
                for (final ActorRef c : consumers)
                    //noinspection unchecked
                    c.send(EXIT);
                for (final ActorRef c : tos)
                    //noinspection unchecked
                    c.send(EXIT);
                consumers.clear();
                tos.clear();
            });
        } finally {
            producer.close();
            es.shutdown();
        }
    }

    public final class ProducerActor<M> extends BasicActor<M, Void> {
        private final String actorID;

        public ProducerActor(String actorID) {
            this.actorID = actorID;
        }

        @Override
        protected final Void doRun() throws InterruptedException, SuspendExecution {
            //noinspection InfiniteLoopStatement
            for(;;) {
                final M m = receive();
                if (EXIT.equals(m))
                    return null;

                try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                     final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                    oos.writeObject(new ProxiedMsg(actorID, m));
                    oos.flush();
                    baos.flush();
                    producer.send(new ProducerRecord<>(topic, null, baos.toByteArray()));
                    producer.flush();
                } catch (final IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public final class ConsumerActor extends BasicActor<Object, Void> {
        @Override
        protected Void doRun() throws InterruptedException, SuspendExecution {
            //noinspection InfiniteLoopStatement
            for (;;) {
                // Try extracting from queue
                final Object msg = tryReceive((Object m) -> {
                    if (EXIT.equals(m))
                        return EXIT;
                    if (m != null) {
                        //noinspection unchecked
                        final ProxiedMsg rmsg = (ProxiedMsg) m;
                        final List<ActorRef> l = subscribers.get(rmsg.actorID);
                        if (l != null && l.size() > 0) // There are subscribers
                            return m;
                    }
                    return null; // No subscribers (leave in queue) or no messages
                });
                // Something processable is there
                if (msg != null) {
                    if (EXIT.equals(msg)) {
                        return null;
                    }
                    //noinspection unchecked
                    final ProxiedMsg rmsg = (ProxiedMsg) msg;
                    final List<ActorRef> l = subscribers.get(rmsg.actorID);
                    for (final ActorRef r : l) {
                        //noinspection unchecked
                        r.send(rmsg.payload);
                    }
                    continue; // Go to next cycle -> precedence to queue
                }

                // Try receiving
                //noinspection Convert2Lambda
                final ConsumerRecords<Void, byte[]> records = call(es, () ->
                    consumer.get().poll(100L)
                );
                for (final ConsumerRecord<Void, byte[]> record : records) {
                    final byte[] v = record.value();
                    try (final ByteArrayInputStream bis = new ByteArrayInputStream(v);
                         final ObjectInputStream ois = new ObjectInputStream(bis)) {

                        //noinspection unchecked
                        final ProxiedMsg rmsg = (ProxiedMsg) ois.readObject();
                        final List<ActorRef> l = subscribers.get(rmsg.actorID);
                        if (l != null && l.size() > 0) {
                            for (final ActorRef r : l) {
                                //noinspection unchecked
                                r.send(rmsg.payload);
                            }
                        } else {
                            ref().send(rmsg); // Enqueue
                        }
                    } catch (final IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private static final Object EXIT = new Object();

    private final static Properties baseProducerConfig;
    static {
        baseProducerConfig = new Properties();
        baseProducerConfig.put("acks", "all");
        baseProducerConfig.put("retries", 10);
        baseProducerConfig.put("batch.size", 1);
        baseProducerConfig.put("linger.ms", 1);
    }
    private final static Properties baseConsumerConfig;
    static {
        baseConsumerConfig = new Properties();
        baseConsumerConfig.put("enable.auto.commit", "true");
        baseConsumerConfig.put("auto.commit.interval.ms", "10");
        baseConsumerConfig.put("session.timeout.ms", "30000");
    }

    private final Map<String, List<ActorRef>> subscribers = new ConcurrentHashMap<>();
    private final List<ActorRef> tos = new ArrayList<>();
    private final List<ActorRef> consumers = new ArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final String topic;
    private final FiberKafkaProducer<Void, byte[]> producer;

    private ThreadLocal<KafkaConsumer<Void, byte[]>> consumer;
    private ExecutorService es;

    private static final class ProxiedMsg implements Serializable {
        private static final long serialVersionUID = 0L;

        public String actorID;
        public Object payload;

        @SuppressWarnings("unused")
        public ProxiedMsg() {} // For serialization
        public ProxiedMsg(String actorID, Object payload) {
            this.actorID = actorID;
            this.payload = payload;
        }
    }

    private void ensureOpen() {
        if (closed.get())
            throw new IllegalStateException("closed");
    }

    private FiberKafkaProducer<Void, byte[]> buildProducer(String bootstrap, Properties kafkaProducerPropertiesOverride) {
        final Properties p = new Properties(baseProducerConfig);
        if (kafkaProducerPropertiesOverride != null)
            p.putAll(kafkaProducerPropertiesOverride);
        if (bootstrap != null)
            p.put("bootstrap.servers", bootstrap);
        p.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new FiberKafkaProducer<>(new KafkaProducer<>(p));
    }

    private void startConsumers(String bootstrap, Properties kafkaConsumerPropertiesOverride) {
        final Properties p = new Properties(baseConsumerConfig);
        if (kafkaConsumerPropertiesOverride != null)
            p.putAll(kafkaConsumerPropertiesOverride);
        if (bootstrap != null)
            p.put("bootstrap.servers", bootstrap);
        p.put("group.id", "group");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        final KafkaConsumer<Void, byte[]> tmp = new KafkaConsumer<>(p);
        tmp.subscribe(Collections.singletonList(topic));
        int partitions = tmp.partitionsFor(topic).size();
        tmp.close();

        es = Executors.newFixedThreadPool(partitions);
        consumer = new ThreadLocal<KafkaConsumer<Void, byte[]>>() {
            @Override
            protected KafkaConsumer<Void, byte[]> initialValue() {
                final KafkaConsumer<Void, byte[]> r = new KafkaConsumer<>(p);
                r.subscribe(Collections.singletonList(topic));
                return r;
            }
        };

        for (int i = 0 ; i < partitions ; i++) {
            try {
                consumers.add (
                    Actor.newActor (
                        new ActorSpec<> (
                            ConsumerActor.class.getConstructor(KafkaProxies.class),
                            new Object[] { this }
                        )
                    ).spawn()
                );
            } catch (final NoSuchMethodException e) {
                throw new AssertionError(e);
            }
        }
    }
}
