package co.paralleluniverse.actors.integration.examples.proxied.zeromq;

import co.paralleluniverse.actors.*;
import co.paralleluniverse.actors.integration.examples.Util;
import co.paralleluniverse.fibers.FiberUtil;
import co.paralleluniverse.fibers.SuspendExecution;
import org.zeromq.ZMQ;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("WeakerAccess")
public final class ZeroMQProxies implements AutoCloseable {
    private final ZMQ.Context zmq;

    /**
     * Creates a new ZeroMQ proxy actors factory.
     *
     * @param ioThreads The number of IO threads to be used.
     */
    public ZeroMQProxies(int ioThreads) {
        zmq = ZMQ.context(ioThreads);
    }

    /**
     * Creates and returns a new ZeroMQ proxy actor.
     *
     * @param trgtZMQAddress The ZeroMQ address of the target actor.
     */
    public final <M> ActorRef<M> to(String trgtZMQAddress) {
        if (trgtZMQAddress == null)
            throw new IllegalArgumentException("`trgtZMQAddress` must be non-null");

        ensureOpen();

        //noinspection UnnecessaryLocalVariable
        final ActorRef a = producerProxies
            .computeIfAbsent(trgtZMQAddress, (k) -> {
                try {
                    //noinspection unchecked
                    return Actor.newActor (
                        new ActorSpec<> (
                            ProducerActor.class.getConstructor(ZeroMQProxies.class, String.class),
                            new Object[] { this, trgtZMQAddress }
                        )
                    ).spawn();
                } catch (final NoSuchMethodException e) {
                    throw new AssertionError(e);
                }
            });

        //noinspection unchecked
        return a;
    }

    /**
     * Subscribes a consumer actor to a ZeroMQ endpoint.
     *
     * @param consumer The consumer actor.
     * @param srcZMQEndpoint The ZeroMQ endpoint.
     */
    public final <M> void subscribe(ActorRef<? super M> consumer, String srcZMQEndpoint) {
        if (consumer == null || srcZMQEndpoint == null)
            throw new IllegalArgumentException("`consumer` and `srcZMQEndpoint` must be non-null");

        ensureOpen();

        producerProxies
            .computeIfAbsent(srcZMQEndpoint, (k) -> {
                try {
                    //noinspection unchecked
                    return Actor.newActor (
                        new ActorSpec<> (
                            ConsumerActor.class.getConstructor(ZeroMQProxies.class, String.class),
                            new Object[] { this, srcZMQEndpoint }
                        )
                    ).spawn();
                } catch (final NoSuchMethodException e) {
                    throw new AssertionError(e);
                }
            });

        subscribers.computeIfAbsent(srcZMQEndpoint, (k) -> new ArrayList<>()).add(consumer);
    }

    /**
     * Unsubscribes a consumer actor from a ZeroMQ endpoint.
     *
     * @param consumer The consumer actor.
     * @param srcZMQEndpoint The ZeroMQ endpoint.
     */
    @SuppressWarnings("unused")
    public final void unsubscribe(ActorRef<?> consumer, String srcZMQEndpoint) {
        if (srcZMQEndpoint == null)
            throw new IllegalArgumentException("`srcZMQEndpoint` must be non-null");

        ensureOpen();

        subscribers.compute(srcZMQEndpoint, (s, actorRefs) -> {
            if (actorRefs != null)
                actorRefs.remove(consumer);
            return actorRefs;
        });
    }

    /**
     * Closes all the actors and ZeroMQ objects produced by this factory.
     *
     * @throws Exception
     */
    @Override
    public final void close() throws Exception {
        if (!closed.compareAndSet(false, true))
            throw new IllegalStateException("closed");

        try {
            FiberUtil.runInFiber(() -> {
                for (final ActorRef a : producerProxies.values())
                    //noinspection unchecked
                    a.send(EXIT);
                producerProxies.clear();
                for (final ActorRef a : consumerProxies.values())
                    //noinspection unchecked
                    a.send(EXIT);
                consumerProxies.clear();
                subscribers.clear();
            });
        } finally {
            e.shutdown();
        }
    }

    public final class ProducerActor extends BasicActor<Object, Void> {
        private final String trgtZMQAddress;

        public ProducerActor(String trgtZMQAddress) {
            this.trgtZMQAddress = trgtZMQAddress;
        }

        @Override
        protected final Void doRun() throws InterruptedException, SuspendExecution {
            try (final ZMQ.Socket trgt = zmq.socket(ZMQ.REQ)) {
                System.err.printf("PROXY PRODUCER: connecting to %s\n", trgtZMQAddress);
                Util.exec(e, () -> trgt.connect(trgtZMQAddress));
                //noinspection InfiniteLoopStatement
                for (;;) {
                    System.err.println("PROXY PRODUCER: receiving from the mailbox");
                    final Object m = receive();
                    if (m == null || EXIT.equals(m)) {
                        System.err.println("PROXY PRODUCER: exiting");
                        return null;
                    } else {
                        System.err.printf("PROXY PRODUCER: forwarding %s\n", m);
                        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                             final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                            oos.writeObject(m);
                            oos.flush();
                            baos.flush();
                            Util.exec(e, () -> trgt.send(baos.toByteArray(), 0));
                            System.err.println("PROXY PRODUCER: waiting for ACK");
                            Util.exec(e, trgt::recv); // ACK
                        } catch (final IOException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }

    public final class ConsumerActor extends BasicActor<Object, Void> {
        private final String srcZMQEndpoint;

        public ConsumerActor(String srcZMQEndpoint) {
            this.srcZMQEndpoint = srcZMQEndpoint;
        }

        @Override
        protected Void doRun() throws InterruptedException, SuspendExecution {
            try(final ZMQ.Socket src = zmq.socket(ZMQ.REP)) {
                System.err.printf("PROXY CONSUMER: binding %s\n", srcZMQEndpoint);
                Util.exec(e, () -> src.bind(srcZMQEndpoint));
                src.setReceiveTimeOut(1000);
                //noinspection InfiniteLoopStatement
                for (;;) {
                    final Object m = tryReceive();
                    if (EXIT.equals(m)) {
                        System.err.println("PROXY CONSUMER: exiting");
                        return null;
                    }
                    System.err.println("PROXY CONSUMER: receiving");
                    final byte[] msg = Util.call(e, src::recv);
                    if (msg != null) {
                        System.err.println("PROXY CONSUMER: ACKing");
                        Util.exec(e, () -> src.send(ACK));
                        final Object o;
                        try (final ByteArrayInputStream bis = new ByteArrayInputStream(msg);
                             final ObjectInputStream ois = new ObjectInputStream(bis)) {
                            o = ois.readObject();
                        } catch (final IOException | ClassNotFoundException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                        System.err.printf("PROXY CONSUMER: distributing '%s' to %d subscribers\n", o, subscribers.size());
                        //noinspection unchecked
                        for (final ActorRef s : subscribers.getOrDefault(srcZMQEndpoint, (List<ActorRef>) Collections.EMPTY_LIST))
                            //noinspection unchecked
                            s.send(o);
                    } else {
                        System.err.println("PROXY CONSUMER: receive timeout");
                    }
                }
            }
        }
    }

    private void ensureOpen() {
        if (closed.get())
            throw new IllegalStateException("Already closed.");
    }

    private static final ExecutorService e = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private static final String ACK = "ACK";
    private static final Object EXIT = new Object();

    private final Map<String, ActorRef> producerProxies = new ConcurrentHashMap<>();

    private final Map<String, ActorRef> consumerProxies = new ConcurrentHashMap<>();
    private final Map<String, List<ActorRef>> subscribers = new ConcurrentHashMap<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);
}