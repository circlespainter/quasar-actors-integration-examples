package co.paralleluniverse.actors.integration.examples.direct.zeromq;

import co.paralleluniverse.actors.Actor;
import co.paralleluniverse.actors.BasicActor;
import co.paralleluniverse.actors.integration.examples.Msg;
import co.paralleluniverse.fibers.SuspendExecution;
import com.google.common.base.Charsets;
import org.zeromq.ZMQ;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static co.paralleluniverse.actors.integration.examples.Util.*;

public final class Main {
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
            ep.shutdown();
            ec.shutdown();
        }
    }

    private static final String TARGET_ADDR = "tcp://localhost:8000";

    private static final ExecutorService ep = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    @SuppressWarnings("WeakerAccess")
    public static final class ProducerActor extends BasicActor<Object, Void> {
        @Override
        protected final Void doRun() throws InterruptedException, SuspendExecution {
            try (final ZMQ.Context zmq = ZMQ.context(1 /* IO threads */);
                 final ZMQ.Socket trgt = zmq.socket(ZMQ.REQ)) {
                System.err.println("PRODUCER: connecting to " + TARGET_ADDR);
                exec(ep, () -> trgt.connect(TARGET_ADDR));
                for (int i = 0; i < MSGS; i++) {
                    final Msg m = new Msg(i);
                    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024) ;
                         final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                        oos.writeObject(m);
                        oos.flush();
                        baos.flush();
                        System.err.printf("PRODUCER: sending %s\n", m);
                        call(ep, () -> trgt.send(baos.toByteArray(), 0));
                        System.err.println("PRODUCER: ACK received");
                        call(ep, trgt::recv); // ACK
                    } catch (final IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }

                System.err.println("PRODUCER: sending exit");
                call(ep, () -> trgt.send("exit".getBytes(Charsets.US_ASCII), 0));
                System.err.println("PRODUCER: exiting");
                return null;
            }
        }
    }

    private static final ExecutorService ec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private static final String SRC_BIND_ENDPOINT = "tcp://*:8000";

    @SuppressWarnings("WeakerAccess")
    public static final class ConsumerActor extends BasicActor<Object, Void> {
        @Override
        protected final Void doRun() throws InterruptedException, SuspendExecution {
            try (final ZMQ.Context zmq = ZMQ.context(1 /* IO threads */);
                 final ZMQ.Socket src = zmq.socket(ZMQ.REP)) {
                System.err.println("CONSUMER: binding src");
                exec(ec, () -> src.bind(SRC_BIND_ENDPOINT));
                //noinspection InfiniteLoopStatement
                for (;;) {
                    System.err.println("CONSUMER: receiving");
                    final byte[] v = call(ec, src::recv);
                    System.err.println("CONSUMER: ACKing");
                    exec(ec, () -> src.send(ACK));
                    if (Arrays.equals("exit".getBytes(Charsets.US_ASCII), v)) {
                        System.err.println("CONSUMER: exiting");
                        return null;
                    }

                    try (final ByteArrayInputStream bis = new ByteArrayInputStream(v);
                         final ObjectInputStream ois = new ObjectInputStream(bis)) {
                        System.err.printf("CONSUMER: received %s\n", ois.readObject());
                    } catch (final IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private static final String ACK = "ACK";
    private Main() {}
}
