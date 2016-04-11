package co.paralleluniverse.actors.integration.examples.proxied.zeromq;

import co.paralleluniverse.actors.Actor;
import co.paralleluniverse.actors.integration.examples.proxied.ConsumerActor;
import co.paralleluniverse.actors.integration.examples.proxied.ProducerActor;

import java.util.concurrent.ExecutionException;

public final class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (final ZeroMQProxies proxies = new ZeroMQProxies(1)) {
            final ProducerActor pa = Actor.newActor(ProducerActor.class, proxies.to("tcp://localhost:8000"));
            final ConsumerActor ca = Actor.newActor(ConsumerActor.class);
            pa.spawn();
            System.err.println("USER PRODUCER started");
            proxies.subscribe(ca.spawn(), "tcp://*:8000");
            System.err.println("USER CONSUMER started");
            pa.join();
            System.err.println("USER PRODUCER finished");
            ca.join();
            System.err.println("USER CONSUMER finished");
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private Main() {}
}
