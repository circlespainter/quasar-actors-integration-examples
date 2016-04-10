package co.paralleluniverse.actors.integration.examples.proxied.kafka;

import co.paralleluniverse.actors.Actor;
import co.paralleluniverse.actors.integration.examples.proxied.ConsumerActor;
import co.paralleluniverse.actors.integration.examples.proxied.ProducerActor;

import java.util.concurrent.ExecutionException;

public final class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (final KafkaProxies proxies = new KafkaProxies("mbp-fabio-lnx:9092", "quasar-actors-kafka-test-proxied")) {
            final ProducerActor pa = Actor.newActor(ProducerActor.class, proxies.create("consumer", 1024));
            final ConsumerActor ca = Actor.newActor(ConsumerActor.class);
            pa.spawn();
            System.err.println("USER PRODUCER started");
            proxies.subscribe(ca.spawn(), "consumer");
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
