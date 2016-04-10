package co.paralleluniverse.actors.integration.examples.proxied;

import co.paralleluniverse.actors.ActorRef;
import co.paralleluniverse.actors.BasicActor;
import co.paralleluniverse.actors.integration.examples.Msg;
import co.paralleluniverse.fibers.SuspendExecution;

import static co.paralleluniverse.actors.integration.examples.Util.*;

public final class ProducerActor extends BasicActor<Void, Void> {
    private final ActorRef<Msg> target;

    public ProducerActor(ActorRef<Msg> target) {
        this.target = target;
    }

    @Override
    protected final Void doRun() throws InterruptedException, SuspendExecution {
        for (int i = 0; i < MSGS; i++) {
            final Msg m = new Msg(i);
            System.err.println("USER PRODUCER: " + m);
            target.send(m);
        }
        System.err.println("USER PRODUCER: " + EXIT);
        target.send(EXIT);
        return null;
    }
}
