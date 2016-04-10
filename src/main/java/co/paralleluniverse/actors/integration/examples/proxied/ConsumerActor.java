package co.paralleluniverse.actors.integration.examples.proxied;

import co.paralleluniverse.actors.BasicActor;
import co.paralleluniverse.actors.integration.examples.Msg;
import co.paralleluniverse.fibers.SuspendExecution;

import static co.paralleluniverse.actors.integration.examples.Util.*;

public final class ConsumerActor extends BasicActor<Msg, Void> {
    @Override
    protected final Void doRun() throws InterruptedException, SuspendExecution {
        for (; ; ) {
            final Msg m = receive();
            System.err.println("USER CONSUMER: " + m);
            if (EXIT.equals(m))
                return null;
        }
    }
}
