package co.paralleluniverse.actors.integration.examples;

import co.paralleluniverse.common.util.CheckedCallable;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static co.paralleluniverse.fibers.FiberAsync.runBlocking;

public final class Util {
    public static final Msg EXIT = new Msg(-1);

    public static final int MSGS = 1000;

    public static void exec(ExecutorService es, Runnable r) throws InterruptedException, SuspendExecution {
        try {
            runBlocking(es, (CheckedCallable<Void, Exception>) () -> {
                r.run();
                return null;
            });
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <V> V call(ExecutorService es, Callable<V> c) throws InterruptedException, SuspendExecution {
        try {
            //noinspection RedundantCast
            return runBlocking(es, (CheckedCallable<V, Exception>) c::call);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Util() {}
}
