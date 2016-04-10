package co.paralleluniverse.actors.integration.examples;

import co.paralleluniverse.common.util.CheckedCallable;
import co.paralleluniverse.fibers.SuspendExecution;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static co.paralleluniverse.fibers.FiberAsync.runBlocking;

public final class Util {
    public static final Msg EXIT = new Msg(-1);

    public static final int MSGS = 1000;

    public static void exec(ExecutorService es, Runnable r) throws InterruptedException, SuspendExecution {
        //noinspection Convert2Lambda
        runBlocking(es, new CheckedCallable<Object, RuntimeException>() {
            @Override
            public final Object call() throws RuntimeException {
                r.run();
                return null;
            }
        });
    }

    public static <V> V call(ExecutorService es, Callable<V> c) throws InterruptedException, SuspendExecution {
        try {
            //noinspection Convert2Lambda,Anonymous2MethodRef
            return runBlocking(es, new CheckedCallable<V, Exception>() {
                @Override
                public final V call() throws Exception {
                    return c.call();
                }
            });
        } catch (final InterruptedException e) {
            throw e;
        } catch (final Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private Util() {}
}
