package de.gsi.dataset.event;

import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.*;

/**
 * Benchmark to compare callback based event listeners to ring buffer based event sourcing.
 * Different event topologies
 * Different threading models:
 * - all same thread
 * - spawn new handlers in new threads
 * - all handlers have threads polling events
 * Measure throughput, latency
 * 
 * @author Alexander Krimm
 */
@State(Scope.Benchmark)
public class EventBenchmark {
    @Param({ "true", "false" })
    private boolean parallel;

    private TestEventSource es1;
    private TestEventSource es2;
    private TestEventSource es3;

    // private TestEventSource es1b;
    // private TestEventSource es2b;
    // private Blackhole[] payload = new Blackhole[1];
    // private UpdateEvent ev1;
    // private UpdateEvent ev2;

    @Setup()
    public void initialize() {
        // 1on1r
        es1 = new TestEventSource();
        es1.addListener(event -> {
            Blackhole hole = (Blackhole) event.getPayLoad();
            Blackhole.consumeCPU(100);
            hole.consume(event);
        });
        // // 1on1r, preallocated
        // es1b = new TestEventSource();
        // es1b.addListener(event -> {
        //     Blackhole hole = ((Blackhole[]) event.getPayLoad())[0];
        //     Blackhole.consumeCPU(100);
        //     hole.consume(event);
        // });
        // ev1 =  new UpdateEvent(es1b, "test", payload);
        // 1 to many
        es2 = new TestEventSource();
        final int nListeners = 10;
        for (int i = 0; i < nListeners; i++) {
            final int index = i;
            es2.addListener(event -> {
                Blackhole hole = (Blackhole) event.getPayLoad();
                Blackhole.consumeCPU(100);
                hole.consume(index);
            });
        }
        // // 1 to many, preallocated
        // es2b = new TestEventSource();
        // for (int i = 0; i < nListeners ; i++) {
        //     final int index = i;
        //     es2b.addListener(event -> {
        //         Blackhole hole = ((Blackhole[]) event.getPayLoad())[0];
        //         Blackhole.consumeCPU(100);
        //         hole.consume(index);
        //     });
        // ev2=new UpdateEvent(es2b,"test",payload);
        // recursive
        es3 = new TestEventSource();
        es3.addListener(event ->

                {
                    int val = ((Integer) event.getPayLoad()).intValue() + 1;
                    if (val < 10) {
                        Blackhole.consumeCPU(100);
                        es3.invokeListener(new UpdateEvent(es3, "test", val), parallel);
                    }
                });
    }

    @Benchmark
    @Warmup(iterations = 1)
    @Fork(value = 2, warmups = 2)
    public void oneToOne(Blackhole blackhole) {
        es1.invokeListener(new UpdateEvent(es1, "test", blackhole), parallel);
    }

    // @Benchmark
    // @Warmup(iterations = 1)
    // @Fork(value = 2, warmups = 2)
    // public void oneToOnePreallocated(Blackhole blackhole) {
    //     payload[0] = blackhole;
    //     es1b.invokeListener(ev1, parallel);
    // }

    @Benchmark
    @Warmup(iterations = 1)
    @Fork(value = 2, warmups = 2)
    public void oneToMany(Blackhole blackhole) {
        es2.invokeListener(new UpdateEvent(es2, "test", blackhole), parallel);
    }

    // @Benchmark
    // @Warmup(iterations = 1)
    // @Fork(value = 2, warmups = 2)
    // public void oneToManyPreallocated(Blackhole blackhole) {
    //     payload[0] = blackhole;
    //     es2b.invokeListener(ev2, parallel);
    // }

    @Benchmark
    @Warmup(iterations = 1)
    @Fork(value = 2, warmups = 2)
    public void recursive(Blackhole blackhole) {
        es3.invokeListener(new UpdateEvent(es3, "test", 0), parallel);
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                // Specify which benchmarks to run. You can be more specific if you'd like to run only one benchmark per test.
                .include(EventBenchmark.class.getName() + ".*")
                // Set the following options as needed
                .mode (Mode.Throughput)
                .timeUnit(TimeUnit.SECONDS)
                .warmupTime(TimeValue.seconds(10))
                .warmupIterations(1)
                .timeout(TimeValue.minutes(10))
                .measurementTime(TimeValue.seconds(10))
                .measurementIterations(5)
                .threads(1)
                .forks(2)
                .warmupForks(2)
                .shouldFailOnError(true)
                .shouldDoGC(true)
                .addProfiler(StackProfiler.class)
                //.jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintInlining")
                .build();
            new Runner(opt).run();
    }
}
