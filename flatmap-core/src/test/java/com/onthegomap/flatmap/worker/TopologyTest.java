package com.onthegomap.flatmap.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.onthegomap.flatmap.monitoring.ProgressLoggers;
import com.onthegomap.flatmap.monitoring.Stats;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TopologyTest {

  Stats stats = Stats.inMemory();

  @Test
  @Timeout(10)
  public void testSimpleTopology() {
    Set<Integer> result = Collections.synchronizedSet(new TreeSet<>());
    var topology = Topology.start("test", stats)
      .<Integer>fromGenerator("reader", (next) -> {
        next.accept(0);
        next.accept(1);
      }).addBuffer("reader_queue", 1)
      .<Integer>addWorker("process", 1, (prev, next) -> {
        Integer item;
        while ((item = prev.get()) != null) {
          next.accept(item * 2 + 1);
          next.accept(item * 2 + 2);
        }
      }).addBuffer("writer_queue", 1)
      .sinkToConsumer("writer", 1, result::add);

    topology.awaitAndLog(new ProgressLoggers("test"), Duration.ofSeconds(1));

    assertEquals(Set.of(1, 2, 3, 4), result);
  }

  @Test
  @Timeout(10)
  public void testTopologyFromQueue() {
    var queue = new WorkQueue<Integer>("readerqueue", 10, 1, stats);
    Set<Integer> result = Collections.synchronizedSet(new TreeSet<>());
    var topology = Topology.start("test", stats)
      .<Integer>readFromQueue(queue)
      .<Integer>addWorker("process", 1, (prev, next) -> {
        Integer item;
        while ((item = prev.get()) != null) {
          next.accept(item * 2 + 1);
          next.accept(item * 2 + 2);
        }
      }).addBuffer("writer_queue", 1)
      .sinkToConsumer("writer", 1, result::add);

    new Thread(() -> {
      queue.accept(0);
      queue.accept(1);
      queue.close();
    }).start();

    topology.await();

    assertEquals(Set.of(1, 2, 3, 4), result);
  }

  @Test
  @Timeout(10)
  public void testTopologyFromIterator() {
    Set<Integer> result = Collections.synchronizedSet(new TreeSet<>());
    var topology = Topology.start("test", stats)
      .readFrom("reader", List.of(0, 1))
      .addBuffer("reader_queue", 1)
      .<Integer>addWorker("process", 1, (prev, next) -> {
        Integer item;
        while ((item = prev.get()) != null) {
          next.accept(item * 2 + 1);
          next.accept(item * 2 + 2);
        }
      }).addBuffer("writer_queue", 1)
      .sinkToConsumer("writer", 1, result::add);

    topology.awaitAndLog(new ProgressLoggers("test"), Duration.ofSeconds(1));

    assertEquals(Set.of(1, 2, 3, 4), result);
  }

  @ParameterizedTest
  @Timeout(10)
  @ValueSource(ints = {1, 2, 3})
  public void testThrowingExceptionInTopologyHandledGracefully(int failureStage) {
    class ExpectedException extends RuntimeException {}
    Set<Integer> result = Collections.synchronizedSet(new TreeSet<>());
    var topology = Topology.start("test", stats)
      .<Integer>fromGenerator("reader", (next) -> {
        if (failureStage == 1) {
          throw new ExpectedException();
        }
        next.accept(0);
        next.accept(1);
      }).addBuffer("reader_queue", 1)
      .<Integer>addWorker("process", 1, (prev, next) -> {
        if (failureStage == 2) {
          throw new ExpectedException();
        }
        Integer item;
        while ((item = prev.get()) != null) {
          next.accept(item * 2 + 1);
          next.accept(item * 2 + 2);
        }
      }).addBuffer("writer_queue", 1)
      .sinkToConsumer("writer", 1, item -> {
        if (failureStage == 3) {
          throw new ExpectedException();
        }
      });

    assertThrows(RuntimeException.class,
      () -> topology.await());//awaitAndLog(new ProgressLoggers("test"), Duration.ofSeconds(1)));
  }
}
