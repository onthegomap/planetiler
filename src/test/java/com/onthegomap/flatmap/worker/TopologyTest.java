package com.onthegomap.flatmap.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.flatmap.monitoring.ProgressLoggers;
import com.onthegomap.flatmap.monitoring.Stats;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TopologyTest {

  Stats stats = new Stats.InMemory();

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

    queue.accept(0);
    queue.accept(1);
    queue.close();

    topology.await();

    assertEquals(Set.of(1, 2, 3, 4), result);
  }

  @Test
  @Timeout(10)
  public void testTopologyFromIterator() {
    Set<Integer> result = Collections.synchronizedSet(new TreeSet<>());
    var topology = Topology.start("test", stats)
      .readFromIterator("reader", List.of(0, 1).iterator())
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
}
