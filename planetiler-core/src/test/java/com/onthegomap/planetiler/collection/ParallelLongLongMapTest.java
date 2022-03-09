package com.onthegomap.planetiler.collection;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

public abstract class ParallelLongLongMapTest extends LongLongMapTest {

  private LongLongMap.ParallelWrites parallel;

  protected abstract LongLongMap.ParallelWrites create(Path path);

  @Test
  @Timeout(1)
  public void testParallelWritesFromSingleThread() {
    try (
      var writer1 = parallel.newWriter();
      var writer2 = parallel.newWriter();
    ) {
      // ordered withing each writer, but out of order between writers
      System.err.println("2 4");
      writer1.put(2, 4);
      System.err.println("1 2");
      writer2.put(1, 2);
      System.err.println("4 8");
      writer1.put(4, 8);
      System.err.println("3 6");
      writer2.put(3, 6);
    }
    assertEquals(Long.MIN_VALUE, sequential.get(0));
    assertEquals(2, sequential.get(1));
    assertEquals(4, sequential.get(2));
    assertEquals(6, sequential.get(3));
    assertEquals(8, sequential.get(4));
  }

  @Test
  @Timeout(10)
  public void testParallelWritesFromMultipleThreads() throws InterruptedException {
    var ready = new CyclicBarrier(2);
    Thread thread1 = new Thread(() -> {
      try (var writer = parallel.newWriter()) {
        ready.await();
        for (int i = 1; i < 100; i++) {
          System.err.println("1: put " + i);
          writer.put(i * 2, i * 2);
        }
      } catch (InterruptedException | BrokenBarrierException e) {
        throw new RuntimeException(e);
      }
    });
    Thread thread2 = new Thread(() -> {
      try (var writer = parallel.newWriter()) {
        ready.await();
        for (int i = 1; i < 100; i++) {
          System.err.println("2: put " + i * 50);
          writer.put(i * 10 + 1, i * 3);
        }
      } catch (InterruptedException | BrokenBarrierException e) {
        throw new RuntimeException(e);
      }
    });
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();
    assertEquals(Long.MIN_VALUE, sequential.get(0));
    for (int i = 1; i < 100; i++) {
      assertEquals(i * 2, sequential.get(i * 2));
      assertEquals(i * 3, sequential.get(i * 10 + 1));
    }
  }

  @BeforeEach
  public void setup(@TempDir Path path) {
    this.parallel = create(path);
    var writer = create(path).newWriter();
    this.sequential = new LongLongMap.SequentialWrites() {
      @Override
      public void put(long key, long value) {
        writer.put(key, value);
      }

      @Override
      public long get(long key) {
        return parallel.get(key);
      }

      @Override
      public void close() throws IOException {
        writer.close();
        parallel.close();
      }
    };
  }

  public static class ArrayMmapLarge extends ParallelLongLongMapTest {

    @Override
    protected LongLongMap.ParallelWrites create(Path path) {
      return new ArrayLongLongMapMmap(path.resolve("node.db"), 20, 2);
    }
  }

  public static class ArrayMmapSmall extends ParallelLongLongMapTest {

    @Override
    protected LongLongMap.ParallelWrites create(Path path) {
      return new ArrayLongLongMapMmap(path.resolve("node.db"), 4, 2);
    }
  }

  public static class ArrayDirectSmall extends ParallelLongLongMapTest {

    @Override
    protected LongLongMap.ParallelWrites create(Path path) {
      return new ArrayLongLongMapDirect(4);
    }
  }

  public static class ArrayDirectLarge extends ParallelLongLongMapTest {

    @Override
    protected LongLongMap.ParallelWrites create(Path path) {
      return new ArrayLongLongMapDirect(10);
    }
  }

  public static class ArrayRamLarge extends ParallelLongLongMapTest {

    @Override
    protected LongLongMap.ParallelWrites create(Path path) {
      return new ArrayLongLongMapRam(10);
    }
  }

  public static class ArrayRamSmall extends ParallelLongLongMapTest {

    @Override
    protected LongLongMap.ParallelWrites create(Path path) {
      return new ArrayLongLongMapRam(4);
    }
  }
}
