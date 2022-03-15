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
  @Timeout(10)
  public void testWaitForBothWritersToClose() throws InterruptedException {
    var writer1 = parallel.newWriter();
    var writer2 = parallel.newWriter();
    writer1.put(0, 1);
    writer1.close();
    writer2.put(1, 2);
    writer2.close();
    assertEquals(1, parallel.get(0));
    assertEquals(2, parallel.get(1));
  }

  @Test
  @Timeout(10)
  public void testInterleavedWritesFromParallelThreads() throws InterruptedException {
    int limit = 1000;
    var ready = new CyclicBarrier(2);
    Thread thread1 = new Thread(() -> {
      try (var writer = parallel.newWriter()) {
        ready.await();
        for (int i = 1; i < limit; i++) {
          writer.put(i * 2, i * 2);
        }
      } catch (InterruptedException | BrokenBarrierException e) {
        throw new RuntimeException(e);
      }
    });
    Thread thread2 = new Thread(() -> {
      try (var writer = parallel.newWriter()) {
        ready.await();
        for (int i = 1; i < limit; i++) {
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
    assertEquals(Long.MIN_VALUE, parallel.get(0));
    for (int i = 1; i < limit; i++) {
      assertEquals(i * 2, parallel.get(i * 2), "item:" + i * 2);
      assertEquals(i * 3, parallel.get(i * 10 + 1), "item:" + (i * 10 + 1));
    }
  }

  @Test
  @Timeout(10)
  public void testAdjacentBlocksFromParallelThreads() throws InterruptedException {
    int limit = 1000;
    var ready = new CyclicBarrier(2);
    Thread thread1 = new Thread(() -> {
      try (var writer = parallel.newWriter()) {
        ready.await();
        for (int i = 1; i < limit; i++) {
          writer.put(i, i);
        }
      } catch (InterruptedException | BrokenBarrierException e) {
        throw new RuntimeException(e);
      }
    });
    Thread thread2 = new Thread(() -> {
      try (var writer = parallel.newWriter()) {
        ready.await();
        for (int i = 1; i < limit * 2; i++) {
          writer.put(i + limit, i * 2L);
        }
      } catch (InterruptedException | BrokenBarrierException e) {
        throw new RuntimeException(e);
      }
    });
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();
    assertEquals(Long.MIN_VALUE, parallel.get(0));
    for (int i = 1; i < limit; i++) {
      assertEquals(i, parallel.get(i), "item:" + i);
      assertEquals(i * 2, parallel.get(i + limit), "item:" + i * 2);
    }
  }

  @BeforeEach
  public void setupParallelWriter(@TempDir Path path) {
    this.parallel = create(path);
  }

  @Override
  protected LongLongMap.SequentialWrites createSequentialWriter(Path path) {
    var sequentialMap = create(path);
    var writer = sequentialMap.newWriter();
    return new LongLongMap.SequentialWrites() {
      @Override
      public void put(long key, long value) {
        writer.put(key, value);
      }

      @Override
      public long get(long key) {
        return sequentialMap.get(key);
      }

      @Override
      public void close() throws IOException {
        sequentialMap.close();
        writer.close();
      }
    };
  }

  public static class ArrayMmapLarge extends ParallelLongLongMapTest {

    @Override
    protected LongLongMap.ParallelWrites create(Path path) {
      return new ArrayLongLongMapMmap(path.resolve("node.db"), 20, 2, true);
    }
  }

  public static class ArrayMmapSmall extends ParallelLongLongMapTest {

    @Override
    protected LongLongMap.ParallelWrites create(Path path) {
      return new ArrayLongLongMapMmap(path.resolve("node.db"), 4, 2, true);
    }
  }

  public static class ArrayDirectSmall extends ParallelLongLongMapTest {

    @Override
    protected LongLongMap.ParallelWrites create(Path path) {
      return new ArrayLongLongMapRam(4);
    }
  }

  public static class ArrayDirectLarge extends ParallelLongLongMapTest {

    @Override
    protected LongLongMap.ParallelWrites create(Path path) {
      return new ArrayLongLongMapRam(10);
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
