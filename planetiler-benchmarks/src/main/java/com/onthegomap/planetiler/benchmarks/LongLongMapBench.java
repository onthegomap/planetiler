package com.onthegomap.planetiler.benchmarks;

import static io.prometheus.client.Collector.NANOSECONDS_PER_SECOND;

import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.worker.Worker;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Performance tests for {@link LongLongMap} implementations. Adds items to the map then reads from it for 30s using
 * multiple threads and reports memory/disk usage, writes and reads per second.
 */
public class LongLongMapBench {

  public static void main(String[] args) throws InterruptedException {
    Path path = Path.of("./llmaptest");
    FileUtils.delete(path);
    LongLongMap map = LongLongMap.from(args[0], args[1], path);
    long entries = Long.parseLong(args[2]);
    int readers = Integer.parseInt(args[3]);

    class LocalCounter {

      long count = 0;
    }
    LocalCounter counter = new LocalCounter();
    ProgressLoggers loggers = ProgressLoggers.create()
      .addRatePercentCounter("entries", entries, () -> counter.count)
      .newLine()
      .addProcessStats();
    AtomicReference<String> writeRate = new AtomicReference<>();
    new Worker("writer", Stats.inMemory(), 1, () -> {
      long start = System.nanoTime();
      for (long i = 0; i < entries; i++) {
        map.put(i + 1L, i + 2L);
        counter.count = i;
      }
      long end = System.nanoTime();
      String rate = Format.formatNumeric(entries * NANOSECONDS_PER_SECOND / (end - start), false) + "/s";
      System.err.println("Loaded " + entries + " in " + Duration.ofNanos(end - start).toSeconds() + "s (" + rate + ")");
      writeRate.set(rate);
    }).awaitAndLog(loggers, Duration.ofSeconds(10));

    map.get(1);
    System.err.println("Storage: " + Format.formatStorage(map.diskUsageBytes(), false));
    System.err.println("RAM: " + Format.formatStorage(map.estimateMemoryUsageBytes(), false));

    Counter.Readable readCount = Counter.newMultiThreadCounter();
    loggers = ProgressLoggers.create()
      .addRateCounter("entries", readCount)
      .newLine()
      .addProcessStats();
    CountDownLatch latch = new CountDownLatch(readers);
    for (int i = 0; i < readers; i++) {
      int rnum = i;
      new Thread(() -> {
        latch.countDown();
        try {
          latch.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        Random random = new Random(rnum);
        try {
          long sum = 0;
          long b = 0;
          while (b == 0) {
            readCount.inc();
            long key = 1L + (Math.abs(random.nextLong()) % entries);
            long value = map.get(key);
            assert key + 1 == value : key + " value was " + value;
            sum += value;
          }
          System.err.println(sum);
        } catch (Throwable e) {
          e.printStackTrace();
          System.exit(1);
        }
      }).start();
    }
    latch.await();
    long start = System.nanoTime();
    for (int i = 0; i < 3; i++) {
      Thread.sleep(10000);
      loggers.log();
    }
    long end = System.nanoTime();
    long read = readCount.getAsLong();
    String readRate = Format.formatNumeric(read * NANOSECONDS_PER_SECOND / (end - start), false) + "/s";
    System.err.println("Read " + read + " in 30s (" + readRate + ")");
    System.err.println(
      String.join("\t",
        args[0],
        args[1],
        args[2],
        args[3],
        Format.formatStorage(map.estimateMemoryUsageBytes(), false),
        Format.formatStorage(map.diskUsageBytes(), false),
        Format.formatStorage(FileUtils.size(path), false),
        writeRate.get(),
        readRate
      )
    );
    FileUtils.delete(path);
    Thread.sleep(100);
    System.exit(0);
  }
}
