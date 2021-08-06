package com.onthegomap.flatmap.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.flatmap.worker.WorkerPipeline;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class ProgressLoggersTest {

  @Test
  @Timeout(10)
  public void testLogWorkerPipeline() throws InterruptedException {
    var continueLatch = new CountDownLatch(1);
    var readyLatch = new CountDownLatch(5);
    var pipeline = WorkerPipeline.start("pipeline", Stats.inMemory())
      .fromGenerator("reader", next -> {
        readyLatch.countDown();
        continueLatch.await();
      })
      .addBuffer("reader_queue", 10)
      .addWorker("worker", 2, (a, b) -> {
        readyLatch.countDown();
        continueLatch.await();
      })
      .addBuffer("writer_queue", 10)
      .sinkTo("writer", 2, a -> {
        readyLatch.countDown();
        continueLatch.await();
      });

    var loggers = new ProgressLoggers("prefix")
      .addPipelineStats(pipeline);

    readyLatch.await();
    String log = loggers.getLog();

    assertEquals("[prefix]\n    reader( 0%) ->    (0/13) -> worker( 0%  0%) ->    (0/14) -> writer( 0%  0%)",
      log.replaceAll("[ 0-9][0-9]%", " 0%"));
    continueLatch.countDown();
    pipeline.awaitAndLog(loggers, Duration.ofSeconds(10));
    loggers.getLog();
    assertEquals("[prefix]\n    reader( -%) ->    (0/13) -> worker( -%  -%) ->    (0/14) -> writer( -%  -%)",
      loggers.getLog());
  }
}
