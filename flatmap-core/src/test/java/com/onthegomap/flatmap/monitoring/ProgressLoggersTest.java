package com.onthegomap.flatmap.monitoring;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.flatmap.worker.WorkerPipeline;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class ProgressLoggersTest {

  @Test
  @Timeout(10)
  public void testLogWorkerPipeline() {
    var latch = new CountDownLatch(1);
    var pipeline = WorkerPipeline.start("topo", Stats.inMemory())
      .fromGenerator("reader", next -> latch.await())
      .addBuffer("reader_queue", 10)
      .addWorker("worker", 2, (a, b) -> latch.await())
      .addBuffer("writer_queue", 10)
      .sinkTo("writer", 2, a -> latch.await());

    var loggers = new ProgressLoggers("prefix")
      .addPipelineStats(pipeline);

    String log;
    while ((log = loggers.getLog()).split("%").length < 6) {
      // spin waiting for threads to start
    }

    assertEquals("[prefix]\n    reader( 0%) ->    (0/10) -> worker( 0%  0%) ->    (0/10) -> writer( 0%  0%)",
      log.replaceAll("[ 0-9][0-9]%", " 0%"));
    latch.countDown();
    pipeline.awaitAndLog(loggers, Duration.ofSeconds(10));
    loggers.getLog();
    assertEquals("[prefix]\n    reader( -%) ->    (0/10) -> worker( -%  -%) ->    (0/10) -> writer( -%  -%)",
      loggers.getLog());
  }
}
