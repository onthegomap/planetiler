package com.onthegomap.flatmap;

import com.onthegomap.flatmap.worker.Topology;
import com.onthegomap.flatmap.worker.WorkQueue;
import com.onthegomap.flatmap.worker.Worker;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public class ProgressLoggers {

  public ProgressLoggers(String name) {

  }

  public ProgressLoggers addRatePercentCounter(String name, long total, AtomicLong value) {
    return addRatePercentCounter(name, total, value::get);
  }

  public ProgressLoggers addRatePercentCounter(String name, long total, LongSupplier getValue) {
    return this;
  }

  public ProgressLoggers addRateCounter(String name, AtomicLong featuresWritten) {
    return this;
  }

  public ProgressLoggers addFileSize(LongSupplier getStorageSize) {
    return this;
  }

  public ProgressLoggers addProcessStats() {
    return this;
  }

  public ProgressLoggers addThreadPoolStats(String name, String prefix) {
    return this;
  }

  public ProgressLoggers addThreadPoolStats(String name, Worker worker) {
    return addThreadPoolStats(name, worker.getPrefix());
  }

  public ProgressLoggers addQueueStats(WorkQueue<?> queue) {
    return this;
  }

  public ProgressLoggers addTopologyStats(Topology<?> topology) {
    if (topology == null) {
      return this;
    }
    return addTopologyStats(topology.previous())
      .addQueueStats(topology.inputQueue())
      .addThreadPoolStats(topology.name(), topology.worker());
  }
}
