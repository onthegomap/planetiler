package com.onthegomap.flatmap;

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
}
