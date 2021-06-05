package com.onthegomap.flatmap.monitoring;

public class Timer {

  private ProcessTime start, end;

  public Timer start() {
    start = ProcessTime.now();
    return this;
  }

  public Timer stop() {
    end = ProcessTime.now();
    return this;
  }

  public boolean running() {
    return end == null;
  }

  public ProcessTime elapsed() {
    return (end == null ? ProcessTime.now() : end).minus(start);
  }

  @Override
  public String toString() {
    return elapsed().toString();
  }
}
