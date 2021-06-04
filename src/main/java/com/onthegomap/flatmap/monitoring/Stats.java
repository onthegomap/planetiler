package com.onthegomap.flatmap.monitoring;

public interface Stats {

  void time(String name, Runnable task);

  default void printSummary() {
    timers().printSummary();
  }

  Timers.Finishable startTimer(String name);

  void gauge(String name, int value);

  void emittedFeature(int z, String layer, int coveringTiles);

  void encodedTile(int zoom, int length);

  void wroteTile(int zoom, int bytes);

  Timers timers();

  class InMemory implements Stats {

    private final Timers timers = new Timers();

    @Override
    public void time(String name, Runnable task) {
      timers.time(name, task);
    }

    @Override
    public Timers.Finishable startTimer(String name) {
      return timers.startTimer(name);
    }

    @Override
    public void encodedTile(int zoom, int length) {

    }

    @Override
    public void wroteTile(int zoom, int bytes) {
    }

    @Override
    public Timers timers() {
      return timers;
    }

    @Override
    public void gauge(String name, int value) {

    }

    @Override
    public void emittedFeature(int z, String layer, int coveringTiles) {
    }
  }
}
