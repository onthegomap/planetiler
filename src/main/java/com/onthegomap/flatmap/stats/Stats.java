package com.onthegomap.flatmap.stats;

public interface Stats {

  void time(String name, Runnable task);

  void printSummary();

  void startTimer(String name);

  void stopTimer(String name);
}
