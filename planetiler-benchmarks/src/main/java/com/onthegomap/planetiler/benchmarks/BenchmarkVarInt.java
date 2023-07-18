package com.onthegomap.planetiler.benchmarks;

import static io.prometheus.client.Collector.NANOSECONDS_PER_SECOND;

import com.carrotsearch.hppc.ByteArrayList;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.VarInt;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BenchmarkVarInt {

  public static void main(String[] args) throws IOException {

    long num = 100000000;

    for (int i = 0; i < 3; i++) {
      ByteArrayList stream = new ByteArrayList();
      var timer = Timer.start();

      long sum = 0;

      for (long l = 0; l < num; l++) {
        VarInt.putVarLong(l, stream);
        sum += l;
      }

      ByteBuffer buf = ByteBuffer.wrap(stream.toArray());

      long acc = 0;
      for (long l = 0; l < num; l++) {
        acc += VarInt.getVarLong(buf);
      }

      if (sum != acc) {
        System.err.println("Sums do not match");
      }


      System.err.println(
        num + " varints took " +
          Format.defaultInstance().duration(timer.stop().elapsed().wall()) + " (" +
          Format.defaultInstance()
            .numeric(num * 1d / (timer.stop().elapsed().wall().toNanos() / NANOSECONDS_PER_SECOND)) +
          "/s)"
      );
    }
  }
}
