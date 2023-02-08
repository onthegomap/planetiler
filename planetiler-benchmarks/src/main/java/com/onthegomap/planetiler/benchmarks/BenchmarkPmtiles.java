package com.onthegomap.planetiler.benchmarks;

import static io.prometheus.client.Collector.NANOSECONDS_PER_SECOND;

import com.onthegomap.planetiler.pmtiles.Pmtiles;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.util.Format;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class BenchmarkPmtiles {

  public static void main(String[] args) throws IOException {

    long num = 60_000_000;

    var random = new Random(0);

    for (int i = 0; i < 3; i++) {

      var entries = new ArrayList<Pmtiles.Entry>();

      long offset = 0;
      for (int j = 0; j < num; j++) {
        int len = 200 + random.nextInt(64000);
        entries.add(new Pmtiles.Entry(j, offset, len, 1));
        offset += len;
      }

      var timer = Timer.start();

      var result = Pmtiles.deserializeDirectory(Pmtiles.serializeDirectory(entries));
      assert (result.size() == entries.size());

      System.err.println(
        num + " entries took " +
          Format.defaultInstance().duration(timer.stop().elapsed().wall()) + " (" +
          Format.defaultInstance()
            .numeric(num * 1d / (timer.stop().elapsed().wall().toNanos() / NANOSECONDS_PER_SECOND)) +
          "/s)"
      );
    }
  }
}
