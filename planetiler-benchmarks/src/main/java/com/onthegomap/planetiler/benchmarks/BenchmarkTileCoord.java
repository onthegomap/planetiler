package com.onthegomap.planetiler.benchmarks;

import static io.prometheus.client.Collector.NANOSECONDS_PER_SECOND;

import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.util.Format;

public class BenchmarkTileCoord {

  public static void main(String[] args) {
    for (int i = 0; i < 3; i++) {
      var timer = Timer.start();
      int num = 0;
      for (int z = 0; z <= 14; z++) {
        int max = 1 << z;
        for (int x = 0; x < max; x++) {
          for (int y = 0; y < max; y++) {
            int encoded = TileCoord.encode(x, y, z);
            int decoded = TileCoord.decode(encoded).encoded();
            // make sure we use the result so it doesn't get jit'ed-out
            if (encoded != decoded) {
              System.err.println("Error on " + z + "/" + x + "/" + y);
            }
            num++;
          }
        }
      }
      System.err.println(
        "z0-z14 took " +
          Format.defaultInstance().duration(timer.stop().elapsed().wall()) + " (" +
          Format.defaultInstance()
            .numeric(num * 1d / (timer.stop().elapsed().wall().toNanos() / NANOSECONDS_PER_SECOND)) +
          "/s)"
      );
    }
  }
}
