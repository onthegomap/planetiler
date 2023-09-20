package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.stats.Stats;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TopOsmTilesTest {
  @Test
  void fetchTopOsmTiles(@TempDir Path dir) throws IOException {
    var output = dir.resolve("test.tsv.gz");
    var config = PlanetilerConfig.defaults();
    var stats = Stats.inMemory();
    var day1 = LocalDate.ofEpochDay(0);
    var day2 = LocalDate.ofEpochDay(1);
    var day3 = LocalDate.ofEpochDay(2); /// no data
    var topOsmTiles = new TopOsmTiles(config, stats) {
      @Override
      Reader fetch(LocalDate date) throws IOException {
        if (date.equals(day1)) {
          return new StringReader("""
            0/0/0 1
            1/0/0 2
            1/1/0 3
            2/0/0 4
            """);
        } else if (date.equals(day2)) {
          return new StringReader("""
            0/0/0 2
            1/0/0 4
            1/1/0 6
            2/0/0 8
            3/0/0 1
            """);
        } else {
          throw new FileNotFoundException();
        }
      }
    };

    topOsmTiles.run(2, 2, 4, output, List.of(day1, day2, day3));
    var tileWeights = TopOsmTiles.loadFromFile(output);
    assertEquals("""
      z x y loads
      0	0	0	15
      1	0	0	12
      """.replace(' ', '\t'), new String(new GZIPInputStream(Files.newInputStream(output)).readAllBytes()));
    assertEquals(15, tileWeights.getWeight(TileCoord.ofXYZ(0, 0, 0)));
    assertEquals(12, tileWeights.getWeight(TileCoord.ofXYZ(0, 0, 1)));
    assertEquals(0, tileWeights.getWeight(TileCoord.ofXYZ(0, 0, 2)));
  }

  @Test
  void retries(@TempDir Path dir) throws IOException {
    var output = dir.resolve("test.tsv.gz");
    var config = PlanetilerConfig.from(Arguments.of(Map.of(
      "http-retries", "3"
    )));
    var stats = Stats.inMemory();
    var day1 = LocalDate.ofEpochDay(0);
    var topOsmTiles = new TopOsmTiles(config, stats) {
      int tries = 3;

      @Override
      Reader fetch(LocalDate date) throws IOException {
        if (date.equals(day1)) {
          if (tries-- > 0) {
            throw new IOException("Injected download failure");
          }
          return new StringReader("""
            1/0/0 2
            """);
        } else {
          throw new IOException("other failure");
        }
      }
    };

    topOsmTiles.run(2, 2, 4, output, List.of(day1));
    TopOsmTiles.loadFromFile(output);
    assertEquals("""
      z x y loads
      0	0	0	2
      """.replace(' ', '\t'), new String(new GZIPInputStream(Files.newInputStream(output)).readAllBytes()));
  }

  @Test
  void exhaustRetries(@TempDir Path dir) throws IOException {
    var output = dir.resolve("test.tsv.gz");
    var config = PlanetilerConfig.from(Arguments.of(Map.of(
      "http-retries", "3"
    )));
    var stats = Stats.inMemory();
    var day1 = LocalDate.ofEpochDay(0);
    var topOsmTiles = new TopOsmTiles(config, stats) {
      int tries = 4;

      @Override
      Reader fetch(LocalDate date) throws IOException {
        if (date.equals(day1)) {
          if (tries-- > 0) {
            throw new IOException("Injected download failure");
          }
          return new StringReader("""
            1/0/0 2
            """);
        } else {
          throw new IOException("other failure");
        }
      }
    };

    topOsmTiles.run(2, 2, 4, output, List.of(day1));
    TopOsmTiles.loadFromFile(output);
    assertEquals("""
      z x y loads
      """.replace(' ', '\t'), new String(new GZIPInputStream(Files.newInputStream(output)).readAllBytes()));
  }
}
