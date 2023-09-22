package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.geo.TileCoord;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TileWeightsTest {
  @Test
  void test() {
    var weights = new TileWeights();
    assertEquals(0, weights.getWeight(TileCoord.ofXYZ(0, 0, 0)));
    assertEquals(0, weights.getZoomWeight(0));
    assertEquals(0, weights.getWeight(TileCoord.ofXYZ(0, 0, 1)));
    assertEquals(0, weights.getWeight(TileCoord.ofXYZ(1, 0, 1)));
    assertEquals(0, weights.getZoomWeight(1));
    assertTrue(weights.isEmpty());

    weights.put(TileCoord.ofXYZ(0, 0, 0), 1);
    weights.put(TileCoord.ofXYZ(0, 0, 0), 2);
    weights.put(TileCoord.ofXYZ(0, 0, 1), 3);
    weights.put(TileCoord.ofXYZ(1, 0, 1), 4);

    assertFalse(weights.isEmpty());
    assertEquals(3, weights.getWeight(TileCoord.ofXYZ(0, 0, 0)));
    assertEquals(3, weights.getZoomWeight(0));
    assertEquals(3, weights.getWeight(TileCoord.ofXYZ(0, 0, 1)));
    assertEquals(4, weights.getWeight(TileCoord.ofXYZ(1, 0, 1)));
    assertEquals(7, weights.getZoomWeight(1));
  }

  @Test
  void testWriteToFileEmpty(@TempDir Path path) throws IOException {
    Path file = path.resolve("test.tsv.gz");
    new TileWeights().writeToFile(file);
    var read = TileWeights.readFromFile(file);
    assertEquals(0, read.getWeight(TileCoord.ofXYZ(0, 0, 0)));
  }

  @Test
  void testWriteToFile(@TempDir Path path) throws IOException {
    Path file = path.resolve("test.tsv.gz");
    new TileWeights()
      .put(TileCoord.ofXYZ(0, 0, 1), 1)
      .put(TileCoord.ofXYZ(0, 0, 1), 1)
      .put(TileCoord.ofXYZ(0, 0, 0), 1)
      .writeToFile(file);
    var read = TileWeights.readFromFile(file);
    assertEquals("""
      z	x	y	loads
      0	0	0	1
      1	0	0	2
      """, new String(new GZIPInputStream(Files.newInputStream(file)).readAllBytes()));
    assertEquals(1, read.getWeight(TileCoord.ofXYZ(0, 0, 0)));
    assertEquals(2, read.getWeight(TileCoord.ofXYZ(0, 0, 1)));
  }

  @Test
  void testReadCorruptFile(@TempDir Path path) throws IOException {
    Path file = path.resolve("test.tsv.gz");
    var result = TileWeights.readFromFile(file);
    assertEquals(0, result.getWeight(TileCoord.ofXYZ(0, 0, 0)));

    Files.write(file, Gzip.gzip("""
      garbage
      """.getBytes(StandardCharsets.UTF_8)));
    assertEquals(0, TileWeights.readFromFile(file).getWeight(TileCoord.ofXYZ(0, 0, 0)));

    Files.write(file, Gzip.gzip("""
      z	x	y	loads
      a	b	c	d
      """.getBytes(StandardCharsets.UTF_8)));
    assertEquals(0, TileWeights.readFromFile(file).getWeight(TileCoord.ofXYZ(0, 0, 0)));

    Files.write(file, Gzip.gzip("""
      z	x	d	loads
      1	2	3	4
      """.getBytes(StandardCharsets.UTF_8)));
    assertEquals(0, TileWeights.readFromFile(file).getWeight(TileCoord.ofXYZ(0, 0, 0)));
    Files.write(file, Gzip.gzip("""
      z	x	y	loads
      -1	2	-3	4
      1	2	5	4
      """.getBytes(StandardCharsets.UTF_8)));
    assertEquals(4, TileWeights.readFromFile(file).getWeight(TileCoord.ofXYZ(0, 1, 1)));
  }
}
