package com.onthegomap.planetiler.reader.osm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Envelope;

public class OsmInputFileTest {

  private final Path path = TestUtils.pathToResource("monaco-latest.osm.pbf");

  @Test
  public void testGetBounds() {
    assertEquals(new Envelope(7.409205, 7.448637, 43.72335, 43.75169), new OsmInputFile(path).getLatLonBounds());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Timeout(30)
  public void testReadMonacoTwice(boolean lazy) {
    for (int i = 1; i <= 2; i++) {
      AtomicInteger nodes = new AtomicInteger(0);
      AtomicInteger ways = new AtomicInteger(0);
      AtomicInteger rels = new AtomicInteger(0);
      var file = new OsmInputFile(path, lazy);
      try (var osmReader = file.newReader()) {
        WorkerPipeline.start("test", Stats.inMemory())
          .fromGenerator("pbf", osmReader.readBlocks())
          .addBuffer("pbf_blocks", 100)
          .sinkToConsumer("counter", 1, block -> {
            for (var elem : block.parse()) {
              if (elem instanceof OsmElement.Node) {
                nodes.incrementAndGet();
              } else if (elem instanceof OsmElement.Way) {
                ways.incrementAndGet();
              } else if (elem instanceof OsmElement.Relation) {
                rels.incrementAndGet();
              }
            }
          }).await();
        assertEquals(25_423, nodes.get(), "nodes pass " + i);
        assertEquals(4_106, ways.get(), "ways pass " + i);
        assertEquals(243, rels.get(), "rels pass " + i);
      }
    }
  }
}
