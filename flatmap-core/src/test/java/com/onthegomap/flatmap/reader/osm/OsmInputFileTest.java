package com.onthegomap.flatmap.reader.osm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.graphhopper.reader.ReaderElement;
import com.onthegomap.flatmap.TestUtils;
import com.onthegomap.flatmap.stats.Stats;
import com.onthegomap.flatmap.worker.WorkerPipeline;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.locationtech.jts.geom.Envelope;

public class OsmInputFileTest {

  private final OsmInputFile file = new OsmInputFile(TestUtils.pathToResource("monaco-latest.osm.pbf"));

  @Test
  public void testGetBounds() {
    assertEquals(new Envelope(7.409205, 7.448637, 43.72335, 43.75169), file.getLatLonBounds());
  }

  @Test
  @Timeout(30)
  public void testReadMonacoTwice() {
    for (int i = 1; i <= 2; i++) {
      AtomicInteger nodes = new AtomicInteger(0);
      AtomicInteger ways = new AtomicInteger(0);
      AtomicInteger rels = new AtomicInteger(0);
      WorkerPipeline.start("test", Stats.inMemory())
        .fromGenerator("pbf", file.read("test", 2))
        .addBuffer("reader_queue", 1_000, 100)
        .sinkToConsumer("counter", 1, elem -> {
          switch (elem.getType()) {
            case ReaderElement.NODE -> nodes.incrementAndGet();
            case ReaderElement.WAY -> ways.incrementAndGet();
            case ReaderElement.RELATION -> rels.incrementAndGet();
          }
        }).await();
      assertEquals(25_423, nodes.get(), "nodes pass " + i);
      assertEquals(4_106, ways.get(), "ways pass " + i);
      assertEquals(243, rels.get(), "rels pass " + i);
    }
  }
}
