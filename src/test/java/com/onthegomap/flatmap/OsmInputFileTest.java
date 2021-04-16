package com.onthegomap.flatmap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.graphhopper.reader.ReaderElement;
import com.onthegomap.flatmap.monitoring.Stats.InMemory;
import com.onthegomap.flatmap.worker.Topology;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class OsmInputFileTest {

  private OsmInputFile file = new OsmInputFile(new File("src/test/resources/andorra-latest.osm.pbf"));

  @Test
  public void testGetBounds() {
    assertArrayEquals(new double[]{1.412368, 42.4276, 1.787481, 42.65717}, file.getBounds());
  }

  @Test
  @Timeout(30)
  public void testReadAndorraTwice() {
    for (int i = 1; i <= 2; i++) {
      AtomicInteger nodes = new AtomicInteger(0);
      AtomicInteger ways = new AtomicInteger(0);
      AtomicInteger rels = new AtomicInteger(0);
      Topology.start("test", new InMemory())
        .fromGenerator("pbf", file.read(2))
        .addBuffer("reader_queue", 1_000, 100)
        .sinkToConsumer("counter", 1, elem -> {
          switch (elem.getType()) {
            case ReaderElement.NODE -> nodes.incrementAndGet();
            case ReaderElement.WAY -> ways.incrementAndGet();
            case ReaderElement.RELATION -> rels.incrementAndGet();
          }
        }).await();
      assertEquals(246_028, nodes.get(), "nodes pass " + i);
      assertEquals(12_677, ways.get(), "ways pass " + i);
      assertEquals(287, rels.get(), "rels pass " + i);
    }
  }
}
