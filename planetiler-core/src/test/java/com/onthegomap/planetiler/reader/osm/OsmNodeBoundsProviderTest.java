package com.onthegomap.planetiler.reader.osm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;

class OsmNodeBoundsProviderTest {
  @Test
  void test() {
    var config = PlanetilerConfig.defaults();
    var stats = Stats.inMemory();
    var inputFile = new OsmInputFile(TestUtils.pathToResource("monaco-latest.osm.pbf"));
    var provider = new OsmNodeBoundsProvider(inputFile, config, stats);
    assertEquals(new Envelope(7.4016897, 7.5002447, 43.5165358, 43.7543341), provider.getLatLonBounds());
  }
}
