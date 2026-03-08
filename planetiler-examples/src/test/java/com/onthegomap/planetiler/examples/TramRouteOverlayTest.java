package com.onthegomap.planetiler.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.reader.SimpleFeature;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TramRouteOverlayTest {

  private final TramRouteOverlay profile = new TramRouteOverlay();

  @Test
  public void testSourceFeatureProcessing() {
    // Test tram stops first, then tram routes
    var node = SimpleFeature.create(
      TestUtils.newPoint(5, 4),
      Map.of("railway", "tram_stop")
    );
    // Produce a one-element list with a node containing the tag railway=tram_stop
    List<FeatureCollector.Feature> mapFeatures = TestUtils.processSourceFeature(node, profile);
    // Assertion cases
    assertEquals(1, mapFeatures.size());
    var feature = mapFeatures.get(0);
    assertEquals("tram_stop", feature.getLayer());
    assertEquals(11, feature.getMinZoom());
    // Create a fake route as well and test that in this unit test
  }


  @Test
  public void unitTest() {
    var profile = new TramRouteOverlay();
    //
  }
}
