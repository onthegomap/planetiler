package com.onthegomap.planetiler.basemap.layers;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class HousenumberTest extends AbstractLayerTest {

  @Test
  public void testHousenumber() {
    assertFeatures(
        14,
        List.of(
            Map.of(
                "_layer",
                "housenumber",
                "_type",
                "point",
                "_minzoom",
                14,
                "_maxzoom",
                14,
                "_buffer",
                8d)),
        process(pointFeature(Map.of("addr:housenumber", "10"))));
    assertFeatures(
        14,
        List.of(
            Map.of(
                "_layer",
                "housenumber",
                "_type",
                "point",
                "_minzoom",
                14,
                "_maxzoom",
                14,
                "_buffer",
                8d)),
        process(polygonFeature(Map.of("addr:housenumber", "10"))));
  }
}
