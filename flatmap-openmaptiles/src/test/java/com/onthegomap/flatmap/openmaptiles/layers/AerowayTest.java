package com.onthegomap.flatmap.openmaptiles.layers;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class AerowayTest extends AbstractLayerTest {

  @Test
  public void aerowayGate() {
    assertFeatures(14, List.of(Map.of(
      "class", "gate",
      "ref", "123",

      "_layer", "aeroway",
      "_type", "point",
      "_minzoom", 14,
      "_maxzoom", 14,
      "_buffer", 4d
    )), process(pointFeature(Map.of(
      "aeroway", "gate",
      "ref", "123"
    ))));
    assertFeatures(14, List.of(), process(lineFeature(Map.of(
      "aeroway", "gate"
    ))));
    assertFeatures(14, List.of(), process(polygonFeature(Map.of(
      "aeroway", "gate"
    ))));
  }

  @Test
  public void aerowayLine() {
    assertFeatures(14, List.of(Map.of(
      "class", "runway",
      "ref", "123",

      "_layer", "aeroway",
      "_type", "line",
      "_minzoom", 10,
      "_maxzoom", 14,
      "_buffer", 4d
    )), process(lineFeature(Map.of(
      "aeroway", "runway",
      "ref", "123"
    ))));
    assertFeatures(14, List.of(), process(pointFeature(Map.of(
      "aeroway", "runway"
    ))));
  }

  @Test
  public void aerowayPolygon() {
    assertFeatures(14, List.of(Map.of(
      "class", "runway",
      "ref", "123",

      "_layer", "aeroway",
      "_type", "polygon",
      "_minzoom", 10,
      "_maxzoom", 14,
      "_buffer", 4d
    )), process(polygonFeature(Map.of(
      "aeroway", "runway",
      "ref", "123"
    ))));
    assertFeatures(14, List.of(Map.of(
      "class", "runway",
      "ref", "123",
      "_layer", "aeroway",
      "_type", "polygon"
    )), process(polygonFeature(Map.of(
      "area:aeroway", "runway",
      "ref", "123"
    ))));
    assertFeatures(14, List.of(Map.of(
      "class", "heliport",
      "ref", "123",
      "_layer", "aeroway",
      "_type", "polygon"
    )), process(polygonFeature(Map.of(
      "aeroway", "heliport",
      "ref", "123"
    ))));
    assertFeatures(14, List.of(), process(lineFeature(Map.of(
      "aeroway", "heliport"
    ))));
    assertFeatures(14, List.of(), process(pointFeature(Map.of(
      "aeroway", "heliport"
    ))));
  }
}
