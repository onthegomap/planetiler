package com.onthegomap.flatmap.openmaptiles.layers;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

public class AerodromeLabelTest extends AbstractLayerTest {


  @TestFactory
  public List<DynamicTest> aerodromeLabel() {
    wikidataTranslations.put(123, "es", "es wd name");
    return List.of(
      dynamicTest("happy path point", () -> assertFeatures(14, List.of(Map.of(
        "class", "international",
        "ele", 100,
        "ele_ft", 328,
        "name", "osm name",
        "name:es", "es wd name",

        "_layer", "aerodrome_label",
        "_type", "point",
        "_minzoom", 10,
        "_maxzoom", 14,
        "_buffer", 64d
      )), process(pointFeature(Map.of(
        "aeroway", "aerodrome",
        "name", "osm name",
        "wikidata", "Q123",
        "ele", "100",
        "aerodrome", "international",
        "iata", "123",
        "icao", "1234"
      ))))),

      dynamicTest("international", () -> assertFeatures(14, List.of(Map.of(
        "class", "international",
        "_layer", "aerodrome_label"
      )), process(pointFeature(Map.of(
        "aeroway", "aerodrome",
        "aerodrome_type", "international"
      ))))),

      dynamicTest("public", () -> {
        assertFeatures(14, List.of(Map.of(
          "class", "public",
          "_layer", "aerodrome_label"
        )), process(pointFeature(Map.of(
          "aeroway", "aerodrome",
          "aerodrome_type", "public airport"
        ))));
        assertFeatures(14, List.of(Map.of(
          "class", "public",
          "_layer", "aerodrome_label"
        )), process(pointFeature(Map.of(
          "aeroway", "aerodrome",
          "aerodrome_type", "civil"
        ))));
      }),

      dynamicTest("military", () -> {
        assertFeatures(14, List.of(Map.of(
          "class", "military",
          "_layer", "aerodrome_label"
        )), process(pointFeature(Map.of(
          "aeroway", "aerodrome",
          "aerodrome_type", "military airport"
        ))));
        assertFeatures(14, List.of(Map.of(
          "class", "military",
          "_layer", "aerodrome_label"
        )), process(pointFeature(Map.of(
          "aeroway", "aerodrome",
          "military", "airfield"
        ))));
      }),

      dynamicTest("private", () -> {
        assertFeatures(14, List.of(Map.of(
          "class", "private",
          "_layer", "aerodrome_label"
        )), process(pointFeature(Map.of(
          "aeroway", "aerodrome",
          "aerodrome_type", "private"
        ))));
        assertFeatures(14, List.of(Map.of(
          "class", "private",
          "_layer", "aerodrome_label"
        )), process(pointFeature(Map.of(
          "aeroway", "aerodrome",
          "aerodrome", "private"
        ))));
      }),

      dynamicTest("other", () -> assertFeatures(14, List.of(Map.of(
        "class", "other",
        "_layer", "aerodrome_label"
      )), process(pointFeature(Map.of(
        "aeroway", "aerodrome"
      ))))),

      dynamicTest("ignore non-points", () -> assertFeatures(14, List.of(), process(lineFeature(Map.of(
        "aeroway", "aerodrome"
      )))))
    );
  }
}
