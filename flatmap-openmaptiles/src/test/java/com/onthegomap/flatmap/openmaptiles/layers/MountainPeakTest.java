package com.onthegomap.flatmap.openmaptiles.layers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.onthegomap.flatmap.geo.GeometryException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

public class MountainPeakTest extends AbstractLayerTest {

  @TestFactory
  public List<DynamicTest> mountainPeakProcessing() {
    wikidataTranslations.put(123, "es", "es wd name");
    return List.of(
      dynamicTest("happy path", () -> {
        var peak = process(pointFeature(Map.of(
          "natural", "peak",
          "name", "test",
          "ele", "100",
          "wikidata", "Q123"
        )));
        assertFeatures(14, List.of(Map.of(
          "class", "peak",
          "ele", 100,
          "ele_ft", 328,

          "_layer", "mountain_peak",
          "_type", "point",
          "_minzoom", 7,
          "_maxzoom", 14,
          "_buffer", 64d
        )), peak);
        assertFeatures(14, List.of(Map.of(
          "name:latin", "test",
          "name", "test",
          "name:es", "es wd name"
        )), peak);
      }),

      dynamicTest("labelgrid", () -> {
        var peak = process(pointFeature(Map.of(
          "natural", "peak",
          "ele", "100"
        )));
        assertFeatures(14, List.of(Map.of(
          "_labelgrid_limit", 0
        )), peak);
        assertFeatures(13, List.of(Map.of(
          "_labelgrid_limit", 5,
          "_labelgrid_size", 100d
        )), peak);
      }),

      dynamicTest("volcano", () ->
        assertFeatures(14, List.of(Map.of(
          "class", "volcano"
        )), process(pointFeature(Map.of(
          "natural", "volcano",
          "ele", "100"
        ))))),

      dynamicTest("no elevation", () ->
        assertFeatures(14, List.of(), process(pointFeature(Map.of(
          "natural", "volcano"
        ))))),

      dynamicTest("bogus elevation", () ->
        assertFeatures(14, List.of(), process(pointFeature(Map.of(
          "natural", "volcano",
          "ele", "11000"
        ))))),

      dynamicTest("ignore lines", () ->
        assertFeatures(14, List.of(), process(lineFeature(Map.of(
          "natural", "peak",
          "name", "name",
          "ele", "100"
        ))))),

      dynamicTest("zorder", () -> {
        assertFeatures(14, List.of(Map.of(
          "_zorder", 100
        )), process(pointFeature(Map.of(
          "natural", "peak",
          "ele", "100"
        ))));
        assertFeatures(14, List.of(Map.of(
          "_zorder", 10100
        )), process(pointFeature(Map.of(
          "natural", "peak",
          "name", "name",
          "ele", "100"
        ))));
        assertFeatures(14, List.of(Map.of(
          "_zorder", 20100
        )), process(pointFeature(Map.of(
          "natural", "peak",
          "name", "name",
          "wikipedia", "wikilink",
          "ele", "100"
        ))));
      })
    );
  }

  @Test
  public void testMountainPeakPostProcessing() throws GeometryException {
    assertEquals(List.of(), profile.postProcessLayerFeatures(MountainPeak.LAYER_NAME, 13, List.of()));

    assertEquals(List.of(pointFeature(
      MountainPeak.LAYER_NAME,
      Map.of("rank", 1),
      1
    )), profile.postProcessLayerFeatures(MountainPeak.LAYER_NAME, 13, List.of(pointFeature(
      MountainPeak.LAYER_NAME,
      Map.of(),
      1
    ))));

    assertEquals(List.of(
      pointFeature(
        MountainPeak.LAYER_NAME,
        Map.of("rank", 2, "name", "a"),
        1
      ), pointFeature(
        MountainPeak.LAYER_NAME,
        Map.of("rank", 1, "name", "b"),
        1
      ), pointFeature(
        MountainPeak.LAYER_NAME,
        Map.of("rank", 1, "name", "c"),
        2
      )
    ), profile.postProcessLayerFeatures(MountainPeak.LAYER_NAME, 13, List.of(
      pointFeature(
        MountainPeak.LAYER_NAME,
        Map.of("name", "a"),
        1
      ),
      pointFeature(
        MountainPeak.LAYER_NAME,
        Map.of("name", "b"),
        1
      ),
      pointFeature(
        MountainPeak.LAYER_NAME,
        Map.of("name", "c"),
        2
      )
    )));
  }
}
