package com.onthegomap.planetiler.basemap.layers;

import static com.onthegomap.planetiler.TestUtils.newPoint;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.geo.GeometryException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MountainPeakTest extends AbstractLayerTest {

  @BeforeEach
  public void setupWikidataTranslation() {
    wikidataTranslations.put(123, "es", "es wd name");
  }

  @Test
  public void testHappyPath() {
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
      "_buffer", 100d
    )), peak);
    assertFeatures(14, List.of(Map.of(
      "name:latin", "test",
      "name", "test",
      "name:es", "es wd name"
    )), peak);
  }

  @Test
  public void testLabelGrid() {
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
  }

  @Test
  public void testVolcano() {
    assertFeatures(14, List.of(Map.of(
      "class", "volcano"
    )), process(pointFeature(Map.of(
      "natural", "volcano",
      "ele", "100"
    ))));
  }

  @Test
  public void testNoElevation() {
    assertFeatures(14, List.of(), process(pointFeature(Map.of(
      "natural", "volcano"
    ))));
  }

  @Test
  public void testBogusElevation() {
    assertFeatures(14, List.of(), process(pointFeature(Map.of(
      "natural", "volcano",
      "ele", "11000"
    ))));
  }

  @Test
  public void testIgnoreLines() {
    assertFeatures(14, List.of(), process(lineFeature(Map.of(
      "natural", "peak",
      "name", "name",
      "ele", "100"
    ))));
  }

  private int getSortKey(Map<String, Object> tags) {
    return process(pointFeature(Map.of(
      "natural", "peak",
      "ele", "100"
    ))).iterator().next().getSortKey();
  }

  @Test
  public void testSortKey() {
    assertAscending(
      getSortKey(Map.of(
        "natural", "peak",
        "name", "name",
        "wikipedia", "wikilink",
        "ele", "100"
      )),
      getSortKey(Map.of(
        "natural", "peak",
        "name", "name",
        "ele", "100"
      )),
      getSortKey(Map.of(
        "natural", "peak",
        "ele", "100"
      ))
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
        Map.of("rank", 1, "name", "a"),
        1
      ), pointFeature(
        MountainPeak.LAYER_NAME,
        Map.of("rank", 2, "name", "b"),
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

  @Test
  public void testMountainPeakPostProcessingLimitsFeaturesOutsideZoom() throws GeometryException {
    assertEquals(Lists.newArrayList(
      new VectorTile.Feature(
        MountainPeak.LAYER_NAME,
        1,
        VectorTile.encodeGeometry(newPoint(-64, -64)),
        Map.of("rank", 1),
        1
      ),
      null,
      new VectorTile.Feature(
        MountainPeak.LAYER_NAME,
        3,
        VectorTile.encodeGeometry(newPoint(256 + 64, 256 + 64)),
        Map.of("rank", 1),
        2
      ),
      null
    ), profile.postProcessLayerFeatures(MountainPeak.LAYER_NAME, 13, Lists.newArrayList(
      new VectorTile.Feature(
        MountainPeak.LAYER_NAME,
        1,
        VectorTile.encodeGeometry(newPoint(-64, -64)),
        new HashMap<>(),
        1
      ),
      new VectorTile.Feature(
        MountainPeak.LAYER_NAME,
        2,
        VectorTile.encodeGeometry(newPoint(-65, -65)),
        new HashMap<>(),
        1
      ),
      new VectorTile.Feature(
        MountainPeak.LAYER_NAME,
        3,
        VectorTile.encodeGeometry(newPoint(256 + 64, 256 + 64)),
        new HashMap<>(),
        2
      ),
      new VectorTile.Feature(
        MountainPeak.LAYER_NAME,
        4,
        VectorTile.encodeGeometry(newPoint(256 + 65, 256 + 65)),
        new HashMap<>(),
        2
      )
    )));
  }
}
