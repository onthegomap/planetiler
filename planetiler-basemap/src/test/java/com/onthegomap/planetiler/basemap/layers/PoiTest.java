package com.onthegomap.planetiler.basemap.layers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class PoiTest extends AbstractLayerTest {

  private SourceFeature feature(boolean area, Map<String, Object> tags) {
    return area ? polygonFeature(tags) : pointFeature(tags);
  }

  @Test
  public void testFenwayPark() {
    assertFeatures(
        7,
        List.of(
            Map.of(
                "_layer", "poi",
                "class", "stadium",
                "subclass", "stadium",
                "name", "Fenway Park",
                "rank", "<null>",
                "_minzoom", 14,
                "_labelgrid_size", 64d)),
        process(
            pointFeature(
                Map.of(
                    "leisure", "stadium",
                    "name", "Fenway Park"))));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testFunicularHalt(boolean area) {
    assertFeatures(
        7,
        List.of(
            Map.of(
                "_layer", "poi",
                "class", "railway",
                "subclass", "halt",
                "rank", "<null>",
                "_minzoom", 12)),
        process(
            feature(
                area,
                Map.of(
                    "railway", "station",
                    "funicular", "yes",
                    "name", "station"))));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testSubway(boolean area) {
    assertFeatures(
        7,
        List.of(
            Map.of(
                "_layer", "poi",
                "class", "railway",
                "subclass", "subway",
                "rank", "<null>",
                "_minzoom", 12)),
        process(
            feature(
                area,
                Map.of(
                    "railway", "station",
                    "station", "subway",
                    "name", "station"))));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testPlaceOfWorshipFromReligionTag(boolean area) {
    assertFeatures(
        7,
        List.of(
            Map.of(
                "_layer", "poi",
                "class", "place_of_worship",
                "subclass", "religion value",
                "rank", "<null>",
                "_minzoom", 14)),
        process(
            feature(
                area,
                Map.of(
                    "amenity", "place_of_worship",
                    "religion", "religion value",
                    "name", "station"))));
  }

  @Test
  public void testPitchFromSportTag() {
    assertFeatures(
        7,
        List.of(
            Map.of(
                "_layer", "poi",
                "class", "pitch",
                "subclass", "soccer",
                "rank", "<null>")),
        process(
            pointFeature(
                Map.of(
                    "leisure", "pitch",
                    "sport", "soccer",
                    "name", "station"))));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testInformation(boolean area) {
    assertFeatures(
        7,
        List.of(
            Map.of(
                "_layer", "poi",
                "class", "information",
                "subclass", "infotype",
                "layer", 3L,
                "level", 2L,
                "indoor", 1,
                "rank", "<null>")),
        process(
            feature(
                area,
                Map.of(
                    "tourism", "information",
                    "information", "infotype",
                    "name", "station",
                    "layer", "3",
                    "level", "2",
                    "indoor", "yes"))));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testFerryTerminal(boolean area) {
    assertFeatures(
        7,
        List.of(
            Map.of(
                "_layer", "poi",
                "class", "ferry_terminal",
                "subclass", "ferry_terminal",
                "name", "Water Taxi",
                "_minzoom", 12)),
        process(
            feature(
                area,
                Map.of(
                    "amenity", "ferry_terminal",
                    "information", "infotype",
                    "name", "Water Taxi",
                    "layer", "3",
                    "level", "2",
                    "indoor", "yes"))));
  }

  @Test
  public void testGridRank() throws GeometryException {
    var layerName = Poi.LAYER_NAME;
    assertEquals(List.of(), profile.postProcessLayerFeatures(layerName, 13, List.of()));

    assertEquals(
        List.of(pointFeature(layerName, Map.of("rank", 1), 1)),
        profile.postProcessLayerFeatures(
            layerName, 14, List.of(pointFeature(layerName, Map.of(), 1))));

    assertEquals(
        List.of(
            pointFeature(layerName, Map.of("rank", 1, "name", "a"), 1),
            pointFeature(layerName, Map.of("rank", 2, "name", "b"), 1),
            pointFeature(layerName, Map.of("rank", 1, "name", "c"), 2)),
        profile.postProcessLayerFeatures(
            layerName,
            14,
            List.of(
                pointFeature(layerName, Map.of("name", "a"), 1),
                pointFeature(layerName, Map.of("name", "b"), 1),
                pointFeature(layerName, Map.of("name", "c"), 2))));
  }

  @Test
  public void testEmbassy() {
    assertFeatures(
        7,
        List.of(
            Map.of(
                "_layer", "poi",
                "class", "diplomatic",
                "subclass", "diplomatic",
                "name", "The Embassy")),
        process(
            pointFeature(
                Map.of(
                    "office", "diplomatic",
                    "name", "The Embassy"))));
  }

  @Test
  public void testLocksmith() {
    assertFeatures(
        7,
        List.of(
            Map.of(
                "_layer", "poi",
                "class", "shop",
                "subclass", "locksmith",
                "name", "The Locksmith")),
        process(
            pointFeature(
                Map.of(
                    "shop", "locksmith",
                    "name", "The Locksmith"))));
  }
}
