package com.onthegomap.planetiler.basemap.layers;

import static com.onthegomap.planetiler.TestUtils.rectangle;
import static com.onthegomap.planetiler.basemap.BasemapProfile.NATURAL_EARTH_SOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SimpleFeature;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class LandcoverTest extends AbstractLayerTest {

  @Test
  public void testNaturalEarthGlaciers() {
    var glacier1 = process(SimpleFeature.create(
      GeoUtils.worldToLatLonCoords(rectangle(0, Math.sqrt(1))),
      Map.of(),
      NATURAL_EARTH_SOURCE,
      "ne_110m_glaciated_areas",
      0
    ));
    var glacier2 = process(SimpleFeature.create(
      GeoUtils.worldToLatLonCoords(rectangle(0, Math.sqrt(1))),
      Map.of(),
      NATURAL_EARTH_SOURCE,
      "ne_50m_glaciated_areas",
      0
    ));
    var glacier3 = process(SimpleFeature.create(
      GeoUtils.worldToLatLonCoords(rectangle(0, Math.sqrt(1))),
      Map.of(),
      NATURAL_EARTH_SOURCE,
      "ne_10m_glaciated_areas",
      0
    ));
    assertFeatures(0, List.of(Map.of(
      "_layer", "landcover",
      "subclass", "glacier",
      "class", "ice",
      "_buffer", 4d
    )), glacier1);
    assertFeatures(0, List.of(Map.of(
      "_layer", "landcover",
      "subclass", "glacier",
      "class", "ice",
      "_buffer", 4d
    )), glacier2);
    assertFeatures(0, List.of(Map.of(
      "_layer", "landcover",
      "subclass", "glacier",
      "class", "ice",
      "_buffer", 4d
    )), glacier3);
    assertCoversZoomRange(0, 6, "landcover",
      glacier1,
      glacier2,
      glacier3
    );
  }

  @Test
  public void testNaturalEarthAntarcticIceShelves() {
    var ice1 = process(SimpleFeature.create(
      GeoUtils.worldToLatLonCoords(rectangle(0, Math.sqrt(1))),
      Map.of(),
      NATURAL_EARTH_SOURCE,
      "ne_50m_antarctic_ice_shelves_polys",
      0
    ));
    var ice2 = process(SimpleFeature.create(
      GeoUtils.worldToLatLonCoords(rectangle(0, Math.sqrt(1))),
      Map.of(),
      NATURAL_EARTH_SOURCE,
      "ne_10m_antarctic_ice_shelves_polys",
      0
    ));
    assertFeatures(0, List.of(Map.of(
      "_layer", "landcover",
      "subclass", "ice_shelf",
      "class", "ice",
      "_buffer", 4d
    )), ice1);
    assertFeatures(0, List.of(Map.of(
      "_layer", "landcover",
      "subclass", "ice_shelf",
      "class", "ice",
      "_buffer", 4d
    )), ice2);
    assertCoversZoomRange(2, 6, "landcover",
      ice1,
      ice2
    );
  }

  @Test
  public void testOsmLandcover() {
    assertFeatures(13, List.of(Map.of(
      "_layer", "landcover",
      "subclass", "wood",
      "class", "wood",
      "_minpixelsize", 8d,
      "_numpointsattr", "_numpoints",
      "_minzoom", 9,
      "_maxzoom", 14
    )), process(polygonFeature(Map.of(
      "natural", "wood"
    ))));
    assertFeatures(12, List.of(Map.of(
      "_layer", "landcover",
      "subclass", "forest",
      "class", "wood",
      "_minpixelsize", 8d,
      "_minzoom", 9,
      "_maxzoom", 14
    )), process(polygonFeature(Map.of(
      "landuse", "forest"
    ))));
    assertFeatures(10, List.of(Map.of(
      "_layer", "landcover",
      "subclass", "dune",
      "class", "sand",
      "_minpixelsize", 4d,
      "_minzoom", 7,
      "_maxzoom", 14
    )), process(polygonFeature(Map.of(
      "natural", "dune"
    ))));
    assertFeatures(10, List.of(), process(polygonFeature(Map.of(
      "landuse", "park"
    ))));
  }

  @Test
  public void testMergeForestsBuNumPointsZ9to13() throws GeometryException {
    Map<String, Object> map = Map.of("subclass", "wood");

    assertMerges(List.of(map, map, map, map, map, map), List.of(
      // don't merge any
      feature(rectangle(10, 20), Map.of("_numpoints", 48, "subclass", "wood")),
      feature(rectangle(10, 20), Map.of("_numpoints", 49, "subclass", "wood")),
      feature(rectangle(12, 18), Map.of("_numpoints", 50, "subclass", "wood")),
      feature(rectangle(12, 18), Map.of("_numpoints", 299, "subclass", "wood")),
      feature(rectangle(12, 18), Map.of("_numpoints", 300, "subclass", "wood")),
      feature(rectangle(12, 18), Map.of("_numpoints", 301, "subclass", "wood"))
    ), 14);
    assertMerges(List.of(map, map, map), List.of(
      // < 300 - merge
      feature(rectangle(10, 20), Map.of("_numpoints", 48, "subclass", "wood")),
      feature(rectangle(10, 20), Map.of("_numpoints", 49, "subclass", "wood")),
      feature(rectangle(12, 18), Map.of("_numpoints", 50, "subclass", "wood")),
      feature(rectangle(12, 18), Map.of("_numpoints", 299, "subclass", "wood")),
      // >= 300 - don't merge
      feature(rectangle(12, 18), Map.of("_numpoints", 300, "subclass", "wood")),
      feature(rectangle(12, 18), Map.of("_numpoints", 301, "subclass", "wood"))
    ), 13);
    assertMerges(List.of(map, map), List.of(
      // < 300 - merge
      feature(rectangle(10, 20), Map.of("_numpoints", 48, "subclass", "wood")),
      feature(rectangle(10, 20), Map.of("_numpoints", 49, "subclass", "wood")),
      feature(rectangle(12, 18), Map.of("_numpoints", 50, "subclass", "wood")),
      feature(rectangle(12, 18), Map.of("_numpoints", 299, "subclass", "wood")),
      // >= 300 - merge
      feature(rectangle(12, 18), Map.of("_numpoints", 300, "subclass", "wood")),
      feature(rectangle(12, 18), Map.of("_numpoints", 301, "subclass", "wood"))
    ), 9);
  }

  @Test
  public void testMergeNonForestsBelowZ9() throws GeometryException {
    Map<String, Object> map = Map.of("subclass", "dune");

    assertMerges(List.of(map, map), List.of(
      feature(rectangle(10, 20), Map.of("_numpoints", 48, "subclass", "dune")),
      feature(rectangle(12, 18), Map.of("_numpoints", 301, "subclass", "dune"))
    ), 9);
    assertMerges(List.of(map), List.of(
      feature(rectangle(10, 20), Map.of("_numpoints", 48, "subclass", "dune")),
      feature(rectangle(12, 18), Map.of("_numpoints", 301, "subclass", "dune"))
    ), 8);
    assertMerges(List.of(map, map), List.of(
      feature(rectangle(10, 20), Map.of("_numpoints", 48, "subclass", "dune")),
      feature(rectangle(12, 18), Map.of("_numpoints", 301, "subclass", "dune"))
    ), 6);
  }


  private VectorTile.Feature feature(org.locationtech.jts.geom.Polygon geom, Map<String, Object> m) {
    return new VectorTile.Feature(
      "landcover",
      1,
      VectorTile.encodeGeometry(geom),
      new HashMap<>(m),
      0
    );
  }

  private void assertMerges(List<Map<String, Object>> expected, List<VectorTile.Feature> in, int zoom)
    throws GeometryException {
    assertEquals(expected,
      profile.postProcessLayerFeatures("landcover", zoom, in).stream().map(
          VectorTile.Feature::attrs)
        .toList());
  }
}
