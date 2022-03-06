package com.onthegomap.planetiler.basemap.layers;

import com.onthegomap.planetiler.geo.GeoUtils;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ParkTest extends AbstractLayerTest {

  @Test
  public void testNationalPark() {
    assertFeatures(
        13,
        List.of(
            Map.of(
                "_layer", "park",
                "_type", "polygon",
                "class", "national_park",
                "name", "Grand Canyon National Park",
                "_minpixelsize", 2d,
                "_minzoom", 4,
                "_maxzoom", 14),
            Map.of(
                "_layer", "park",
                "_type", "point",
                "class", "national_park",
                "name", "Grand Canyon National Park",
                "name_int", "Grand Canyon National Park",
                "name:latin", "Grand Canyon National Park",
                //      "name:es", "es name", // don't include all translations
                "_minzoom", 5,
                "_maxzoom", 14)),
        process(
            polygonFeature(
                Map.of(
                    "boundary", "national_park",
                    "name", "Grand Canyon National Park",
                    "name:es", "es name",
                    "protection_title", "National Park",
                    "wikipedia", "en:Grand Canyon National Park"))));

    // needs a name
    assertFeatures(
        13,
        List.of(
            Map.of(
                "_layer", "park",
                "_type", "polygon")),
        process(
            polygonFeature(
                Map.of(
                    "boundary", "national_park",
                    "protection_title", "National Park"))));
  }

  @Test
  public void testSmallerPark() {
    double z11area =
        Math.pow((GeoUtils.metersToPixelAtEquator(0, Math.sqrt(70_000)) / 256d), 2)
            * Math.pow(2, 20 - 11);
    assertFeatures(
        13,
        List.of(
            Map.of(
                "_layer", "park",
                "_type", "polygon",
                "class", "protected_area",
                "name", "Small park",
                "_minpixelsize", 2d,
                "_minzoom", 4,
                "_maxzoom", 14),
            Map.of(
                "_layer", "park",
                "_type", "point",
                "class", "protected_area",
                "name", "Small park",
                "name_int", "Small park",
                "_minzoom", 11,
                "_maxzoom", 14)),
        process(
            polygonFeatureWithArea(
                z11area,
                Map.of(
                    "boundary", "protected_area",
                    "name", "Small park",
                    "wikipedia", "en:Small park"))));
    assertFeatures(
        13,
        List.of(
            Map.of(
                "_layer", "park",
                "_type", "polygon"),
            Map.of("_layer", "park", "_type", "point", "_minzoom", 5, "_maxzoom", 14)),
        process(
            polygonFeatureWithArea(
                1,
                Map.of(
                    "boundary", "protected_area",
                    "name", "Small park",
                    "wikidata", "Q123"))));
  }

  @Test
  public void testSortKeys() {
    assertAscending(
        getLabelSortKey(
            1,
            Map.of(
                "boundary", "national_park",
                "name", "a",
                "wikipedia", "en:park")),
        getLabelSortKey(
            1e-10,
            Map.of(
                "boundary", "national_park",
                "name", "a",
                "wikipedia", "en:Park")),
        getLabelSortKey(
            1,
            Map.of(
                "boundary", "national_park",
                "name", "a")),
        getLabelSortKey(
            1e-10,
            Map.of(
                "boundary", "national_park",
                "name", "a")),
        getLabelSortKey(
            1,
            Map.of(
                "boundary", "protected_area",
                "name", "a",
                "wikipedia", "en:park")),
        getLabelSortKey(
            1e-10,
            Map.of(
                "boundary", "protected_area",
                "name", "a",
                "wikipedia", "en:Park")),
        getLabelSortKey(
            1,
            Map.of(
                "boundary", "protected_area",
                "name", "a")),
        getLabelSortKey(
            1e-10,
            Map.of(
                "boundary", "protected_area",
                "name", "a")));
  }

  private int getLabelSortKey(double area, Map<String, Object> tags) {
    var iter = process(polygonFeatureWithArea(area, tags)).iterator();
    iter.next();
    return iter.next().getSortKey();
  }
}
