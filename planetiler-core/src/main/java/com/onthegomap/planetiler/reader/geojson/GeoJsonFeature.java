package com.onthegomap.planetiler.reader.geojson;

import com.onthegomap.planetiler.reader.WithTags;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;

/**
 * Feature read from a geojson document.
 *
 * @param geometry The parsed JTS geometry from {@code geometry} field, or an empty geometry if it was invalid
 * @param tags     The parsed map from {@code properties} field, or empty map if properties are missing
 */
public record GeoJsonFeature(Geometry geometry, @Override Map<String, Object> tags)
  implements WithTags {}
