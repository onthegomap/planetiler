package com.onthegomap.flatmap.basemap.util;

import com.onthegomap.flatmap.mbtiles.Mbtiles;
import com.onthegomap.flatmap.mbtiles.Verify;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * A utility to check the contents of an mbtiles file generated for Monaco.
 */
public class VerifyMonaco {

  public static final Envelope MONACO_BOUNDS = new Envelope(7.40921, 7.44864, 43.72335, 43.75169);

  /**
   * Returns a verification result with a basic set of checks against an openmaptiles map built from an extract for
   * Monaco.
   */
  public static Verify verify(Mbtiles mbtiles) {
    Verify verify = Verify.verify(mbtiles);
    verify.checkMinFeatureCount(MONACO_BOUNDS, "building", Map.of(), 13, 14, 100, Polygon.class);
    verify.checkMinFeatureCount(MONACO_BOUNDS, "transportation", Map.of(), 10, 14, 5, LineString.class);
    verify.checkMinFeatureCount(MONACO_BOUNDS, "landcover", Map.of(
      "class", "grass",
      "subclass", "park"
    ), 14, 10, Polygon.class);
    verify.checkMinFeatureCount(MONACO_BOUNDS, "water", Map.of("class", "ocean"), 0, 14, 1, Polygon.class);
    verify.checkMinFeatureCount(MONACO_BOUNDS, "place", Map.of("class", "country"), 2, 14, 1, Point.class);
    return verify;
  }

  public static void main(String[] args) throws IOException {
    try (var mbtiles = Mbtiles.newReadOnlyDatabase(Path.of(args[0]))) {
      var result = verify(mbtiles);
      result.print();
      result.failIfErrors();
    }
  }
}
