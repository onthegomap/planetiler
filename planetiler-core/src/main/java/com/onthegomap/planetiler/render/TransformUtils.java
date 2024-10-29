package com.onthegomap.planetiler.render;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.AffineTransformation;

public class TransformUtils {

  public static AffineTransformation transform = new AffineTransformation();

  static {
    transform.scale(1.0 / 4096, 1.0 / 4096);
  }

  public static Geometry transform(Geometry geom) {
    return transform.transform(geom);
  }
}
