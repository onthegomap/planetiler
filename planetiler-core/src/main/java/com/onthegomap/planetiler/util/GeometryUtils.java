package com.onthegomap.planetiler.util;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.util.AffineTransformation;

public class GeometryUtils {

  public static String toGeoJSON(List<Geometry> list) {
    AffineTransformation transform = new AffineTransformation();
    double scale = 1.0 / 4096;
    List<Geometry> collect = list.stream().map(g -> transform.scale(scale, scale).transform(g))
      .toList();
    GeometryCollection geometryCollection = new GeometryCollection(collect.toArray(new Geometry[0]),
      new GeometryFactory());
    StringWriter writer = new StringWriter();
    GeometryJSON g = new GeometryJSON();
    try {
      g.write(geometryCollection, writer);
    } catch (IOException e) {
      e.printStackTrace();
    }
    String geoJson = writer.toString();
    return geoJson;
  }
}
