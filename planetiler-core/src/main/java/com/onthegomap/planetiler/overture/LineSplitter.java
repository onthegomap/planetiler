package com.onthegomap.planetiler.overture;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.MutableCoordinateSequence;
import java.util.Map;
import java.util.TreeMap;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;

public class LineSplitter {

  private final LineString line;
  private double length = 0;
  private TreeMap<Double, Integer> nodeLocations = null;

  LineSplitter(Geometry geom) {
    if (geom instanceof LineString line) {
      this.line = line;
    } else {
      throw new IllegalArgumentException("Expected LineString, got " + geom.getGeometryType());
    }
  }

  public LineString get(double start, double end) {
    if (start < 0d || end > 1d || end < start) {
      throw new IllegalArgumentException("Invalid range: " + start + " to " + end);
    }
    if (start <= 0 && end >= 1) {
      return line;
    }
    var cs = line.getCoordinateSequence();
    if (nodeLocations == null) {
      nodeLocations = new TreeMap<>();
      double x1 = cs.getX(0);
      double y1 = cs.getY(0);
      nodeLocations.put(0d, 0);
      for (int i = 1; i < cs.size(); i++) {
        double x2 = cs.getX(i);
        double y2 = cs.getY(i);
        double dx = x2 - x1;
        double dy = y2 - y1;
        length += Math.sqrt(dx * dx + dy * dy);
        nodeLocations.put(length, i);
        x1 = x2;
        y1 = y2;
      }
    }
    MutableCoordinateSequence result = new MutableCoordinateSequence();
    var first = nodeLocations.floorEntry(start * length);
    var last = nodeLocations.lowerEntry(end * length);
    addInterpolated(result, cs, first, start * length);
    for (int i = first.getValue() + 1; i <= last.getValue(); i++) {
      result.addPoint(cs.getX(i), cs.getY(i));
    }
    addInterpolated(result, cs, last, end * length);

    return GeoUtils.JTS_FACTORY.createLineString(result);
  }

  private void addInterpolated(MutableCoordinateSequence result, CoordinateSequence cs,
    Map.Entry<Double, Integer> first, double position) {
    double startPos = first.getKey();
    double endPos = nodeLocations.higherKey(startPos);
    int startIdx = first.getValue();
    int endIdx = startIdx + 1;
    double x1 = cs.getX(startIdx);
    double y1 = cs.getY(startIdx);
    double x2 = cs.getX(endIdx);
    double y2 = cs.getY(endIdx);
    double ratio = (position - startPos) / (endPos - startPos);
    result.addPoint(
      x1 + (x2 - x1) * ratio,
      y1 + (y2 - y1) * ratio
    );
  }
}
