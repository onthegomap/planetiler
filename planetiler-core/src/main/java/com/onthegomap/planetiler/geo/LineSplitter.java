package com.onthegomap.planetiler.geo;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;

/**
 * Utility for extracting sub-ranges of a line.
 * <p>
 * For example:
 * {@snippet :
 * LineSplitter splitter = new LineSplitter(line);
 * LineString firstHalf = splitter.get(0, 0.5);
 * LineString lastQuarter = splitter.get(0.75, 1);
 * }
 */
public class LineSplitter {

  private final LineString line;
  private double length = 0;
  private TreeMap<Double, Integer> nodeLocations = null;
  private final Map<Range, LineString> cache = new HashMap<>();

  private record Range(double lo, double hi) {}

  public LineSplitter(Geometry geom) {
    if (geom instanceof LineString line) {
      this.line = line;
    } else {
      throw new IllegalArgumentException("Expected LineString, got " + geom.getGeometryType());
    }
  }

  /**
   * Returns a partial segment of this line from {@code start} to {@code end} where 0 is the beginning of the line and 1
   * is the end.
   */
  public LineString get(double start, double end) {
    if (start < 0d || end > 1d || end < start) {
      throw new IllegalArgumentException("Invalid range: " + start + " to " + end);
    }
    if (start <= 0 && end >= 1) {
      return line;
    }
    var key = new Range(start, end);
    if (cache.containsKey(key)) {
      return cache.get(key);
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

    var line = GeoUtils.JTS_FACTORY.createLineString(result);
    cache.put(key, line);
    return line;
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
