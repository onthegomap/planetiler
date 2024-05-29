package com.onthegomap.planetiler.geo;

import java.util.Arrays;
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
  private double[] nodeLocations = null;

  public LineSplitter(Geometry geom) {
    if (geom instanceof LineString linestring) {
      this.line = linestring;
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
    var cs = line.getCoordinateSequence();
    if (nodeLocations == null) {
      nodeLocations = new double[cs.size()];
      double x1 = cs.getX(0);
      double y1 = cs.getY(0);
      nodeLocations[0] = 0d;
      for (int i = 1; i < cs.size(); i++) {
        double x2 = cs.getX(i);
        double y2 = cs.getY(i);
        double dx = x2 - x1;
        double dy = y2 - y1;
        length += Math.sqrt(dx * dx + dy * dy);
        nodeLocations[i] = length;
        x1 = x2;
        y1 = y2;
      }
    }
    MutableCoordinateSequence result = new MutableCoordinateSequence();

    double startPos = start * length;
    double endPos = end * length;
    var first = floorIndex(startPos);
    var last = lowerIndex(endPos);
    addInterpolated(result, cs, first, startPos);
    for (int i = first + 1; i <= last; i++) {
      result.addPoint(cs.getX(i), cs.getY(i));
    }
    addInterpolated(result, cs, last, endPos);

    return GeoUtils.JTS_FACTORY.createLineString(result);
  }

  private int floorIndex(double length) {
    int idx = Arrays.binarySearch(nodeLocations, length);
    return idx < 0 ? (-idx - 2) : idx;
  }

  private int lowerIndex(double length) {
    int idx = Arrays.binarySearch(nodeLocations, length);
    return idx < 0 ? (-idx - 2) : idx - 1;
  }

  private void addInterpolated(MutableCoordinateSequence result, CoordinateSequence cs,
    int startIdx, double position) {
    int endIdx = startIdx + 1;
    double startPos = nodeLocations[startIdx];
    double endPos = nodeLocations[endIdx];
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
