package com.onthegomap.flatmap;

import com.onthegomap.flatmap.collections.MutableCoordinateSequence;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.operation.linemerge.LineMerger;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureMerge {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureMerge.class);

  public static List<VectorTileEncoder.Feature> mergeLineStrings(List<VectorTileEncoder.Feature> items,
    double minLength, double tolerance, double clip) throws GeometryException {
    return mergeLineStrings(items, attrs -> minLength, tolerance, clip);
  }

  public static List<VectorTileEncoder.Feature> mergeLineStrings(List<VectorTileEncoder.Feature> features,
    Function<Map<String, Object>, Double> lengthLimitCalculator, double tolerance, double clip)
    throws GeometryException {
    List<VectorTileEncoder.Feature> result = new ArrayList<>(features.size());
    LinkedHashMap<Map<String, Object>, List<VectorTileEncoder.Feature>> groupedByAttrs = new LinkedHashMap<>();
    for (VectorTileEncoder.Feature feature : features) {
      if (feature.geometry().geomType() != GeometryType.LINE) {
        // just ignore and pass through non-linestring features
        result.add(feature);
      } else {
        groupedByAttrs
          .computeIfAbsent(feature.attrs(), k -> new ArrayList<>())
          .add(feature);
      }
    }
    for (var entry : groupedByAttrs.entrySet()) {
      List<VectorTileEncoder.Feature> groupedFeatures = entry.getValue();
      VectorTileEncoder.Feature feature1 = groupedFeatures.get(0);
      double lengthLimit = lengthLimitCalculator.apply(feature1.attrs());

      // as a shortcut, can skip line merging only if there is:
      // - only 1 element in the group
      // - it doesn't need to be clipped
      // - it can't possibly be filtered out for being too short
      if (groupedFeatures.size() == 1 && clip == 0d && lengthLimit == 0) {
        result.add(feature1);
      } else {
        LineMerger merger = new LineMerger();
        for (VectorTileEncoder.Feature feature : groupedFeatures) {
          merger.add(feature.geometry().decode());
        }
        List<LineString> outputSegments = new ArrayList<>();
        for (Object merged : merger.getMergedLineStrings()) {
          if (merged instanceof LineString line && line.getLength() >= lengthLimit) {
            // re-simplify since some endpoints of merged segments may be unnecessary
            if (line.getNumPoints() > 2) {
              DouglasPeuckerSimplifier simplifier = new DouglasPeuckerSimplifier(line);
              simplifier.setDistanceTolerance(tolerance);
              simplifier.setEnsureValid(false);
              Geometry simplified = simplifier.getResultGeometry();
              if (simplified instanceof LineString simpleLineString) {
                line = simpleLineString;
              } else {
                LOGGER.warn("line string merge simplify emitted " + simplified.getGeometryType());
              }
            }
            if (clip > 0) {
              removeDetailOutsideTile(line, clip, outputSegments);
            } else {
              outputSegments.add(line);
            }
          }
        }
        if (outputSegments.size() == 0) {
          // no segments to output - skip this feature
        } else {
          Geometry newGeometry =
            outputSegments.size() == 1 ?
              outputSegments.get(0) :
              GeoUtils.createMultiLineString(outputSegments);
          result.add(feature1.copyWithNewGeometry(newGeometry));
        }
      }
    }
    return result;
  }

  private static void removeDetailOutsideTile(LineString input, double buffer, List<LineString> output) {
    MutableCoordinateSequence current = new MutableCoordinateSequence();
    CoordinateSequence seq = input.getCoordinateSequence();
    boolean wasIn = false;
    double min = -buffer, max = 256 + buffer;
    double x = seq.getX(0), y = seq.getY(0);
    Envelope env = new Envelope();
    Envelope outer = new Envelope(min, max, min, max);
    for (int i = 0; i < seq.size() - 1; i++) {
      double nextX = seq.getX(i + 1), nextY = seq.getY(i + 1);
      env.init(x, nextX, y, nextY);
      boolean nowIn = env.intersects(outer);
      if (nowIn || wasIn) {
        current.addPoint(x, y);
      } else { // out
        // wait to flush until 2 consecutive outs
        if (!current.isEmpty()) {
          output.add(GeoUtils.JTS_FACTORY.createLineString(current));
          current = new MutableCoordinateSequence();
        }
      }
      wasIn = nowIn;
      x = nextX;
      y = nextY;
    }
    double lastX = seq.getX(seq.size() - 1), lastY = seq.getY(seq.size() - 1);
    env.init(x, lastX, y, lastY);
    if (env.intersects(outer) || wasIn) {
      current.addPoint(lastX, lastY);
    }

    if (!current.isEmpty()) {
      output.add(GeoUtils.JTS_FACTORY.createLineString(current));
    }
  }

  public static List<VectorTileEncoder.Feature> mergePolygons(List<VectorTileEncoder.Feature> items, double minSize,
    double minDist, double buffer) {
    return items;
  }
}
