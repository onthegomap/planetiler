/*
ISC License

Copyright (c) 2015, Mapbox

Permission to use, copy, modify, and/or distribute this software for any purpose
with or without fee is hereby granted, provided that the above copyright notice
and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER
TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF
THIS SOFTWARE.
 */
package com.onthegomap.planetiler.render;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.onthegomap.planetiler.collection.Hppc;
import com.onthegomap.planetiler.collection.IntRangeSet;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.MutableCoordinateSequence;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileExtents;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import javax.annotation.concurrent.NotThreadSafe;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Splits geometries represented by lists of {@link CoordinateSequence CoordinateSequences} into the geometries that
 * appear on individual tiles that the geometry touches.
 * <p>
 * {@link GeometryCoordinateSequences} converts between JTS {@link Geometry} instances and {@link CoordinateSequence}
 * lists for this utility.
 * <p>
 * This class is adapted from the stripe clipping algorithm in https://github.com/mapbox/geojson-vt/ and modified so
 * that it eagerly produces all sliced tiles at a zoom level for each input geometry.
 */
@NotThreadSafe
class TiledGeometry {

  private static final Logger LOGGER = LoggerFactory.getLogger(TiledGeometry.class);
  private static final double NEIGHBOR_BUFFER_EPS = 0.1d / 4096;

  private final long featureId;
  private final Map<TileCoord, List<List<CoordinateSequence>>> tileContents = new HashMap<>();
  /** Map from X coordinate to range of Y coordinates that contain filled tiles inside this geometry */
  private Map<Integer, IntRangeSet> filledRanges = null;
  private final TileExtents.ForZoom extents;
  private final double buffer;
  private final double neighborBuffer;
  private final int z;
  private final boolean area;
  private final int maxTilesAtThisZoom;

  private TiledGeometry(TileExtents.ForZoom extents, double buffer, int z, boolean area, long featureId) {
    this.featureId = featureId;
    this.extents = extents;
    this.buffer = buffer;
    // make sure we inspect neighboring tiles when a line runs along an edge
    this.neighborBuffer = buffer + NEIGHBOR_BUFFER_EPS;
    this.z = z;
    this.area = area;
    this.maxTilesAtThisZoom = 1 << z;
  }

  /**
   * Returns all the points that appear on tiles representing points at {@code coords}.
   *
   * @param extents range of tile coordinates within the bounds of the map to generate
   * @param buffer  how far detail should be included beyond the edge of each tile (0=none, 1=a full tile width)
   * @param z       zoom level
   * @param coords  the world web mercator coordinates of each point to emit at this zoom level where (0,0) is the
   *                northwest and (2^z,2^z) is the southeast corner of the planet
   * @param id      feature ID
   * @return each tile this feature touches, and the points that appear on each
   */
  public static TiledGeometry slicePointsIntoTiles(TileExtents.ForZoom extents, double buffer, int z,
    Coordinate[] coords, long id) {
    TiledGeometry result = new TiledGeometry(extents, buffer, z, false, id);
    for (Coordinate coord : coords) {
      result.slicePoint(coord);
    }
    return result;
  }

  private static int wrapInt(int value, int max) {
    value %= max;
    if (value < 0) {
      value += max;
    }
    return value;
  }

  private void slicePoint(Coordinate coord) {
    var shape = this.extents.shape();
    double worldX = coord.getX() * maxTilesAtThisZoom;
    double worldY = coord.getY() * maxTilesAtThisZoom;
    int minX = (int) Math.floor(worldX - neighborBuffer);
    int maxX = (int) Math.floor(worldX + neighborBuffer);
    int minY = Math.max(extents.minY(), (int) Math.floor(worldY - neighborBuffer));
    int maxY = Math.min(extents.maxY() - 1, (int) Math.floor(worldY + neighborBuffer));
    for (int x = minX; x <= maxX; x++) {
      double tileX = worldX - x;
      int wrappedX = wrapInt(x, maxTilesAtThisZoom);
      // point may end up inside bounds after wrapping
      if (extents.testX(wrappedX)) {
        for (int y = minY; y <= maxY; y++) {
          TileCoord tile = TileCoord.ofXYZ(wrappedX, y, z);
          if (shape != null) {
            TileCoord tileID = TileCoord.ofXYZ(wrappedX, y, z);
            if (!shape.intersects(tileID.getEnvelope())) {
              continue;
            }
          }
          double tileY = worldY - y;
          tileContents.computeIfAbsent(tile, t -> List.of(new ArrayList<>()))
            .get(0)
            .add(GeoUtils.coordinateSequence(tileX * 256, tileY * 256));
        }
      }
    }
  }

  public int zoomLevel() {
    return z;
  }

  /**
   * Returns all the points that appear on tiles representing points at {@code coords}.
   *
   * @param groups  the list of linestrings or polygon rings extracted using {@link GeometryCoordinateSequences} in
   *                world web mercator coordinates where (0,0) is the northwest and (2^z,2^z) is the southeast corner of
   *                the planet
   * @param buffer  how far detail should be included beyond the edge of each tile (0=none, 1=a full tile width)
   * @param area    {@code true} if this is a polygon {@code false} if this is a linestring
   * @param z       zoom level
   * @param extents range of tile coordinates within the bounds of the map to generate
   * @param id      feature ID
   * @return each tile this feature touches, and the points that appear on each
   */
  public static TiledGeometry sliceIntoTiles(List<List<CoordinateSequence>> groups, double buffer, boolean area, int z,
    TileExtents.ForZoom extents, long id) {
    TiledGeometry result = new TiledGeometry(extents, buffer, z, area, id);
    EnumSet<Direction> wrapResult = result.sliceWorldCopy(groups, 0);
    if (wrapResult.contains(Direction.RIGHT)) {
      result.sliceWorldCopy(groups, -result.maxTilesAtThisZoom);
    }
    if (wrapResult.contains(Direction.LEFT)) {
      result.sliceWorldCopy(groups, result.maxTilesAtThisZoom);
    }
    return result;
  }

  /**
   * Returns an iterator over the coordinates of every tile that is completely filled within this polygon at this zoom
   * level, ordered by x ascending, y ascending.
   */
  public Iterable<TileCoord> getFilledTiles() {
    return filledRanges == null ? Collections.emptyList() :
      () -> filledRanges.entrySet().stream()
        .<TileCoord>mapMulti((entry, next) -> {
          int x = entry.getKey();
          for (int y : entry.getValue()) {
            TileCoord coord = TileCoord.ofXYZ(x, y, z);
            if (!tileContents.containsKey(coord)) {
              next.accept(coord);
            }
          }
        }).iterator();
  }

  /**
   * Returns every tile that this geometry touches, and the partial geometry contained on that tile that can be
   * reassembled using {@link GeometryCoordinateSequences}.
   */
  public Iterable<Map.Entry<TileCoord, List<List<CoordinateSequence>>>> getTileData() {
    return tileContents.entrySet();
  }

  private static int wrapX(int x, int max) {
    x %= max;
    if (x < 0) {
      x += max;
    }
    return x;
  }

  /** Adds a new point to {@code out} where the line segment from (ax,ay) to (bx,by) crosses a vertical line at x=x. */
  private static void intersectX(MutableCoordinateSequence out, double ax, double ay, double bx, double by, double x) {
    double t = (x - ax) / (bx - ax);
    out.addPoint(x, ay + (by - ay) * t);
  }

  /**
   * Adds a new point to {@code out} where the line segment from (ax,ay) to (bx,by) crosses a horizontal line at y=y.
   */
  private static void intersectY(MutableCoordinateSequence out, double ax, double ay, double bx, double by, double y) {
    double t = (y - ay) / (by - ay);
    out.addPoint(ax + (bx - ax) * t, y);
  }

  private static CoordinateSequence fill(double buffer) {
    double min = -256d * buffer;
    double max = 256d - min;
    return new PackedCoordinateSequence.Double(new double[]{
      min, min,
      max, min,
      max, max,
      min, max,
      min, min
    }, 2, 0);
  }

  /**
   * Slices a geometry into tiles and stores in member fields for a single "copy" of the world.
   * <p>
   * Instead of handling content outside -180 to 180 degrees longitude, return {@link Direction#LEFT} or
   * {@link Direction#RIGHT} to indicate whether this method should be called again with a different {@code xOffset} to
   * process wrapped content.
   *
   * @param groups  the geometry
   * @param xOffset offset to apply to each X coordinate (-2^z handles content that wraps too far east and 2^z handles
   *                content that wraps too far west)
   * @return {@link Direction#LEFT} if there is more content to the west and {@link Direction#RIGHT} if there is more
   *         content to the east.
   */
  private EnumSet<Direction> sliceWorldCopy(List<List<CoordinateSequence>> groups, int xOffset) {
    EnumSet<Direction> overflow = EnumSet.noneOf(Direction.class);
    for (List<CoordinateSequence> group : groups) {
      Map<TileCoord, List<CoordinateSequence>> inProgressShapes = new HashMap<>();
      for (int i = 0; i < group.size(); i++) {
        CoordinateSequence segment = group.get(i);
        boolean isOuterRing = i == 0;
        /*
         * Step 1 in the striped clipping algorithm: slice the geometry into vertical slices representing each "x" tile
         * coordinate:
         * x=0 1 2 3 4 ...
         *  | | | | | |
         *  |-|-| | | |
         *  | | |\| | |
         *  | | | |-|-|
         *  | | | | | |
         */
        IntObjectMap<List<MutableCoordinateSequence>> xSlices = sliceX(segment);
        if (z >= 6 && xSlices.size() >= Math.pow(2, z) - 1) {
          LOGGER.warn("Feature " + featureId + " crosses world at z" + z + ": " + xSlices.size());
        }
        for (IntObjectCursor<List<MutableCoordinateSequence>> xCursor : xSlices) {
          int x = xCursor.key + xOffset;
          // skip processing content past the edge of the world, but return that we saw it
          if (x >= maxTilesAtThisZoom) {
            overflow.add(Direction.RIGHT);
          } else if (x < 0) {
            overflow.add(Direction.LEFT);
          } else {
            /*
             * Step 2 in the striped clipping algorithm: split each vertical column x slice into horizontal slices
             * representing the row for each Y coordinate.
             */
            for (CoordinateSequence stripeSegment : xCursor.value) {
              // sliceY only stores content for rings of a polygon, need to store the
              // filled tiles that it spanned separately
              IntRangeSet filledYRange = sliceY(stripeSegment, x, isOuterRing, inProgressShapes);
              if (area && filledYRange != null) {
                if (isOuterRing) {
                  addFilledRange(x, filledYRange);
                } else {
                  removeFilledRange(x, filledYRange);
                }
              }
            }
          }
        }
      }
      addShapeToResults(inProgressShapes);
    }

    return overflow;
  }

  private void addShapeToResults(Map<TileCoord, List<CoordinateSequence>> inProgressShapes) {
    for (var entry : inProgressShapes.entrySet()) {
      TileCoord tileID = entry.getKey();
      List<CoordinateSequence> inSeqs = entry.getValue();
      if (area && inSeqs.get(0).size() < 4) {
        // not enough points in outer polygon, ignore
        continue;
      }
      int minPoints = area ? 4 : 2;
      List<CoordinateSequence> outSeqs = inSeqs.stream()
        .filter(seq -> seq.size() >= minPoints)
        .toList();
      if (!outSeqs.isEmpty()) {
        tileContents.computeIfAbsent(tileID, tile -> new ArrayList<>()).add(outSeqs);
      }
    }
  }

  /**
   * Returns a map from X coordinate to segments of this geometry that cross the vertical column formed by all tiles
   * where {@code x=x}.
   */
  private IntObjectMap<List<MutableCoordinateSequence>> sliceX(CoordinateSequence segment) {
    double leftLimit = -buffer;
    double rightLimit = 1 + buffer;
    IntObjectMap<List<MutableCoordinateSequence>> newGeoms = Hppc.newIntObjectHashMap();
    IntObjectMap<MutableCoordinateSequence> xSlices = Hppc.newIntObjectHashMap();
    int end = segment.size() - 1;
    for (int i = 0; i < end; i++) {
      double ax = segment.getX(i);
      double ay = segment.getY(i);
      double bx = segment.getX(i + 1);
      double by = segment.getY(i + 1);

      double minX = Math.min(ax, bx);
      double maxX = Math.max(ax, bx);

      int startX = (int) Math.floor(minX - neighborBuffer);
      int endX = (int) Math.floor(maxX + neighborBuffer);

      // for each column this segment crosses
      for (int x = startX; x <= endX; x++) {
        double axTile = ax - x;
        double bxTile = bx - x;
        MutableCoordinateSequence slice = xSlices.get(x);
        if (slice == null) {
          xSlices.put(x, slice = new MutableCoordinateSequence());
          List<MutableCoordinateSequence> newGeom = newGeoms.get(x);
          if (newGeom == null) {
            newGeoms.put(x, newGeom = new ArrayList<>());
          }
          newGeom.add(slice);
        }

        boolean exited = false;

        if (axTile < leftLimit) {
          // ---|-->  | (line enters the clip region from the left)
          if (bxTile > leftLimit) {
            intersectX(slice, axTile, ay, bxTile, by, leftLimit);
          }
        } else if (axTile > rightLimit) {
          // |  <--|--- (line enters the clip region from the right)
          if (bxTile < rightLimit) {
            intersectX(slice, axTile, ay, bxTile, by, rightLimit);
          }
        } else {
          // | --> | (line starts inside)
          slice.addPoint(axTile, ay);
        }
        if (bxTile < leftLimit && axTile >= leftLimit) {
          // <--|---  | or <--|-----|--- (line exits the clip region on the left)
          intersectX(slice, axTile, ay, bxTile, by, leftLimit);
          exited = true;
        }
        if (bxTile > rightLimit && axTile <= rightLimit) {
          // |  ---|--> or ---|-----|--> (line exits the clip region on the right)
          intersectX(slice, axTile, ay, bxTile, by, rightLimit);
          exited = true;
        }

        if (!area && exited) {
          xSlices.remove(x);
        }
      }
    }
    // add the last point
    double ax = segment.getX(segment.size() - 1);
    double ay = segment.getY(segment.size() - 1);
    int startX = (int) Math.floor(ax - neighborBuffer);
    int endX = (int) Math.floor(ax + neighborBuffer);

    for (int x = startX - 1; x <= endX + 1; x++) {
      double axTile = ax - x;
      MutableCoordinateSequence slice = xSlices.get(x);
      if (slice != null && axTile >= leftLimit && axTile <= rightLimit) {
        slice.addPoint(axTile, ay);
      }
    }

    // close the polygons if endpoints are not the same after clipping
    if (area) {
      for (IntObjectCursor<MutableCoordinateSequence> cursor : xSlices) {
        cursor.value.closeRing();
      }
    }
    newGeoms.removeAll((x, value) -> {
      int wrapped = wrapX(x, maxTilesAtThisZoom);
      return !extents.testX(wrapped);
    });
    return newGeoms;
  }

  /**
   * Splits an entire vertical X column of edge segments into Y rows that form (X, Y) tile coordinates at this zoom
   * level, stores the result in {@link #tileContents} and returns the Y ranges of filled tile coordinates if this is a
   * polygon.
   */
  private IntRangeSet sliceY(CoordinateSequence stripeSegment, int x, boolean outer,
    Map<TileCoord, List<CoordinateSequence>> inProgressShapes) {
    if (stripeSegment.size() == 0) {
      return null;
    }
    double leftEdge = -buffer;
    double rightEdge = 1 + buffer;

    TreeSet<Integer> tileYsWithDetail = null;
    IntRangeSet rightFilled = null;
    IntRangeSet leftFilled = null;

    IntObjectMap<MutableCoordinateSequence> ySlices = Hppc.newIntObjectHashMap();
    if (x < 0 || x >= maxTilesAtThisZoom) {
      return null;
    }

    // keep a record of filled tiles that we skipped because an edge of the polygon that gets processed
    // later may intersect the edge of a filled tile, and we'll need to replay all the edges we skipped
    record SkippedSegment(Direction side, int lo, int hi) {}
    List<SkippedSegment> skipped = null;

    var shape = this.extents.shape();

    for (int i = 0; i < stripeSegment.size() - 1; i++) {
      double ax = stripeSegment.getX(i);
      double ay = stripeSegment.getY(i);
      double bx = stripeSegment.getX(i + 1);
      double by = stripeSegment.getY(i + 1);

      double minY = Math.min(ay, by);
      double maxY = Math.max(ay, by);

      int extentMinY = extents.minY();
      int extentMaxY = extents.maxY();
      int startY = Math.max(extentMinY, (int) Math.floor(minY - neighborBuffer));
      int endStartY = Math.max(extentMinY, (int) Math.floor(minY + neighborBuffer));
      int startEndY = Math.min(extentMaxY - 1, (int) Math.floor(maxY - neighborBuffer));
      int endY = Math.min(extentMaxY - 1, (int) Math.floor(maxY + neighborBuffer));

      // inside a fill if one edge of the polygon runs straight down the right side or up the left side of the column
      boolean onRightEdge = area && ax == bx && ax == rightEdge && by > ay;
      boolean onLeftEdge = area && ax == bx && ax == leftEdge && by < ay;

      for (int y = startY; y <= endY; y++) {

        if (shape != null) {
          TileCoord tileID = TileCoord.ofXYZ(x, y, z);
          if (!shape.intersects(tileID.getEnvelope())) {
            continue;
          }
        }
        // skip over filled tiles until we get to the next tile that already has detail on it
        if (area && y > endStartY && y < startEndY) {
          if (onRightEdge || onLeftEdge) {
            if (tileYsWithDetail == null) {
              tileYsWithDetail = new TreeSet<>();
              for (IntCursor cursor : ySlices.keys()) {
                tileYsWithDetail.add(cursor.value);
              }
            }
            Integer next = tileYsWithDetail.ceiling(y);
            int nextNonEdgeTile = next == null ? startEndY : Math.min(next, startEndY);
            int endSkip = nextNonEdgeTile - 1;
            if (skipped == null) {
              skipped = new ArrayList<>();
            }
            // save the Y range that we skipped in case a later edge intersects a filled tile
            skipped.add(new SkippedSegment(
              onLeftEdge ? Direction.LEFT : Direction.RIGHT,
              y,
              endSkip
            ));

            if (rightFilled == null) {
              rightFilled = new IntRangeSet();
              leftFilled = new IntRangeSet();
            }
            (onRightEdge ? rightFilled : leftFilled).add(y, endSkip);

            y = nextNonEdgeTile;
          }
        }

        // emit linestring/polygon ring detail
        double topLimit = y - buffer;
        double bottomLimit = y + 1 + buffer;
        MutableCoordinateSequence slice = ySlices.get(y);
        if (slice == null) {
          if (tileYsWithDetail != null) {
            tileYsWithDetail.add(y);
          }
          // X is already relative to tile, but we need to adjust Y
          ySlices.put(y, slice = MutableCoordinateSequence.newScalingSequence(0, y, 256));
          TileCoord tileID = TileCoord.ofXYZ(x, y, z);
          List<CoordinateSequence> toAddTo = inProgressShapes.computeIfAbsent(tileID, tile -> new ArrayList<>());

          // if this is tile is inside a fill from an outer tile, infer that fill here
          if (area && !outer && toAddTo.isEmpty()) {
            toAddTo.add(fill(buffer));
          }
          toAddTo.add(slice);

          // if this tile was skipped because we skipped an edge, and now it needs more points,
          // backfill all the edges that we skipped for it
          if (area && leftFilled != null && skipped != null && (leftFilled.contains(y) || rightFilled.contains(y))) {
            for (SkippedSegment skippedSegment : skipped) {
              if (skippedSegment.lo <= y && skippedSegment.hi >= y) {
                double top = y - buffer;
                double bottom = y + 1 + buffer;
                if (skippedSegment.side == Direction.LEFT) {
                  slice.addPoint(-buffer, bottom);
                  slice.addPoint(-buffer, top);
                } else { // side == RIGHT
                  slice.addPoint(1 + buffer, top);
                  slice.addPoint(1 + buffer, bottom);
                }
              }
            }
          }
        }

        boolean exited = false;

        if (ay < topLimit) {
          // ---|-->  | (line enters the clip region from the top)
          if (by > topLimit) {
            intersectY(slice, ax, ay, bx, by, topLimit);
          }
        } else if (ay > bottomLimit) {
          // |  <--|--- (line enters the clip region from the bottom)
          if (by < bottomLimit) {
            intersectY(slice, ax, ay, bx, by, bottomLimit);
          }
        } else {
          // | --> | (line starts inside the clip region)
          slice.addPoint(ax, ay);
        }
        if (by < topLimit && ay >= topLimit) {
          // <--|---  | or <--|-----|--- (line exits the clip region on the top)
          intersectY(slice, ax, ay, bx, by, topLimit);
          exited = true;
        }
        if (by > bottomLimit && ay <= bottomLimit) {
          // |  ---|--> or ---|-----|--> (line exits the clip region on the bottom)
          intersectY(slice, ax, ay, bx, by, bottomLimit);
          exited = true;
        }

        if (!area && exited) {
          ySlices.remove(y);
        }
      }
    }

    // add the last point
    int last = stripeSegment.size() - 1;
    double ax = stripeSegment.getX(last);
    double ay = stripeSegment.getY(last);
    int startY = (int) Math.floor(ay - neighborBuffer);
    int endY = (int) Math.floor(ay + neighborBuffer);

    for (int y = startY - 1; y <= endY + 1; y++) {
      MutableCoordinateSequence slice = ySlices.get(y);
      double k1 = y - buffer;
      double k2 = y + 1 + buffer;
      if (ay >= k1 && ay <= k2 && slice != null) {
        slice.addPoint(ax, ay);
      }
    }
    // close the polygons if endpoints are not the same after clipping
    if (area) {
      for (IntObjectCursor<MutableCoordinateSequence> cursor : ySlices) {
        cursor.value.closeRing();
      }
    }
    // a tile is filled if we skipped over the entire left and right side of it while processing a polygon
    return rightFilled != null ? rightFilled.intersect(leftFilled) : null;
  }

  private void addFilledRange(int x, IntRangeSet yRange) {
    if (yRange == null) {
      return;
    }
    if (filledRanges == null) {
      filledRanges = new HashMap<>();
    }
    IntRangeSet existing = filledRanges.get(x);
    if (existing == null) {
      filledRanges.put(x, yRange);
    } else {
      existing.addAll(yRange);
    }
  }

  private void removeFilledRange(int x, IntRangeSet yRange) {
    if (yRange == null) {
      return;
    }
    if (filledRanges == null) {
      filledRanges = new HashMap<>();
    }
    IntRangeSet existing = filledRanges.get(x);
    if (existing != null) {
      existing.removeAll(yRange);
    }
  }

  private enum Direction {
    RIGHT,
    LEFT
  }
}
