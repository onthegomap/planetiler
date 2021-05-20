/*
 * ISC License
 * <p>
 * Copyright (c) 2015, Mapbox
 * <p>
 * Permission to use, copy, modify, and/or distribute this software for any purpose with or without fee is hereby
 * granted, provided that the above copyright notice and this permission notice appear in all copies.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
 * INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
 * AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 */
package com.onthegomap.flatmap.render;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.graphhopper.coll.GHIntObjectHashMap;
import com.onthegomap.flatmap.TileExtents;
import com.onthegomap.flatmap.collections.IntRange;
import com.onthegomap.flatmap.collections.MutableCoordinateSequence;
import com.onthegomap.flatmap.geo.TileCoord;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is adapted from the stripe clipping algorithm in https://github.com/mapbox/geojson-vt/ and modified so
 * that it eagerly produces all sliced tiles at a zoom level for each input geometry.
 */
class TiledGeometry {

  private static final Logger LOGGER = LoggerFactory.getLogger(TiledGeometry.class);

  private final Map<TileCoord, List<List<CoordinateSequence>>> tileContents = new HashMap<>();
  private final Map<Integer, IntRange> filledRanges = new HashMap<>();
  private final TileExtents.ForZoom extents;
  private final double buffer;
  private final int z;
  private final boolean area;
  private final int max;

  private TiledGeometry(TileExtents.ForZoom extents, double buffer, int z, boolean area) {
    this.extents = extents;
    this.buffer = buffer;
    this.z = z;
    this.area = area;
    this.max = 1 << z;
  }

  public int zoomLevel() {
    return z;
  }

  public static TiledGeometry sliceIntoTiles(List<List<CoordinateSequence>> groups, double buffer, boolean area, int z,
    TileExtents.ForZoom extents) {
    int worldExtent = 1 << z;
    TiledGeometry result = new TiledGeometry(extents, buffer, z, area);
    EnumSet<Direction> wrapResult = result.sliceWorldCopy(groups, 0);
    if (wrapResult.contains(Direction.RIGHT)) {
      result.sliceWorldCopy(groups, -worldExtent);
    }
    if (wrapResult.contains(Direction.LEFT)) {
      result.sliceWorldCopy(groups, worldExtent);
    }
    return result;
  }

  public Iterable<TileCoord> getFilledTiles() {
    return () -> filledRanges.entrySet().stream()
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

  private static void intersectX(MutableCoordinateSequence out, double ax, double ay, double bx, double by, double x) {
    double t = (x - ax) / (bx - ax);
    out.addPoint(x, ay + (by - ay) * t);
  }

  private static void intersectY(MutableCoordinateSequence out, double ax, double ay, double bx, double by, double y) {
    double t = (y - ay) / (by - ay);
    out.addPoint(ax + (bx - ax) * t, y);
  }

  private static CoordinateSequence fill(double buffer) {
    buffer += 1d / 4096;
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

  private EnumSet<Direction> sliceWorldCopy(List<List<CoordinateSequence>> groups, int xOffset) {
    EnumSet<Direction> overflow = EnumSet.noneOf(Direction.class);
    for (List<CoordinateSequence> group : groups) {
      Map<TileCoord, List<CoordinateSequence>> inProgressShapes = new HashMap<>();
      for (int i = 0; i < group.size(); i++) {
        CoordinateSequence segment = group.get(i);
        boolean outer = i == 0;
        IntObjectMap<List<MutableCoordinateSequence>> xSlices = sliceX(segment);
        if (z >= 6 && xSlices.size() >= Math.pow(2, z) - 1) {
          LOGGER.warn("Feature crosses world at z" + z + ": " + xSlices.size());
        }
        for (IntObjectCursor<List<MutableCoordinateSequence>> xCursor : xSlices) {
          int x = xCursor.key + xOffset;
          if (x >= max) {
            overflow.add(Direction.RIGHT);
          } else if (x < 0) {
            overflow.add(Direction.LEFT);
          } else {
            for (CoordinateSequence stripeSegment : xCursor.value) {
              IntRange filledYRange = sliceY(stripeSegment, x, outer, inProgressShapes);
              if (area && filledYRange != null) {
                if (outer) {
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

  private IntObjectMap<List<MutableCoordinateSequence>> sliceX(CoordinateSequence segment) {
    int maxIndex = 1 << z;
    double k1 = -buffer;
    double k2 = 1 + buffer;
    IntObjectMap<List<MutableCoordinateSequence>> newGeoms = new GHIntObjectHashMap<>();
    IntObjectMap<MutableCoordinateSequence> xSlices = new GHIntObjectHashMap<>();
    int end = segment.size() - 1;
    for (int i = 0; i < end; i++) {
      double _ax = segment.getX(i);
      double ay = segment.getY(i);
      double _bx = segment.getX(i + 1);
      double by = segment.getY(i + 1);

      double minX = Math.min(_ax, _bx);
      double maxX = Math.max(_ax, _bx);

      int startX = (int) Math.floor(minX - buffer);
      int endX = (int) Math.floor(maxX + buffer);

      for (int x = startX; x <= endX; x++) {
        double ax = _ax - x;
        double bx = _bx - x;
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

        if (ax < k1) {
          // ---|-->  | (line enters the clip region from the left)
          if (bx > k1) {
            intersectX(slice, ax, ay, bx, by, k1);
          }
        } else if (ax > k2) {
          // |  <--|--- (line enters the clip region from the right)
          if (bx < k2) {
            intersectX(slice, ax, ay, bx, by, k2);
          }
        } else {
          slice.addPoint(ax, ay);
        }
        if (bx < k1 && ax >= k1) {
          // <--|---  | or <--|-----|--- (line exits the clip region on the left)
          intersectX(slice, ax, ay, bx, by, k1);
          exited = true;
        }
        if (bx > k2 && ax <= k2) {
          // |  ---|--> or ---|-----|--> (line exits the clip region on the right)
          intersectX(slice, ax, ay, bx, by, k2);
          exited = true;
        }

        if (!area && exited) {
          xSlices.remove(x);
        }
      }
    }
    // add the last point
    double _ax = segment.getX(segment.size() - 1);
    double ay = segment.getY(segment.size() - 1);
    int startX = (int) Math.floor(_ax - buffer);
    int endX = (int) Math.floor(_ax + buffer);

    for (int x = startX - 1; x <= endX + 1; x++) {
      double ax = _ax - x;
      MutableCoordinateSequence slice = xSlices.get(x);
      if (slice != null && ax >= k1 && ax <= k2) {
        slice.addPoint(ax, ay);
      }
    }

    // close the polygons if endpoints are not the same after clipping
    if (area) {
      for (IntObjectCursor<MutableCoordinateSequence> cursor : xSlices) {
        cursor.value.closeRing();
      }
    }
    newGeoms.removeAll((x, value) -> {
      int wrapped = wrapX(x, maxIndex);
      return !extents.testX(wrapped);
    });
    return newGeoms;
  }

  private IntRange sliceY(CoordinateSequence stripeSegment, int x, boolean outer,
    Map<TileCoord, List<CoordinateSequence>> inProgressShapes) {
    double leftEdge = -buffer;
    double rightEdge = 1 + buffer;

    TreeSet<Integer> tiles = null;
    IntRange rightFilled = null;
    IntRange leftFilled = null;

    IntObjectMap<MutableCoordinateSequence> ySlices = new GHIntObjectHashMap<>();
    int max = 1 << z;
    if (x < 0 || x >= max) {
      return null;
    }
    record SkippedSegment(Direction side, int lo, int hi) {

    }
    List<SkippedSegment> skipped = null;
    for (int i = 0; i < stripeSegment.size() - 1; i++) {
      double ax = stripeSegment.getX(i);
      double ay = stripeSegment.getY(i);
      double bx = stripeSegment.getX(i + 1);
      double by = stripeSegment.getY(i + 1);

      double minY = Math.min(ay, by);
      double maxY = Math.max(ay, by);

      int extentMinY = extents.minY();
      int extentMaxY = extents.maxY();
      int startY = Math.max(extentMinY, (int) Math.floor(minY - buffer));
      int endStartY = Math.max(extentMinY, (int) Math.floor(minY + buffer));
      int startEndY = Math.min(extentMaxY - 1, (int) Math.floor(maxY - buffer));
      int endY = Math.min(extentMaxY - 1, (int) Math.floor(maxY + buffer));

      boolean onRightEdge = area && ax == bx && ax == rightEdge && by > ay;
      boolean onLeftEdge = area && ax == bx && ax == leftEdge && by < ay;

      for (int y = startY; y <= endY; y++) {
        if (area && y > endStartY && y < startEndY) {
          if (onRightEdge || onLeftEdge) {
            // skip over filled tile
            if (tiles == null) {
              tiles = new TreeSet<>();
              for (IntCursor cursor : ySlices.keys()) {
                tiles.add(cursor.value);
              }
            }
            Integer next = tiles.ceiling(y);
            int nextNonEdgeTile = next == null ? startEndY : Math.min(next, startEndY);
            int endSkip = nextNonEdgeTile - 1;
            if (skipped == null) {
              skipped = new ArrayList<>();
            }
            skipped.add(new SkippedSegment(
              onLeftEdge ? Direction.LEFT : Direction.RIGHT,
              y,
              endSkip
            ));

            if (rightFilled == null) {
              rightFilled = new IntRange();
              leftFilled = new IntRange();
            }
            (onRightEdge ? rightFilled : leftFilled).add(y, endSkip);

            y = nextNonEdgeTile;
          }
        }
        double k1 = y - buffer;
        double k2 = y + 1 + buffer;
        MutableCoordinateSequence slice = ySlices.get(y);
        if (slice == null) {
          if (tiles != null) {
            tiles.add(y);
          }
          // x is already relative to tile
          ySlices.put(y, slice = MutableCoordinateSequence.newScalingSequence(0, y, 256));
          TileCoord tileID = TileCoord.ofXYZ(x, y, z);
          List<CoordinateSequence> toAddTo = inProgressShapes.computeIfAbsent(tileID, tile -> new ArrayList<>());

          // if this is tile is inside a fill from an outer tile, infer that fill here
          if (area && !outer && toAddTo.isEmpty()) {
            toAddTo.add(fill(buffer));
          }
          toAddTo.add(slice);

          // if this tile was skipped because we skipped an edge and now it needs more points,
          // backfill all of the edges that we skipped for it
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

        if (ay < k1) {
          // ---|-->  | (line enters the clip region from the top)
          if (by > k1) {
            intersectY(slice, ax, ay, bx, by, k1);
          }
        } else if (ay > k2) {
          // |  <--|--- (line enters the clip region from the bottom)
          if (by < k2) {
            intersectY(slice, ax, ay, bx, by, k2);
          }
        } else {
          slice.addPoint(ax, ay);
        }
        if (by < k1 && ay >= k1) {
          // <--|---  | or <--|-----|--- (line exits the clip region on the top)
          intersectY(slice, ax, ay, bx, by, k1);
          exited = true;
        }
        if (by > k2 && ay <= k2) {
          // |  ---|--> or ---|-----|--> (line exits the clip region on the bottom)
          intersectY(slice, ax, ay, bx, by, k2);
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
    int startY = (int) (ay - buffer);
    int endY = (int) (ay + buffer);

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
    return rightFilled != null ? rightFilled.intersect(leftFilled) : null;
  }

  private void addFilledRange(int x, IntRange yRange) {
    if (yRange == null) {
      return;
    }
    IntRange existing = filledRanges.get(x);
    if (existing == null) {
      filledRanges.put(x, yRange);
    } else {
      existing.addAll(yRange);
    }
  }

  private void removeFilledRange(int x, IntRange yRange) {
    if (yRange == null) {
      return;
    }
    IntRange existing = filledRanges.get(x);
    if (existing != null) {
      existing.removeAll(yRange);
    }
  }

  private enum Direction {RIGHT, LEFT}
}
