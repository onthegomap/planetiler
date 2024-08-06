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
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.MutableCoordinateSequence;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileExtents;
import com.onthegomap.planetiler.geo.TilePredicate;
import com.onthegomap.planetiler.util.Format;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Stream;
import javax.annotation.concurrent.NotThreadSafe;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.Puntal;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.roaringbitmap.RoaringBitmap;

/**
 * Splits geometries represented by lists of {@link CoordinateSequence CoordinateSequences} into the geometries that
 * appear on individual tiles that the geometry touches.
 * <p>
 * {@link GeometryCoordinateSequences} converts between JTS {@link Geometry} instances and {@link CoordinateSequence}
 * lists for this utility.
 * <p>
 * This class is adapted from the stripe clipping algorithm in
 * <a href="https://github.com/mapbox/geojson-vt/">geojson-vt</a> and modified so that it eagerly produces all sliced
 * tiles at a zoom level for each input geometry.
 */
@NotThreadSafe
public class TiledGeometry {

  private static final Format FORMAT = Format.defaultInstance();
  private static final double NEIGHBOR_BUFFER_EPS = 0.1d / 4096;

  private final Map<TileCoord, List<List<CoordinateSequence>>> tileContents = new HashMap<>();
  private final TileExtents.ForZoom extents;
  private final double buffer;
  private final double neighborBuffer;
  private final int z;
  private final boolean area;
  private final int maxTilesAtThisZoom;
  /** Map from X coordinate to range of Y coordinates that contain filled tiles inside this geometry */
  private Map<Integer, IntRangeSet> filledRanges = null;

  private TiledGeometry(TileExtents.ForZoom extents, double buffer, int z, boolean area) {
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
   * @return each tile this feature touches, and the points that appear on each
   */
  static TiledGeometry slicePointsIntoTiles(TileExtents.ForZoom extents, double buffer, int z,
    Coordinate[] coords) {
    TiledGeometry result = new TiledGeometry(extents, buffer, z, false);
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

  /**
   * Returns all the points that appear on tiles representing points at {@code coords}.
   *
   * @param scaledGeom the scaled geometry to slice into tiles, in world web mercator coordinates where (0,0) is the
   *                   northwest and (1,1) is the southeast corner of the planet
   * @param minSize    the minimum length of a line or area of a polygon to emit
   * @param buffer     how far detail should be included beyond the edge of each tile (0=none, 1=a full tile width)
   * @param z          zoom level
   * @param extents    range of tile coordinates within the bounds of the map to generate
   * @return each tile this feature touches, and the points that appear on each
   */
  public static TiledGeometry sliceIntoTiles(Geometry scaledGeom, double minSize, double buffer, int z,
    TileExtents.ForZoom extents) throws GeometryException {

    if (scaledGeom.isEmpty()) {
      // ignore
      return new TiledGeometry(extents, buffer, z, false);
    } else if (scaledGeom instanceof Point point) {
      return slicePointsIntoTiles(extents, buffer, z, point.getCoordinates());
    } else if (scaledGeom instanceof MultiPoint points) {
      return slicePointsIntoTiles(extents, buffer, z, points.getCoordinates());
    } else if (scaledGeom instanceof Polygon || scaledGeom instanceof MultiPolygon ||
      scaledGeom instanceof LineString ||
      scaledGeom instanceof MultiLineString) {
      var coordinateSequences = GeometryCoordinateSequences.extractGroups(scaledGeom, minSize);
      boolean area = scaledGeom instanceof Polygonal;
      return sliceIntoTiles(coordinateSequences, buffer, area, z, extents);
    } else {
      throw new UnsupportedOperationException(
        "Unsupported JTS geometry type " + scaledGeom.getClass().getSimpleName() + " " +
          scaledGeom.getGeometryType());
    }
  }

  /**
   * Returns the set of tiles that {@code scaledGeom} touches at a zoom level.
   *
   * @param scaledGeom The geometry in scaled web mercator coordinates where northwest is (0,0) and southeast is
   *                   (2^z,2^z)
   * @param zoom       The zoom level
   * @param extents    The tile extents for this zoom level.
   * @return A {@link CoveredTiles} instance for the tiles that are covered by this geometry.
   */
  public static CoveredTiles getCoveredTiles(Geometry scaledGeom, int zoom, TileExtents.ForZoom extents)
    throws GeometryException {
    if (scaledGeom.isEmpty()) {
      return new CoveredTiles(new RoaringBitmap(), zoom);
    } else if (scaledGeom instanceof Puntal || scaledGeom instanceof Polygonal || scaledGeom instanceof Lineal) {
      return sliceIntoTiles(scaledGeom, 0, 0, zoom, extents).getCoveredTiles();
    } else if (scaledGeom instanceof GeometryCollection gc) {
      CoveredTiles result = new CoveredTiles(new RoaringBitmap(), zoom);
      for (int i = 0; i < gc.getNumGeometries(); i++) {
        result = CoveredTiles.merge(getCoveredTiles(gc.getGeometryN(i), zoom, extents), result);
      }
      return result;
    } else {
      throw new UnsupportedOperationException(
        "Unsupported JTS geometry type " + scaledGeom.getClass().getSimpleName() + " " +
          scaledGeom.getGeometryType());
    }
  }

  /**
   * 返回此几何体触碰到的瓦片及其内容。
   *
   * @param groups  使用 {@link GeometryCoordinateSequences} 提取的线串或多边形环的列表，
   *                在 Web Mercator 坐标系中，其中 (0,0) 是西北角，(2^z,2^z) 是东南角。
   * @param buffer  在每个瓦片边缘之外应包含的详细信息（0=无，1=整个瓦片宽度）。
   * @param area    如果是多边形则为 {@code true}，如果是线串则为 {@code false}。
   * @param z       缩放级别。
   * @param extents 地图范围内的瓦片坐标范围。
   * @return 此特征触碰到的每个瓦片及其上出现的点。
   * @throws GeometryException 如果多边形无效并干扰裁剪。
   */
  static TiledGeometry sliceIntoTiles(List<List<CoordinateSequence>> groups, double buffer, boolean area, int z,
    TileExtents.ForZoom extents) throws GeometryException {
    TiledGeometry result = new TiledGeometry(extents, buffer, z, area);
    EnumSet<Direction> wrapResult = result.sliceWorldCopy(groups, 0);
    if (wrapResult.contains(Direction.RIGHT)) {
      result.sliceWorldCopy(groups, -result.maxTilesAtThisZoom);
    }
    if (wrapResult.contains(Direction.LEFT)) {
      result.sliceWorldCopy(groups, result.maxTilesAtThisZoom);
    }
    return result;
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

  private void slicePoint(Coordinate coord) {
    double worldX = coord.getX();
    double worldY = coord.getY();
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
          if (extents.test(wrappedX, y)) {
            TileCoord tile = TileCoord.ofXYZ(wrappedX, y, z);
            double tileY = worldY - y;
            tileContents.computeIfAbsent(tile, t -> List.of(new ArrayList<>()))
              .getFirst()
              .add(GeoUtils.coordinateSequence(tileX * 256, tileY * 256));
          }
        }
      }
    }
  }

  public int zoomLevel() {
    return z;
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
            if (extents.test(x, y)) {
              TileCoord coord = TileCoord.ofXYZ(x, y, z);
              if (!tileContents.containsKey(coord)) {
                next.accept(coord);
              }
            }
          }
        }).iterator();
  }

  /** Returns the tiles touched by this geometry. */
  public CoveredTiles getCoveredTiles() {
    RoaringBitmap bitmap = new RoaringBitmap();
    for (TileCoord coord : tileContents.keySet()) {
      bitmap.add(maxTilesAtThisZoom * coord.x() + coord.y());
    }
    if (filledRanges != null) {
      for (var entry : filledRanges.entrySet()) {
        long colStart = (long) entry.getKey() * maxTilesAtThisZoom;
        var yRanges = entry.getValue();
        bitmap.or(RoaringBitmap.addOffset(yRanges.bitmap(), colStart));
      }
    }
    return new CoveredTiles(bitmap, z);
  }

  /**
   * Returns every tile that this geometry touches, and the partial geometry contained on that tile that can be
   * reassembled using {@link GeometryCoordinateSequences}.
   */
  public Map<TileCoord, List<List<CoordinateSequence>>> getTileData() {
    return tileContents;
  }

  /**
   * 将几何体切片到瓦片，并将结果存储在成员字段中，表示世界的单个“副本”。
   * <p>
   * 而不是处理经度范围超出 -180 到 180 度的内容，返回 {@link Direction#LEFT} 或 {@link Direction#RIGHT}
   * 以指示是否应使用不同的 {@code xOffset} 再次调用此方法以处理包裹的内容。
   *
   * @param groups  几何体。
   * @param xOffset 要应用于每个 X 坐标的偏移量（-2^z 处理包裹过远的东部内容，2^z 处理包裹过远的西部内容）。
   * @return 如果有更多内容在西部，则返回 {@link Direction#LEFT}；如果有更多内容在东部，则返回 {@link Direction#RIGHT}。
   * @throws GeometryException 如果多边形无效并干扰裁剪。
   */
  private EnumSet<Direction> sliceWorldCopy(List<List<CoordinateSequence>> groups, int xOffset)
    throws GeometryException {
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
      if (area && inSeqs.getFirst().size() < 4) {
        // not enough points in outer polygon, ignore
        continue;
      }
      int minPoints = area ? 4 : 2;
      List<CoordinateSequence> outSeqs = inSeqs.stream()
        .filter(seq -> seq.size() >= minPoints)
        .toList();
      if (!outSeqs.isEmpty() && extents.test(tileID.x(), tileID.y())) {
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
    Map<TileCoord, List<CoordinateSequence>> inProgressShapes) throws GeometryException {
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
    record SkippedSegment(Direction side, int lo, int hi, boolean asc) {}
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
      int startY = Math.max(extentMinY, (int) Math.floor(minY - neighborBuffer));
      int endStartY = Math.max(extentMinY, (int) Math.floor(minY + neighborBuffer));
      int startEndY = Math.min(extentMaxY - 1, (int) Math.floor(maxY - neighborBuffer));
      int endY = Math.min(extentMaxY - 1, (int) Math.floor(maxY + neighborBuffer));

      // inside a fill if one edge of the polygon runs straight down the right side or up the left side of the column
      boolean onRightEdge = area && ax == bx && ax == rightEdge;
      boolean onLeftEdge = area && ax == bx && ax == leftEdge;

      for (int y = startY; y <= endY; y++) {
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
            // save the Y range that we skipped in case a later edge intersects a filled tile
            if (endSkip >= y) {
              if (skipped == null) {
                skipped = new ArrayList<>();
              }
              var skippedSegment = new SkippedSegment(
                onLeftEdge ? Direction.LEFT : Direction.RIGHT,
                y,
                endSkip,
                by > ay
              );
              skipped.add(skippedSegment);

              //              System.err.println("    " + skippedSegment);
              if (rightFilled == null) {
                rightFilled = new IntRangeSet();
                leftFilled = new IntRangeSet();
              }
              /*
              A tile is inside a filled region when there is an odd number of vertical edges to the left and right

              for example a simple shape:
                     ---------
               out   |  in   | out
               (0/2) | (1/1) | (2/0)
                     ---------

              or a more complex shape
                     ---------       ---------
               out   |  in   | out   | in    |
               (0/4) | (1/3) | (2/2) | (3/1) |
                     |       ---------       |
                     -------------------------

              So we keep track of this number by xor'ing the left and right fills repeatedly,
              then and'ing them together at the end.
               */
              (onRightEdge ? rightFilled : leftFilled).xor(y, endSkip);

              y = nextNonEdgeTile;
            }
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
            // since we process outer shells before holes, if a hole is the first thing to intersect
            // a tile then it must be inside a filled tile from the outer shell. If that's not the case
            // then the geometry is invalid, so throw an exception so the caller can decide how to handle,
            // for example fix the polygon then try again.
            if (!isFilled(x, y)) {
              throw new GeometryException("bad_polygon_fill", x + ", " + y + " is not filled!");
            }
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
                double start = skippedSegment.asc ? top : bottom;
                double end = skippedSegment.asc ? bottom : top;
                double edgeX = skippedSegment.side == Direction.LEFT ? -buffer : (1 + buffer);
                slice.addPoint(edgeX, start);
                slice.addPoint(edgeX, end);
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
    if (yRange == null || yRange.isEmpty()) {
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
    if (yRange == null || yRange.isEmpty()) {
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

  private boolean isFilled(int x, int y) {
    if (filledRanges == null) {
      return false;
    }
    var filledCol = filledRanges.get(x);
    if (filledCol == null) {
      return false;
    }
    return filledCol.contains(y);
  }

  private enum Direction {
    RIGHT,
    LEFT
  }

  /**
   * A set of tiles touched by a geometry.
   */
  public static class CoveredTiles implements TilePredicate, Iterable<TileCoord> {
    private final RoaringBitmap bitmap;
    private final int maxTilesAtZoom;
    private final int z;

    private CoveredTiles(RoaringBitmap bitmap, int z) {
      this.bitmap = bitmap;
      this.maxTilesAtZoom = 1 << z;
      this.z = z;
    }

    /**
     * Returns the union of tiles covered by {@code a} and {@code b}.
     *
     * @throws IllegalArgumentException if {@code a} and {@code b} have different zoom levels.
     */
    public static CoveredTiles merge(CoveredTiles a, CoveredTiles b) {
      if (a.z != b.z) {
        throw new IllegalArgumentException("Cannot combine CoveredTiles with different zoom levels ");
      }
      return new CoveredTiles(RoaringBitmap.or(a.bitmap, b.bitmap), a.z);
    }

    @Override
    public boolean test(int x, int y) {
      return bitmap.contains(x * maxTilesAtZoom + y);
    }

    @Override
    public String toString() {
      return "CoveredTiles{z=" + z + ", tiles=" + FORMAT.integer(bitmap.getCardinality()) + ", storage=" +
        FORMAT.storage(bitmap.getSizeInBytes()) + "B}";
    }

    public Stream<TileCoord> stream() {
      return bitmap.stream().mapToObj(i -> TileCoord.ofXYZ(i / maxTilesAtZoom, i % maxTilesAtZoom, z));
    }

    @Override
    public Iterator<TileCoord> iterator() {
      return stream().iterator();
    }
  }
}
