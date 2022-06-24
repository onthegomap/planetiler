package com.onthegomap.planetiler.geo;

import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;

import java.util.function.Predicate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.geom.util.GeometryTransformer;

/**
 * A function that filters to only tile coordinates that overlap a given {@link Envelope}.
 */
public class TileExtents implements Predicate<TileCoord> {

  private final ForZoom[] zoomExtents;

  private TileExtents(ForZoom[] zoomExtents) {
    this.zoomExtents = zoomExtents;
  }

  private static int quantizeDown(double value, int levels) {
    return Math.max(0, Math.min(levels, (int) Math.floor(value * levels)));
  }

  private static int quantizeUp(double value, int levels) {
    return Math.max(0, Math.min(levels, (int) Math.ceil(value * levels)));
  }

  /** Returns a filter to tiles that intersect {@code worldBounds} (specified in world web mercator coordinates). */
  public static TileExtents computeFromWorldBounds(int maxzoom, Envelope worldBounds) {
    return computeFromWorldBounds(maxzoom, worldBounds, null);
  }

  public static Geometry latLonToTileCoordinates(Geometry geom, int zoom) {
    var transformer = new GeometryTransformer() {
      @Override
      protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
        CoordinateSequence copy = new PackedCoordinateSequence.Double(coords.size(), 2, 0);
        double n = Math.pow(2d, zoom);
        for (int i = 0; i < coords.size(); i++) {
          copy.setOrdinate(i, 0, n * GeoUtils.getWorldX(coords.getX(i)));
          copy.setOrdinate(i, 1, n * GeoUtils.getWorldY(coords.getY(i)));
        }
        return copy;
      }
    };
    // arbitrary tolerance to make test fast while getting as less tiles as possible
    double tolerance = 10d / 256d / zoom;
    var simplified = DouglasPeuckerSimplifier.simplify(geom, tolerance);
    return transformer.transform(simplified);
  }

  /** Returns a filter to tiles that intersect {@code worldBounds} (specified in world web mercator coordinates). */
  public static TileExtents computeFromWorldBounds(int maxzoom, Envelope worldBounds, Geometry shape) {
    ForZoom[] zoomExtents = new ForZoom[maxzoom + 1];
    for (int zoom = 0; zoom <= maxzoom; zoom++) {
      int max = 1 << zoom;
      zoomExtents[zoom] = new ForZoom(
        quantizeDown(worldBounds.getMinX(), max),
        quantizeDown(worldBounds.getMinY(), max),
        quantizeUp(worldBounds.getMaxX(), max),
        quantizeUp(worldBounds.getMaxY(), max),
        shape != null ? PreparedGeometryFactory.prepare(latLonToTileCoordinates(shape, zoom)) : null
      );
    }
    return new TileExtents(zoomExtents);
  }

  public ForZoom getForZoom(int zoom) {
    return zoomExtents[zoom];
  }

  public boolean test(int x, int y, int z) {
    return getForZoom(z).test(x, y);
  }

  @Override
  public boolean test(TileCoord tileCoord) {
    return test(tileCoord.x(), tileCoord.y(), tileCoord.z());
  }

  /**
   * X/Y extents within a given zoom level. {@code minX} and {@code minY} are inclusive and {@code maxX} and {@code
   * maxY} are exclusive. shape is an optional polygon defining a more refine shape
   */
  public record ForZoom(int minX, int minY, int maxX, int maxY, PreparedGeometry shape) {

    public boolean test(int x, int y) {
      if (shape != null) {

        return shape.intersects(JTS_FACTORY.toGeometry(new Envelope(
          x,
          x + 1.0f,
          y,
          y + 1.0f
        )));
      }
      return testX(x) && testY(y);
    }

    public boolean testOverShape(int x, int y, int z) {
      if (shape != null) {
        return shape.intersects(JTS_FACTORY.toGeometry(new Envelope(
          x,
          x + 1.0f,
          y,
          y + 1.0f
        )));
      }
      return true;
    }

    public boolean testX(int x) {
      return x >= minX && x < maxX;
    }

    public boolean testY(int y) {
      return y >= minY && y < maxY;
    }
  }
}
