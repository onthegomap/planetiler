package com.onthegomap.planetiler.geo;

import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;

import java.util.function.Predicate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A function that filters to only tile coordinates that overlap a given {@link Envelope}.
 */
public class TileExtents implements Predicate<TileCoord> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TileExtents.class);
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

  private static Geometry prepareShapeForZoom(Geometry geom, int zoom) {
    double scale = 1 << zoom;
    var mercator = GeoUtils.latLonToWorldCoords(geom);
    var scaled = AffineTransformation.scaleInstance(scale, scale).transform(mercator);
    return TopologyPreservingSimplifier.simplify(scaled, 0.1);
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
        shape != null ? PreparedGeometryFactory.prepare(prepareShapeForZoom(shape, zoom)) : null
      );
      LOGGER.warn("prepareShapeForZoom {} {}", zoom, prepareShapeForZoom(shape, zoom).getNumPoints());
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
      return testOverShape(x, y) && testX(x) && testY(y);
    }

    public boolean testOverShape(int x, int y) {
      if (shape != null) {
        return shape
          .intersects(JTS_FACTORY.createPolygon(PackedCoordinateSequenceFactory.DOUBLE_FACTORY.create(new double[]{
            x, y,
            x, y + 1d,
            x + 1d, y + 1d,
            x + 1d, y,
            x, y
          }, 2)));
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
