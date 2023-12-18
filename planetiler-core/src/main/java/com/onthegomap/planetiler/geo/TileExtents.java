package com.onthegomap.planetiler.geo;

import com.onthegomap.planetiler.render.TiledGeometry;
import java.util.function.Predicate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.AffineTransformation;
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
    return Math.clamp((int) Math.floor(value * levels), 0, levels);
  }

  private static int quantizeUp(double value, int levels) {
    return Math.clamp((int) Math.ceil(value * levels), 0, levels);
  }

  /** Returns a filter to tiles that intersect {@code worldBounds} (specified in world web mercator coordinates). */
  public static TileExtents computeFromWorldBounds(int maxzoom, Envelope worldBounds) {
    return computeFromWorldBounds(maxzoom, worldBounds, null);
  }


  /** Returns a filter to tiles that intersect {@code worldBounds} (specified in world web mercator coordinates). */
  public static TileExtents computeFromWorldBounds(int maxzoom, Envelope worldBounds, Geometry shape) {
    ForZoom[] zoomExtents = new ForZoom[maxzoom + 1];
    var mercator = shape == null ? null : GeoUtils.latLonToWorldCoords(shape);
    for (int zoom = 0; zoom <= maxzoom; zoom++) {
      int max = 1 << zoom;

      var forZoom = new ForZoom(
        zoom,
        quantizeDown(worldBounds.getMinX(), max),
        quantizeDown(worldBounds.getMinY(), max),
        quantizeUp(worldBounds.getMaxX(), max),
        quantizeUp(worldBounds.getMaxY(), max),
        null
      );

      if (mercator != null) {
        Geometry scaled = AffineTransformation.scaleInstance(1 << zoom, 1 << zoom).transform(mercator);
        TiledGeometry.CoveredTiles covered;
        try {
          covered = TiledGeometry.getCoveredTiles(scaled, zoom, forZoom);
        } catch (GeometryException e) {
          throw new IllegalArgumentException("Invalid geometry: " + scaled);
        }
        forZoom = forZoom.withShape(covered);
        LOGGER.info("prepareShapeForZoom z{} {}", zoom, covered);
      }

      zoomExtents[zoom] = forZoom;
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
  public record ForZoom(int z, int minX, int minY, int maxX, int maxY, TilePredicate shapeFilter)
    implements TilePredicate {

    public ForZoom withShape(TilePredicate shape) {
      return new ForZoom(z, minX, minY, maxX, maxY, shape);
    }

    @Override
    public boolean test(int x, int y) {
      return testX(x) && testY(y) && testOverShape(x, y);
    }

    private boolean testOverShape(int x, int y) {
      if (shapeFilter != null) {
        return shapeFilter.test(x, y);
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
