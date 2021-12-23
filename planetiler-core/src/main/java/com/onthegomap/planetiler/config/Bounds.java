package com.onthegomap.planetiler.config;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.TileExtents;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds the bounds of the map to generate.
 * <p>
 * Call {@link #setFallbackProvider(Provider)} when input data source (i.e. {@link OsmInputFile}) is available to infer
 * bounds automatically. If no bounds are set, defaults to the entire planet.
 */
public class Bounds {

  private static final Logger LOGGER = LoggerFactory.getLogger(Bounds.class);

  private Envelope latLon;
  private Envelope world;
  private TileExtents tileExtents;

  Bounds(Envelope latLon) {
    set(latLon);
  }

  public Envelope latLon() {
    return latLon == null ? GeoUtils.WORLD_LAT_LON_BOUNDS : latLon;
  }

  public Envelope world() {
    return world == null ? GeoUtils.WORLD_BOUNDS : world;
  }

  public TileExtents tileExtents() {
    if (tileExtents == null) {
      tileExtents = TileExtents.computeFromWorldBounds(PlanetilerConfig.MAX_MAXZOOM, world());
    }
    return tileExtents;
  }

  /** If no bounds were set initially, then infer bounds now from {@code latLonProvider}. */
  public Bounds setFallbackProvider(Provider latLonProvider) {
    if (latLon == null) {
      Envelope bounds = latLonProvider.getLatLonBounds();
      LOGGER.info("Setting map bounds from input: " + bounds);
      set(bounds);
    }
    return this;
  }

  private void set(Envelope latLon) {
    if (latLon != null) {
      this.latLon = latLon;
      this.world = GeoUtils.toWorldBounds(latLon);
      this.tileExtents = TileExtents.computeFromWorldBounds(PlanetilerConfig.MAX_MAXZOOM, world);
    }
  }

  /** A source for lat/lon bounds (i.e. {@link OsmInputFile}). */
  public interface Provider {

    /** Returns the latitude/longitude bounds of the map to generate. */
    Envelope getLatLonBounds();
  }
}
