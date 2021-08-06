package com.onthegomap.flatmap.config;

import com.onthegomap.flatmap.geo.GeoUtils;
import org.locationtech.jts.geom.Envelope;

public interface BoundsProvider {

  BoundsProvider WORLD = () -> GeoUtils.WORLD_LAT_LON_BOUNDS;

  Envelope getBounds();
}
