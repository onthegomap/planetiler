package com.onthegomap.planetiler.reader.osm;

import java.util.List;
import org.locationtech.jts.geom.Envelope;

public record OsmHeader(
  Envelope bounds,
  List<String> requiredFeatures
) {}
