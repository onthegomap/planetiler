package com.onthegomap.planetiler.reader.osm;

import java.time.Instant;
import java.util.List;
import org.locationtech.jts.geom.Envelope;

/** Data parsed from the header block of an OSM input file. */
public record OsmHeader(
    Envelope bounds,
    List<String> requiredFeatures,
    List<String> optionalFeaturesList,
    String writingprogram,
    String source,
    Instant instant,
    long osmosisReplicationSequenceNumber,
    String osmosisReplicationBaseUrl) {}
