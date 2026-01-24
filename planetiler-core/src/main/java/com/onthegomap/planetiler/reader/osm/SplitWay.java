package com.onthegomap.planetiler.reader.osm;

/**
 * Marker interface that indicates a {@link com.onthegomap.planetiler.reader.SourceFeature} is a part of a way that has
 * been split at intersections.
 */
public interface SplitWay {
  /** A new unique ID assigned to this segment of the way. */
  long uniqueId();
}
