package com.onthegomap.planetiler.reader.osm;

public interface OsmSourceFeature<T extends OsmElement> {
  T originalElement();
}
