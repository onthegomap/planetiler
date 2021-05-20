package com.onthegomap.flatmap.render;

import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.geo.TileCoord;
import java.util.Optional;

public record RenderedFeature(
  TileCoord tile,
  VectorTileEncoder.Feature vectorTileFeature,
  int zOrder,
  Optional<Group> group
) {

  public RenderedFeature {
    assert vectorTileFeature != null;
  }

  public static record Group(long group, int limit) {

  }
}
