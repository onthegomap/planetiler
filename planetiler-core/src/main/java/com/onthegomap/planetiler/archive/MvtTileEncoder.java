package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.util.TileSizeStats;

/** Encodes tiles to Mapbox Vector Tile protobufs. */
final class MvtTileEncoder implements TileEncoder {

  @Override
  public Encoded encode(VectorTile tile, boolean includeIds) {
    var proto = tile.toProto(includeIds);
    return new Encoded(proto.toByteArray(), TileSizeStats.computeTileStats(proto));
  }
}
