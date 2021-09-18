package com.onthegomap.flatmap.render;

import com.onthegomap.flatmap.VectorTile;
import com.onthegomap.flatmap.geo.TileCoord;
import java.util.Optional;

/**
 * An encoded vector tile feature on a tile with an extra {@code sortKey} and {@code group} that define its placement in
 * the eventual output tile.
 *
 * @param tile              the tile this feature will live in
 * @param vectorTileFeature the encoded vector tile feature
 * @param sortKey           ordering of features in the output tile
 * @param group             if present, a group ID and limit that is used to limit features in a certain area of tile
 */
public record RenderedFeature(
  TileCoord tile,
  VectorTile.Feature vectorTileFeature,
  int sortKey,
  Optional<Group> group
) {

  public RenderedFeature {
    assert vectorTileFeature != null;
  }

  /**
   * Information used to limit features or assign a "rank" for features in a certain area of the tile
   *
   * @param group ID of the group that features live in
   * @param limit maximum rank within {@code group} in a tile that this feature should be included at, for example if
   *              this is the 4th feature in a group with lowest sort-key then the feature is included if {@code limit
   *              <= 4}
   */
  public static record Group(long group, int limit) {}
}
