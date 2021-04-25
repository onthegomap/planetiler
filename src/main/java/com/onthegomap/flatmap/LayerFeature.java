package com.onthegomap.flatmap;

import com.onthegomap.flatmap.VectorTileEncoder.VectorTileFeature;
import java.util.Map;

public record LayerFeature(
  boolean hasGroup,
  long group,
  int zorder,
  Map<String, Object> attrs,
  byte geomType,
  int[] commands,
  long id
) implements VectorTileFeature {

}
