package com.onthegomap.flatmap;

import java.util.Map;

public record LayerFeature(
  boolean hasGroup,
  long group,
  int zorder,
  Map<String, Object> attrs,
  byte geomType,
  int[] commands,
  long id
) {

}
