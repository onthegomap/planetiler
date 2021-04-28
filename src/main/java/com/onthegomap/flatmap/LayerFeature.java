package com.onthegomap.flatmap;

import com.onthegomap.flatmap.VectorTileEncoder.VectorTileFeature;
import com.onthegomap.flatmap.collections.MergeSortFeatureMap.FeatureMapKey;
import com.onthegomap.flatmap.collections.MergeSortFeatureMap.FeatureMapValue;
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

  public static LayerFeature of(FeatureMapKey key, FeatureMapValue value) {
    return new LayerFeature(
      key.hasGroup(),
      value.group(),
      key.zOrder(),
      value.attrs(),
      value.geomType(),
      value.commands(),
      value.featureId()
    );
  }

}
