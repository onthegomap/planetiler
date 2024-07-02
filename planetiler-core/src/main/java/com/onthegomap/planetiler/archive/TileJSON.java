package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.util.LayerAttrStats;
import java.util.List;
import java.util.Map;

public record TileJSON(
  String tilejson,
  String name,
  String description,
  String version,
  String attribution,
  String template,
  String legend,
  String scheme,
  List<String> tiles,
  List<String> grids,
  List<String> data,
  double[] bounds,
  double[] center,
  Integer minzoom,
  Integer maxzoom,
  List<LayerAttrStats.VectorLayer> vectorLayers,
  Map<String, Object> custom
) {
}
