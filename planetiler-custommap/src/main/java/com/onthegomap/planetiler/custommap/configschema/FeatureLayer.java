package com.onthegomap.planetiler.custommap.configschema;

import java.util.Collection;

public record FeatureLayer(
  String id,
  Collection<FeatureItem> features
) {}
