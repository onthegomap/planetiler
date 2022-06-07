package com.onthegomap.planetiler.custommap.configschema;

import java.util.Collection;

public record FeatureLayer(
  String name,
  Collection<FeatureItem> features
) {}
