package com.onthegomap.planetiler.custommap.configschema;

import com.onthegomap.planetiler.geo.GeometryType;
import java.util.Collection;

public record FeatureItem(
  Collection<String> sources,
  ZoomConfig zoom,
  GeometryType geometry,
  TagCriteria includeWhen,
  TagCriteria excludeWhen,
  Collection<AttributeDefinition> attributes
) {}
