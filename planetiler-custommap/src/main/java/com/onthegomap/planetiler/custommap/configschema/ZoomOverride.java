package com.onthegomap.planetiler.custommap.configschema;

import java.util.Map;

/**
 * Configuration item that instructs the renderer to override the default zoom range for features which contain specific
 * tag combinations.
 */
public record ZoomOverride(
  Integer min,
  Integer max,
  Map<String, Object> tag) {}
