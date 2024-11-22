package com.onthegomap.planetiler.geo;

public enum SimplifyStrategy {
  RETAIN_IMPORTANT_POINTS,
  RETAIN_EFFECTIVE_AREAS,
  RETAIN_WEIGHTED_EFFECTIVE_AREAS;

  public static final SimplifyStrategy DOUGLAS_PEUCKER = RETAIN_IMPORTANT_POINTS;
  public static final SimplifyStrategy VISVALINGAM_WHYATT = RETAIN_EFFECTIVE_AREAS;
}
