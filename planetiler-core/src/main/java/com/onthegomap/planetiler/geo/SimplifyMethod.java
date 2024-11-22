package com.onthegomap.planetiler.geo;

public enum SimplifyMethod {
  RETAIN_IMPORTANT_POINTS,
  RETAIN_EFFECTIVE_AREAS,
  RETAIN_WEIGHTED_EFFECTIVE_AREAS;

  public static final SimplifyMethod DOUGLAS_PEUCKER = RETAIN_IMPORTANT_POINTS;
  public static final SimplifyMethod VISVALINGAM_WHYATT = RETAIN_EFFECTIVE_AREAS;
}
