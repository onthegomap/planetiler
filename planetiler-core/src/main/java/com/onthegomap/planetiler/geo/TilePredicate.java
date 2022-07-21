package com.onthegomap.planetiler.geo;

public interface TilePredicate {
  boolean test(int x, int y);
}
