package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.util.LoopLineMerger;
import org.locationtech.jts.geom.Coordinate;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class LoopLineMergerTest {

  @Test
  void testRoundCoordinate() {
    // rounds coordinates to fraction of 1 / 16 == 0.0625
    Coordinate coordinate = new Coordinate(0.05, 0.07);
    assertEquals(LoopLineMerger.roundCoordinate(coordinate).getX(), 0.0625);
    assertEquals(LoopLineMerger.roundCoordinate(coordinate).getY(), 0.0625);
  }
}
