package com.onthegomap.planetiler.geo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class UnitTest {
  @ParameterizedTest
  @CsvSource({
    "METER, 1, m; meters; metre; metres",
    "KILOMETER, 0.001, km; kilometers",
    "FOOT, 3.28084, ft; feet; foot",
    "MILE, 0.000621371, mi; mile; miles",
    "NAUTICAL_MILE, 0.000539957, nm; nautical miles",
    "YARD, 1.0936136964129, yd; yards; yds",

    "Z0_PIXEL, 0.00390625, z0px; z0 pixels; z0 pixel",
    "Z0_TILE, 1, z0ti; z0tile; z0 tile; z0_tiles; z0_tiles; z0 tiles",
  })
  void testLengthAndDerivedArea(String name, double expected, String aliases) {
    Unit.Length length = Unit.Length.from(name);
    Unit.Area area = Unit.Area.from("SQUARE_" + name);
    assertEquals(expected, length.fromBaseUnit(1), expected / 1e5);
    double expectedArea = expected * expected;
    assertEquals(expected * expected, area.fromBaseUnit(1), expectedArea / 1e5);

    for (String alias : aliases.split(";")) {
      assertEquals(length, Unit.Length.from(alias), alias);
      assertEquals(area, Unit.Area.from("s" + alias), "s" + alias);
      assertEquals(area, Unit.Area.from("sq " + alias), "sq " + alias);
      assertEquals(area, Unit.Area.from("square " + alias), "square " + alias);
      assertEquals(area, Unit.Area.from(alias + "2"), alias + "2");
    }
  }

  @ParameterizedTest
  @CsvSource({
    "ARE, 0.01, a; ares",
    "HECTARE, 0.0001, ha; hectares",
    "ACRE, 0.000247105, ac; acres",
  })
  void testCustomArea(String name, double expected, String aliases) {
    Unit.Area area = Unit.Area.valueOf(name);
    assertEquals(expected, area.fromBaseUnit(1), expected / 1e5);
    for (String alias : aliases.split(";")) {
      assertEquals(area, Unit.Area.from(alias));
    }
  }
}
