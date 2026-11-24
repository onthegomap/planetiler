package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class BuildInfoTest {

  @Test
  void testVersionLine() {
    assertEquals("planetiler 0.11.0 (1a2b3c4d, 1970-01-01T00:00:01Z)",
      new BuildInfo("1a2b3c4d", "0.11.0", 1000L).versionLine());
  }

  @Test
  void testVersionLineWithoutBuildTime() {
    assertEquals("planetiler 0.11.0 (1a2b3c4d)", new BuildInfo("1a2b3c4d", "0.11.0", null).versionLine());
  }

  @Test
  void testVersionDetails() {
    var lines = BuildInfo.versionDetails().lines().toList();
    assertEquals(2, lines.size());
    assertTrue(lines.get(0).startsWith("planetiler "), lines.get(0));
    assertTrue(lines.get(1).startsWith("Java " + Runtime.version() + " ("), lines.get(1));
  }
}
