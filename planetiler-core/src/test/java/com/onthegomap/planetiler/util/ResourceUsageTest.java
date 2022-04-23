package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class ResourceUsageTest {

  @Test
  void testEmpty() {
    new ResourceUsage("testing it out").checkAgainstLimits(true, false);
  }

  @Test
  void testFakeResource() {
    var resource = new ResourceUsage.Global("testing resource", "get more", () -> OptionalLong.of(10L));
    var check = new ResourceUsage("testing it out")
      .add(resource, 9, "description");

    check.checkAgainstLimits(false, false);
    check.add(resource, 2, "more");
    assertThrows(IllegalStateException.class, () -> check.checkAgainstLimits(false, false));
    check.checkAgainstLimits(true, false);
  }

  @Test
  void testTooMuchRam() {
    var check = new ResourceUsage("testing it out")
      .addMemory(Runtime.getRuntime().maxMemory() - 1, "test");

    check.checkAgainstLimits(false, false);
    check.addMemory(2, "more");
    assertThrows(IllegalStateException.class, () -> check.checkAgainstLimits(false, false));
    check.checkAgainstLimits(true, false);
  }
}
