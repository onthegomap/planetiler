package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class ResourceUsageTest {

  @Test
  void testEmpty() {
    new ResourceUsage("testing it out").checkAgainstLimits(true, false);
  }

  @Test
  void testFakeResource() {
    var resource = new ResourceUsage.Global("testing resource", "Get more.", () -> OptionalLong.of(100L));
    var check = new ResourceUsage("testing it out")
      .add(resource, 90, "description");

    check.checkAgainstLimits(false, false);
    // within 15% over the limit is close enough for an estimate
    check.add(resource, 20, "more");
    check.checkAgainstLimits(false, false);
    check.add(resource, 10, "even more");
    var exception = assertThrows(InsufficientResourcesException.class, () -> check.checkAgainstLimits(false, false));
    assertEquals("Not enough testing resource for testing it out: needs about 120B, 100B available. Get more.",
      exception.getMessage());
    check.checkAgainstLimits(true, false);
  }

  @Test
  void testTooMuchRam() {
    long limit = Runtime.getRuntime().maxMemory();
    var check = new ResourceUsage("testing it out")
      .addMemory(limit, "test");

    check.checkAgainstLimits(false, false);
    check.addMemory(limit / 2, "more");
    var exception = assertThrows(InsufficientResourcesException.class, () -> check.checkAgainstLimits(false, false));
    assertTrue(exception.getMessage().startsWith("Not enough memory for testing it out"), exception.getMessage());
    check.checkAgainstLimits(true, false);
  }

  @Test
  void testDiskAllowsAtLeastOneGigabyteOver() {
    var disk = new ResourceUsage.DiskUsage(FileUtils.getFileStore(Path.of("")));
    assertEquals(1L << 30, disk.margin(100));
    assertEquals(15L << 30, disk.margin(100L << 30));
  }

  @Test
  void testNotEnoughDisk() {
    Path dir = Path.of("").toAbsolutePath();
    long free = FileUtils.usableSpace(FileUtils.getFileStore(dir));
    var check = new ResourceUsage("testing it out", "Free up space.")
      .addDisk(dir, free * 2 + (2L << 30), "test");

    var exception = assertThrows(InsufficientResourcesException.class, () -> check.checkAgainstLimits(false, false));
    assertTrue(exception.getMessage().startsWith("Not enough disk space on "), exception.getMessage());
    assertTrue(exception.getMessage().endsWith(" Free up space."), exception.getMessage());
  }
}
