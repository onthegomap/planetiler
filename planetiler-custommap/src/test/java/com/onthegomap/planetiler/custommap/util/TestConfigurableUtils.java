package com.onthegomap.planetiler.custommap.util;

import java.nio.file.Path;

public class TestConfigurableUtils {
  public static Path pathToTestResource(String resource) {
    return resolve(Path.of("planetiler-custommap", "src", "test", "resources", "validSchema", resource));
  }

  public static Path pathToTestInvalidResource(String resource) {
    return resolve(Path.of("planetiler-custommap", "src", "test", "resources", "invalidSchema", resource));
  }

  public static Path pathToSample(String resource) {
    return resolve(Path.of("planetiler-custommap", "src", "main", "resources", "samples", resource));
  }

  private static Path resolve(Path pathFromRoot) {
    Path cwd = Path.of("").toAbsolutePath();
    return cwd.resolveSibling(pathFromRoot);
  }
}
