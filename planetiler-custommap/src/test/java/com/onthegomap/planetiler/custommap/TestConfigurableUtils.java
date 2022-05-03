package com.onthegomap.planetiler.custommap;

import java.nio.file.Path;

class TestConfigurableUtils {
  static Path pathToTestResource(String resource) {
    return resolve(Path.of("planetiler-custommap", "src", "test", "resources", resource));
  }

  static Path pathToTestInvalidResource(String resource) {
    return resolve(Path.of("planetiler-custommap", "src", "test", "resources", "invalidSchema", resource));
  }

  static Path pathToSample(String resource) {
    return resolve(Path.of("planetiler-custommap", "src", "main", "resources", "samples", resource));
  }

  private static Path resolve(Path pathFromRoot) {
    Path cwd = Path.of("").toAbsolutePath();
    return cwd.resolveSibling(pathFromRoot);
  }
}
