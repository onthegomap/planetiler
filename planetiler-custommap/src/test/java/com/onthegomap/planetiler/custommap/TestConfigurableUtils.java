package com.onthegomap.planetiler.custommap;

import java.nio.file.Path;

public class TestConfigurableUtils {
  public static Path pathToResource(String resource) {
    Path cwd = Path.of("").toAbsolutePath();
    Path pathFromRoot = Path.of("planetiler-custommap", "src", "test", "resources", resource);
    return cwd.resolveSibling(pathFromRoot);
  }
}
