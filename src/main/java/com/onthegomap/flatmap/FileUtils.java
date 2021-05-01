package com.onthegomap.flatmap;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FileUtils {

  private FileUtils() {
  }

  public static Stream<Path> walkFileSystem(FileSystem fileSystem) {
    return StreamSupport.stream(fileSystem.getRootDirectories().spliterator(), false)
      .flatMap(rootDirectory -> {
        try {
          return Files.walk(rootDirectory);
        } catch (IOException e) {
          throw new IllegalStateException("Unable to walk " + rootDirectory + " in " + fileSystem, e);
        }
      });
  }

  public static boolean hasExtension(Path path, String extension) {
    return path.toString().toLowerCase().endsWith("." + extension.toLowerCase());
  }
}
