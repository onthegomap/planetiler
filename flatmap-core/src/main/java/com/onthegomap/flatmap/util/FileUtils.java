package com.onthegomap.flatmap.util;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Comparator;
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

  public static long fileSize(Path path) {
    try {
      return Files.size(path);
    } catch (IOException e) {
      return 0;
    }
  }

  public static long directorySize(Path path) {
    try {
      return Files.walk(path)
        .filter(Files::isRegularFile)
        .mapToLong(FileUtils::fileSize)
        .sum();
    } catch (IOException e) {
      return 0;
    }
  }

  public static long size(Path path) {
    return Files.isDirectory(path) ? directorySize(path) : fileSize(path);
  }

  public static void deleteFile(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to delete " + path, e);
    }
  }

  public static void deleteDirectory(Path path) {
    try {
      Files.walk(path)
        .sorted(Comparator.reverseOrder())
        .forEach(FileUtils::deleteFile);
    } catch (NoSuchFileException e) {
      // this is OK, file doesn't exist, so can't walk
    } catch (IOException e) {
      throw new IllegalStateException("Unable to delete " + path, e);
    }
  }

  public static void delete(Path path) {
    if (Files.isDirectory(path)) {
      deleteDirectory(path);
    } else {
      deleteFile(path);
    }
  }

  public static void createParentDirectories(Path path) {
    try {
      if (Files.isDirectory(path)) {
        Files.createDirectories(path);
      } else {
        Files.createDirectories(path.getParent());
      }
    } catch (IOException e) {
      throw new IllegalStateException("Unable to create parent directories " + path, e);
    }
  }
}
